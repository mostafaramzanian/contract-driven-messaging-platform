import {
  Controller,
  Get,
  Inject,
  Req,
  Logger,
  BadRequestException,
  HttpCode,
  HttpStatus,
} from '@nestjs/common';
import type { Request } from 'express';
import { AppService } from './app.service';
import {
  CORRELATION_ID_HEADER,
  MetricsService,
  TracingService,
} from '@app/common';
import {
  buildCreateMessageEventV1,
  buildCreateMessageEventV2,
  validateEvent,
  CreateMessageEvent,
  CreateMessageEventNameV2,
  EVENT_LIFECYCLE_PUBLISHER,
  type EventLifecyclePublisher,
} from '@app/contracts';
import { GatewayOutboxTransactionService } from './outbox/gateway-outbox-transaction.service';

/**
 * AppController (gateway) — Producer Reliability Stage
 *
 * ## What changed, and why (CRITICAL ISSUE #1 — Producer Reliability Gap)
 *
 * Previously, both `sendTestMessage` (v1) and `sendTestMessageV2`
 * validated the event, then called `this.client.emit(...)` — a DIRECT,
 * synchronous-within-the-request publish to RabbitMQ. If RabbitMQ was
 * unreachable at that exact moment, the publish failed, the event was
 * gone (no persistence anywhere), and the HTTP caller got a 5xx with no
 * way to know whether the event would ever be delivered, because it
 * would not be.
 *
 * Now, both handlers validate the event exactly as before (contract
 * validation is unchanged — an invalid event is still rejected with 400
 * and never reaches the outbox at all), then call
 * `GatewayOutboxTransactionService.record()`, which durably persists the
 * event to the `gateway_outbox_events` table in a committed Postgres
 * transaction — a write whose success does NOT depend on RabbitMQ being
 * reachable. `GatewayOutboxRelayService` (started independently by
 * `GatewayOutboxModule`, see that class) asynchronously delivers the row
 * to RabbitMQ afterward, with its own retry/back-off and crash recovery.
 *
 * ## Response contract change
 *
 * The HTTP response now reflects this: a successful call returns
 * `202 Accepted` with `status: 'accepted'` (not `200`/`'success'`) and the
 * `outboxId` of the persisted row, because at the moment this method
 * returns, the event has been durably ACCEPTED for delivery, not
 * necessarily yet delivered. This is the correct and honest contract for
 * an asynchronous, outbox-backed producer — the previous `200`/`'success'`
 * response was already slightly misleading even before this fix (it
 * meant "accepted by RabbitMQ's local write buffer", not "delivered to
 * or processed by the messaging service"), and is now explicitly
 * async-shaped instead of implying a synchronous guarantee that was never
 * actually possible to give.
 */
@Controller()
export class AppController {
  private readonly logger = new Logger(AppController.name);

  constructor(
    private readonly appService: AppService,
    private readonly gatewayOutbox: GatewayOutboxTransactionService,
    @Inject(EVENT_LIFECYCLE_PUBLISHER)
    private readonly lifecyclePublisher: EventLifecyclePublisher,
    private readonly metrics: MetricsService,
    private readonly tracing: TracingService,
  ) {}

  @Get()
  getRoot(@Req() req: Request) {
    const correlationId = req.headers[CORRELATION_ID_HEADER] as string;
    this.logger.log(
      'Root endpoint accessed',
      AppController.name,
      correlationId,
    );

    return {
      message: 'Welcome to the Messaging Showcase platform',
      status: 'active',
      endpoints: {
        api: '/api',
        testRabbit: '/api/test-rabbit',
        testRabbitV2: '/api/test-rabbit-v2',
      },
    };
  }

  @Get('test-rabbit')
  @HttpCode(HttpStatus.ACCEPTED)
  async sendTestMessage(@Req() req: Request) {
    const correlationId = req.headers[CORRELATION_ID_HEADER] as string;

    return this.tracing.withSpan(
      'gateway.emit_create_message_event',
      async (span) => {
        const endTimer = this.metrics.processingDurationSeconds.startTimer({
          service: 'gateway',
          event_type: CreateMessageEvent.name,
        });

        // CorrelationIdMiddleware always sets this header (generating a v4
        // UUID if the caller did not supply one), but a caller-supplied header
        // value could in principle be any string. Building the event below
        // and validating it against the contract is what actually enforces
        // the "correlationId must be a UUID" rule end to end, rather than
        // trusting the header blindly.
        const event = buildCreateMessageEventV1(
          {
            subject: 'System test message',
            content: 'Hello RabbitMQ!',
          },
          correlationId,
        );

        span.setAttributes({
          'messaging.event_type': CreateMessageEvent.name,
          'messaging.correlation_id': correlationId,
          'messaging.event_id': event.eventId,
        });

        const result = validateEvent(CreateMessageEvent.name, event);

        if (!result.valid) {
          // Fail fast: an event that does not match its own contract is never
          // persisted to the outbox, let alone emitted onto RabbitMQ.
          this.logger.error(
            `Refused to emit invalid ${CreateMessageEvent.name} event: ${JSON.stringify(result.errors)}`,
            undefined,
            AppController.name,
            correlationId,
          );
          this.metrics.messagesFailedTotal.inc({
            service: 'gateway',
            event_type: CreateMessageEvent.name,
            error_class: 'VALIDATION',
          });
          endTimer({ outcome: 'rejected' });
          await this.lifecyclePublisher.publish({
            stage: 'rejected',
            eventType: CreateMessageEvent.name,
            eventId: event.eventId,
            correlationId,
            errors: result.errors,
          });
          throw new BadRequestException({
            status: 'rejected',
            reason: 'event_contract_violation',
            eventType: CreateMessageEvent.name,
            eventId: event.eventId,
            errors: result.errors,
          });
        }

        try {
          // Producer Reliability fix: durably persist to the gateway's own
          // transactional outbox INSTEAD OF publishing directly to
          // RabbitMQ here. This commit succeeds (or the whole request
          // fails loudly with a 5xx, which is the correct behavior for a
          // genuine Postgres outage) regardless of whether RabbitMQ is
          // reachable at this instant. GatewayOutboxRelayService delivers
          // the row to RabbitMQ asynchronously, with its own retry and
          // crash-recovery guarantees — see that class's doc comment.
          const outboxRow = await this.gatewayOutbox.record({
            eventType: CreateMessageEvent.name,
            payload: result.event,
            correlationId,
            eventId: result.event.eventId,
          });

          this.logger.log(
            `Validated ${CreateMessageEvent.name} event durably accepted into gateway outbox ` +
              `(outboxId=${outboxRow.id}, eventId=${result.event.eventId})`,
            AppController.name,
            correlationId,
          );

          this.metrics.messagesProcessedTotal.inc({
            service: 'gateway',
            event_type: CreateMessageEvent.name,
            outcome: 'accepted',
          });
          endTimer({ outcome: 'accepted' });

          await this.lifecyclePublisher.publish({
            stage: 'emitted',
            eventType: CreateMessageEvent.name,
            eventId: result.event.eventId,
            correlationId,
          });

          return {
            status: 'accepted',
            message:
              'Event durably persisted to the gateway outbox and will be delivered to RabbitMQ asynchronously',
            correlationId,
            eventId: result.event.eventId,
            eventType: CreateMessageEvent.name,
            outboxId: outboxRow.id,
          };
        } catch (error: unknown) {
          const stack = error instanceof Error ? error.stack : undefined;
          this.logger.error(
            'Failed to durably persist event to the gateway outbox',
            stack,
            AppController.name,
            correlationId,
          );
          this.metrics.messagesFailedTotal.inc({
            service: 'gateway',
            event_type: CreateMessageEvent.name,
            error_class: 'TRANSIENT',
          });
          endTimer({ outcome: 'failure' });
          throw error;
        }
      },
    );
  }

  /**
   * V2 sibling of `sendTestMessage`. Same Producer Reliability fix
   * applied identically: validate, then `gatewayOutbox.record()` instead
   * of a direct broker publish. `SCHEMA_VERSION_HEADER` (previously
   * attached via `RmqRecordBuilder` at `client.emit()` time) is now
   * embedded directly into the outbox payload's headers by
   * `GatewayOutboxRelayService.publishOne()` reading `row.event_type` —
   * see that class for the AMQP-header-construction details. The
   * envelope's own `schemaVersion` field remains authoritative either
   * way (see `dispatch-schema-version.ts`).
   */
  @Get('test-rabbit-v2')
  @HttpCode(HttpStatus.ACCEPTED)
  async sendTestMessageV2(@Req() req: Request) {
    const correlationId = req.headers[CORRELATION_ID_HEADER] as string;

    return this.tracing.withSpan(
      'gateway.emit_create_message_event_v2',
      async (span) => {
        const endTimer = this.metrics.processingDurationSeconds.startTimer({
          service: 'gateway',
          event_type: CreateMessageEventNameV2.name,
        });

        const event = buildCreateMessageEventV2(
          {
            subject: 'System test message (v2)',
            content: 'Hello RabbitMQ!',
          },
          correlationId,
        );

        span.setAttributes({
          'messaging.event_type': CreateMessageEventNameV2.name,
          'messaging.correlation_id': correlationId,
          'messaging.event_id': event.eventId,
          'messaging.schema_version': event.schemaVersion,
        });

        const result = validateEvent(CreateMessageEventNameV2.name, event);

        if (!result.valid) {
          this.logger.error(
            `Refused to emit invalid ${CreateMessageEventNameV2.name} event: ${JSON.stringify(result.errors)}`,
            undefined,
            AppController.name,
            correlationId,
          );
          this.metrics.messagesFailedTotal.inc({
            service: 'gateway',
            event_type: CreateMessageEventNameV2.name,
            error_class: 'VALIDATION',
          });
          endTimer({ outcome: 'rejected' });
          await this.lifecyclePublisher.publish({
            stage: 'rejected',
            eventType: CreateMessageEventNameV2.name,
            eventId: event.eventId,
            correlationId,
            errors: result.errors,
          });
          throw new BadRequestException({
            status: 'rejected',
            reason: 'event_contract_violation',
            eventType: CreateMessageEventNameV2.name,
            eventId: event.eventId,
            errors: result.errors,
          });
        }

        try {
          const outboxRow = await this.gatewayOutbox.record({
            eventType: CreateMessageEventNameV2.name,
            payload: result.event,
            correlationId,
            eventId: result.event.eventId,
          });

          this.logger.log(
            `Validated ${CreateMessageEventNameV2.name} event durably accepted into gateway outbox ` +
              `(outboxId=${outboxRow.id}, eventId=${result.event.eventId})`,
            AppController.name,
            correlationId,
          );

          this.metrics.messagesProcessedTotal.inc({
            service: 'gateway',
            event_type: CreateMessageEventNameV2.name,
            outcome: 'accepted',
          });
          endTimer({ outcome: 'accepted' });

          await this.lifecyclePublisher.publish({
            stage: 'emitted',
            eventType: CreateMessageEventNameV2.name,
            eventId: result.event.eventId,
            correlationId,
          });

          return {
            status: 'accepted',
            message:
              'Event durably persisted to the gateway outbox and will be delivered to RabbitMQ asynchronously',
            correlationId,
            eventId: result.event.eventId,
            eventType: CreateMessageEventNameV2.name,
            schemaVersion: result.event.schemaVersion,
            outboxId: outboxRow.id,
          };
        } catch (error: unknown) {
          const stack = error instanceof Error ? error.stack : undefined;
          this.logger.error(
            'Failed to durably persist event to the gateway outbox',
            stack,
            AppController.name,
            correlationId,
          );
          this.metrics.messagesFailedTotal.inc({
            service: 'gateway',
            event_type: CreateMessageEventNameV2.name,
            error_class: 'TRANSIENT',
          });
          endTimer({ outcome: 'failure' });
          throw error;
        }
      },
    );
  }
}
