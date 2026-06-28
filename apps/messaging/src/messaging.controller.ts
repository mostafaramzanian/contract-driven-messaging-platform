import { Controller, Inject, Logger } from '@nestjs/common';
import {
  MessagePattern,
  Payload,
  Ctx,
  RmqContext,
} from '@nestjs/microservices';
import type * as amqplib from 'amqplib';
import { MessagingService } from './messaging.service';
import {
  validateEvent,
  CreateMessageEvent,
  CreateMessageEventNameV2,
  resolveSchemaVersion,
  SCHEMA_VERSION_HEADER,
  upcastCreateMessageEventV1ToV2,
  EVENT_LIFECYCLE_PUBLISHER,
  type EventLifecyclePublisher,
  type CreateMessageEventV1,
  type CreateMessageEventV2,
} from '@app/contracts';
import { classifyError, ErrorClass } from './reliability/error-classifier';
import { RetryPublisherService } from './reliability/retry-publisher.service';
import { RetryAttemptTrackerService } from './reliability/retry-attempt-tracker.service';
import { RETRY_CONFIG } from './reliability/topology';
import { PinoLoggerService, MetricsService, TracingService } from '@app/common';

/**
 * Safe extraction of eventId/correlationId from an unvalidated payload.
 * Used only for lifecycle/logging records when validation itself has failed.
 */
function extractIdentifiersForRejection(raw: unknown): {
  eventId: string;
  correlationId: string;
} {
  const asRecord = typeof raw === 'object' && raw !== null ? raw : {};
  return {
    eventId:
      'eventId' in asRecord && typeof asRecord.eventId === 'string'
        ? asRecord.eventId
        : 'unknown',
    correlationId:
      'correlationId' in asRecord && typeof asRecord.correlationId === 'string'
        ? asRecord.correlationId
        : 'unknown',
  };
}

/**
 * Read x-retry-count from AMQP message headers.
 * Returns 0 when the header is absent (first delivery).
 */
function readRetryCount(msg: amqplib.ConsumeMessage): number {
  const headers: Record<string, unknown> = msg.properties.headers ?? {};
  const raw: unknown = headers['x-retry-count'];
  return typeof raw === 'number' ? raw : 0;
}

/**
 * Read the schema-version AMQP header (see `SCHEMA_VERSION_HEADER` in
 * `@app/contracts`), if present. Returns `undefined` rather than a
 * default when absent — `resolveSchemaVersion` (the caller of this value)
 * owns the actual "absent means v1" default, this helper just surfaces
 * what is or isn't on the wire.
 */
function readSchemaVersionHeader(msg: amqplib.ConsumeMessage): unknown {
  const headers: Record<string, unknown> = msg.properties.headers ?? {};
  return headers[SCHEMA_VERSION_HEADER];
}

@Controller()
export class MessagingController {
  // Retain NestJS built-in Logger for bootstrap-time messages that fire
  // before PinoLoggerService is fully injectable (e.g. during DI setup).
  private readonly nestLogger = new Logger(MessagingController.name);

  constructor(
    private readonly messagingService: MessagingService,
    @Inject(EVENT_LIFECYCLE_PUBLISHER)
    private readonly lifecyclePublisher: EventLifecyclePublisher,
    private readonly retryPublisher: RetryPublisherService,
    private readonly retryAttemptTracker: RetryAttemptTrackerService,
    private readonly pinoLogger: PinoLoggerService,
    private readonly metrics: MetricsService,
    private readonly tracing: TracingService,
  ) {}

  // ── Legacy pattern kept for backward-compatibility ────────────────────
  @MessagePattern('test-rabbit')
  async handleTestRabbit(
    @Payload()
    data: { subject?: string; content?: string; correlationId?: string },
    @Ctx() context: RmqContext,
  ) {
    const channel = context.getChannelRef() as amqplib.Channel;
    const msg = context.getMessage() as amqplib.ConsumeMessage;
    const correlationId = data.correlationId ?? 'unknown';
    const messageId = String(msg.fields.deliveryTag);

    const log = this.pinoLogger.child({
      correlationId,
      messageId,
      service: 'messaging',
      operation: 'handleTestRabbit',
    });

    log.info('Test message received from RabbitMQ');

    const endTimer = this.metrics.processingDurationSeconds.startTimer({
      service: 'messaging',
      event_type: 'test-rabbit',
    });

    try {
      const result = await this.messagingService.handleMessageCreation(
        data,
        correlationId,
      );

      log.info({ messageDbId: result.id }, 'Test message created successfully');
      this.metrics.messagesProcessedTotal.inc({
        service: 'messaging',
        event_type: 'test-rabbit',
        outcome: 'success',
      });
      endTimer({ outcome: 'success' });
      channel.ack(msg);
      return result;
    } catch (error: unknown) {
      const stack = error instanceof Error ? error.stack : undefined;
      log.error({ stack }, 'Failed to create test message');
      this.metrics.messagesFailedTotal.inc({
        service: 'messaging',
        event_type: 'test-rabbit',
        error_class: ErrorClass.PERMANENT,
      });
      endTimer({ outcome: 'failure' });
      // Nack without requeue → goes to DLX → DLQ
      channel.nack(msg, false, false);
      throw error;
    }
  }

  // ── Primary contract-driven handler ───────────────────────────────────
  // Registered against BOTH versioned patterns. NestJS's @MessagePattern
  // decorator accepts an array (confirmed against the installed
  // @nestjs/microservices source: it wraps metadata in `[].concat(...)`,
  // and `ListenersController.registerPatternHandlers` fans an array out
  // into one `addHandler(pattern, ...)` call per pattern, all pointing at
  // this same method) — so 'CreateMessageEvent.v1' and
  // 'CreateMessageEvent.v2' both dispatch here, with no RabbitMQ topology
  // change: both still land in the same `messaging.work` queue (see
  // reliability/topology.ts), and pattern matching is purely a NestJS
  // dispatch-layer concern, not an AMQP routing-key concern. Add a new
  // pattern string here (e.g. `CreateMessageEventNameV3.name`) when a
  // future v3 is introduced.
  @MessagePattern([CreateMessageEvent.name, CreateMessageEventNameV2.name])
  async handleMessage(
    @Payload() data: unknown,
    @Ctx() context: RmqContext,
  ): Promise<unknown> {
    const channel = context.getChannelRef() as amqplib.Channel;
    const msg = context.getMessage() as amqplib.ConsumeMessage;
    const retryCount = readRetryCount(msg);
    const messageId = String(msg.fields.deliveryTag);

    // ── Step 1: Resolve which contract version this message claims ──────
    // Precedence (see dispatch-schema-version.ts): the envelope's own
    // schemaVersion field first, then the AMQP header mirror, then the
    // `type` discriminator's .vN suffix, then default to v1 (the only
    // version that ever shipped without a schemaVersion field at all).
    // `data` is unvalidated at this point — resolveSchemaVersion is
    // designed to run on `unknown` input for exactly this reason.
    const schemaVersion = resolveSchemaVersion({
      envelope: data,
      header: readSchemaVersionHeader(msg),
    });
    const eventTypeName =
      schemaVersion === '2'
        ? CreateMessageEventNameV2.name
        : CreateMessageEvent.name;

    const endTimer = this.metrics.processingDurationSeconds.startTimer({
      service: 'messaging',
      event_type: eventTypeName,
    });

    // ── Step 2: Contract validation against the resolved version ────────
    const validationResult = validateEvent(eventTypeName, data);

    if (!validationResult.valid) {
      const { eventId, correlationId } = extractIdentifiersForRejection(data);

      const log = this.pinoLogger.child({
        correlationId,
        eventId,
        messageId,
        service: 'messaging',
        operation: 'handleMessage.validate',
      });

      log.error(
        { validationErrors: validationResult.errors },
        `Dropped invalid ${eventTypeName} event`,
      );

      this.tracing.addEvent('contract_validation_failed', {
        eventId,
        correlationId,
      });
      this.metrics.messagesFailedTotal.inc({
        service: 'messaging',
        event_type: eventTypeName,
        error_class: ErrorClass.VALIDATION,
      });

      await this.lifecyclePublisher.publish({
        stage: 'rejected',
        eventType: eventTypeName,
        eventId,
        correlationId,
        errors: validationResult.errors,
      });

      // Validation failure = PERMANENT.  Ack (not nack) to avoid routing
      // a schema-violation through DLX into the DLQ — the DLQ is for
      // infrastructure-failed messages, not contract violations.
      channel.ack(msg);
      return;
    }

    // ── Step 3: Normalize to a single internal shape (v2) ────────────────
    // `validationResult.event` is whichever version's shape actually
    // validated (CreateMessageEventV1 | CreateMessageEventV2). Upcasting
    // v1 results here means everything from this point on — idempotency,
    // MessagingService.handleMessageCreation, logging, lifecycle records
    // that touch `.payload` — operates on exactly one shape, regardless
    // of which version arrived on the wire. See
    // `upcast/upcast-create-message-event.ts`'s doc comment for the full
    // rationale; the short version is that the wire contract supports two
    // versions so producers can migrate independently, but business logic
    // should not have to re-litigate "which version is this" at every
    // call site past the validation boundary.
    //
    // `eventTypeName` (resolved above, before validation) is deliberately
    // still used for logging/metrics/lifecycle records below, NOT a
    // hardcoded v2 string — those records should reflect what actually
    // arrived on the wire (useful for migration-progress observability:
    // "what fraction of traffic is still v1"), even though `event` itself
    // is now uniformly v2-shaped.
    const event: CreateMessageEventV2 =
      schemaVersion === '2'
        ? (validationResult.event as CreateMessageEventV2)
        : upcastCreateMessageEventV1ToV2(
            validationResult.event as CreateMessageEventV1,
          );
    const correlationId = event.correlationId;
    const eventId = event.eventId;
    const trace = [...event.trace, 'messaging'];

    // ── Durable retry-attempt tracking (Requirement 4) ───────────────────
    // Recorded for EVERY delivery of this eventId, regardless of how it
    // arrived — first delivery, an AMQP-redelivered retry (carrying
    // x-retry-count), a manually-requeued DLQ message (no header at
    // all), or a relay replay of a previously-failed outbox row (a
    // brand-new AMQP message with fresh headers). `durableAttemptCount`
    // is therefore authoritative for the MAX_ATTEMPTS decision below —
    // unlike `retryCount` (read from the `x-retry-count` header), it
    // cannot be silently reset by any redelivery path that doesn't
    // happen to carry that specific header forward.
    //
    // Recorded here (after validation, once a real eventId exists) and
    // BEFORE the idempotency/business-processing step, so an attempt is
    // counted even if this delivery turns out to be a duplicate or fails
    // before reaching business logic — "an attempt was made" is true the
    // moment the handler is invoked for this eventId, independent of the
    // outcome.
    const durableAttemptCount =
      await this.retryAttemptTracker.recordAttempt(eventId);

    // Per-event structured logger bound once, used for the rest of this handler
    const log = this.pinoLogger.child({
      correlationId,
      eventId,
      messageId,
      service: 'messaging',
      operation: 'handleMessage',
    });

    log.info(
      {
        attempt: durableAttemptCount,
        headerRetryCount: retryCount,
        trace: trace.join('->'),
      },
      `${eventTypeName} received`,
    );

    await this.lifecyclePublisher.publish({
      stage: 'received',
      eventType: eventTypeName,
      eventId,
      correlationId,
    });

    await this.lifecyclePublisher.publish({
      stage: 'validated',
      eventType: eventTypeName,
      eventId,
      correlationId,
    });

    // ── Step 4: Idempotent business processing (atomic) ─────────────────
    // `handleMessageCreationIdempotent` performs the idempotency-ledger
    // INSERT, the Message write, and the outbox-event write all inside
    // ONE database transaction (see
    // `OutboxTransactionService.runIdempotentWithOutboxEvents`'s doc
    // comment for the full rationale). This replaces what was previously
    // two separate calls — `IdempotencyService.checkAndMark()` committing
    // on its own, followed later by `MessagingService.handleMessageCreation()`
    // committing separately — which left a real crash window where the
    // idempotency row could exist with no corresponding message, causing
    // silent, permanent message loss on redelivery. That window is closed
    // now: the idempotency row, the message, and the outbox event commit
    // or roll back together.
    try {
      const outcome =
        await this.messagingService.handleMessageCreationIdempotent(
          event.payload,
          eventId,
          eventTypeName,
          correlationId,
        );

      if (outcome.duplicate) {
        log.warn({}, 'Duplicate event detected — acking without re-processing');

        this.metrics.messagesProcessedTotal.inc({
          service: 'messaging',
          event_type: eventTypeName,
          outcome: 'duplicate',
        });
        endTimer({ outcome: 'duplicate' });

        // Terminal outcome for this eventId (it was already fully
        // processed by an earlier delivery) — clear the durable retry
        // counter so event_attempts doesn't grow unbounded.
        await this.retryAttemptTracker.clearAttempts(eventId);

        channel.ack(msg);
        return null;
      }

      const persisted = outcome.result;

      log.info({ messageDbId: persisted.id }, 'Event persisted successfully');

      this.metrics.messagesProcessedTotal.inc({
        service: 'messaging',
        event_type: eventTypeName,
        outcome: 'success',
      });
      endTimer({ outcome: 'success' });

      await this.lifecyclePublisher.publish({
        stage: 'persisted',
        eventType: eventTypeName,
        eventId,
        correlationId,
      });

      // Terminal outcome — clear the durable retry counter.
      await this.retryAttemptTracker.clearAttempts(eventId);

      // ✓ Success — acknowledge
      channel.ack(msg);
      return persisted;
    } catch (error: unknown) {
      const stack = error instanceof Error ? error.stack : undefined;
      const errorClass = classifyError(error);
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      const now = new Date().toISOString();

      log.error(
        { stack, attempt: durableAttemptCount, errorClass, errorMessage },
        'Handler failed during business processing',
      );

      this.metrics.messagesFailedTotal.inc({
        service: 'messaging',
        event_type: eventTypeName,
        error_class: errorClass,
      });

      // ── Step 5: Decide retry vs DLQ ──────────────────────────────────
      // Requirement 4: the cap is now enforced against `durableAttemptCount`
      // (from RetryAttemptTrackerService, backed by event_attempts), NOT
      // `retryCount` (read from the `x-retry-count` AMQP header alone).
      // This is the actual fix — `retryCount` is still read (above, via
      // readRetryCount) and still used to build outgoing retry headers
      // below for AMQP-level back-off bookkeeping, but it no longer
      // GOVERNS the retry-vs-DLQ decision, because it is exactly the
      // value that resets to zero on a manual requeue or a relay replay.
      // `durableAttemptCount` cannot be reset by either of those, because
      // it was recorded against `eventId` (the application-level identity
      // of this logical event) the moment this delivery was received,
      // not derived from anything attached to the AMQP message itself.
      const isRetryable =
        errorClass === ErrorClass.TRANSIENT &&
        durableAttemptCount < RETRY_CONFIG.MAX_ATTEMPTS;

      if (isRetryable) {
        const nextAttempt = retryCount + 1;
        const retryHeaders: amqplib.MessagePropertyHeaders = {
          ...msg.properties.headers,
          'x-retry-count': nextAttempt,
          'x-first-error':
            (msg.properties.headers?.['x-first-error'] as string | undefined) ??
            errorMessage,
          'x-error-class': errorClass,
          'x-failed-at':
            (msg.properties.headers?.['x-failed-at'] as string | undefined) ??
            now,
          'x-correlation-id': correlationId,
        };

        // ── Production-readiness fix: publish-before-ack ────────────────
        // Previously this called channel.ack(msg) BEFORE awaiting
        // publishToRetry(), which meant a publish failure (broker
        // back-pressure, a connection mid-reconnect) occurred AFTER the
        // original message was already permanently removed from the
        // broker — silent, unrecoverable message loss. The original
        // message now stays unacked (and therefore redeliverable by the
        // broker if this process crashes) until the retry copy is
        // confirmed durably queued. See RetryPublisherService.publishToRetry,
        // which now throws on back-pressure instead of only logging it,
        // specifically so this catch block has something to catch.
        try {
          await this.retryPublisher.publishToRetry(
            msg.content,
            retryHeaders,
            nextAttempt,
            correlationId,
          );
        } catch (publishErr: unknown) {
          log.error(
            {
              publishError:
                publishErr instanceof Error
                  ? publishErr.message
                  : String(publishErr),
            },
            'Retry publish failed — requeueing original message instead of losing it',
          );
          // Deliberate, narrow exception to the no-requeue convention
          // used everywhere else in this handler: requeue=true here is
          // strictly better than the alternative (the message is simply
          // gone), because the only thing we know for certain at this
          // point is that we could not schedule a retry copy — the
          // broker still has the original, so give it back.
          channel.nack(msg, false, true);
          endTimer({ outcome: 'failure' });
          return;
        }

        channel.ack(msg);

        this.metrics.retryCountTotal.inc({
          service: 'messaging',
          attempt: String(nextAttempt),
        });
        endTimer({ outcome: 'retry' });

        log.warn(
          { nextAttempt, maxAttempts: RETRY_CONFIG.MAX_ATTEMPTS },
          'Message scheduled for retry',
        );
      } else {
        const reason =
          errorClass !== ErrorClass.TRANSIENT
            ? `permanent error (class=${errorClass})`
            : `retry budget exhausted (durableAttemptCount=${durableAttemptCount}/${RETRY_CONFIG.MAX_ATTEMPTS})`;

        log.error({ reason }, 'Routing to DLQ');

        endTimer({ outcome: 'dlq' });

        // Deliberately NOT clearing the durable attempt record here.
        // Requirement: retry limits must survive manual replay. If this
        // eventId's event_attempts row were cleared on DLQ, an operator
        // manually requeueing it from the DLQ would get a fresh
        // MAX_ATTEMPTS budget — exactly the silent-reset behavior this
        // whole mechanism exists to prevent. The row is left in place
        // (already at or past MAX_ATTEMPTS), so a manually-replayed
        // message that fails again is correctly routed straight back to
        // DLQ on its very next attempt, not given another 5 tries.
        // event_attempts rows for permanently-dead-lettered events are
        // cleaned up by retention/TTL tooling (see migration 006's
        // IDX_event_attempts_updated_at), not by this code path.
        channel.nack(msg, false, false);
      }

      return;
    }
  }
}
