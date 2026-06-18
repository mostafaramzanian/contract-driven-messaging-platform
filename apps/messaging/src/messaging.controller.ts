import { Controller, Inject, Logger } from '@nestjs/common';
import {
  MessagePattern,
  Payload,
  Ctx,
  RmqContext,
} from '@nestjs/microservices';
import { MessagingService } from './messaging.service';
import {
  validateEvent,
  CreateMessageEvent,
  EVENT_LIFECYCLE_PUBLISHER,
  type EventLifecyclePublisher,
} from '@app/contracts';

/**
 * Best-effort extraction of identifying fields from a payload that has
 * already failed contract validation, purely so the resulting `rejected`
 * lifecycle record can still be correlated with the eventId/correlationId
 * a test (or operator) is looking for. Falls back to 'unknown' rather than
 * throwing -- this function must never itself fail validation a second
 * time over.
 */
function extractIdentifiersForRejection(raw: unknown): {
  eventId: string;
  correlationId: string;
} {
  const asRecord = typeof raw === 'object' && raw !== null ? raw : {};
  const eventId =
    'eventId' in asRecord && typeof asRecord.eventId === 'string'
      ? asRecord.eventId
      : 'unknown';
  const correlationId =
    'correlationId' in asRecord && typeof asRecord.correlationId === 'string'
      ? asRecord.correlationId
      : 'unknown';
  return { eventId, correlationId };
}

@Controller()
export class MessagingController {
  private readonly logger = new Logger(MessagingController.name);

  constructor(
    private readonly messagingService: MessagingService,
    @Inject(EVENT_LIFECYCLE_PUBLISHER)
    private readonly lifecyclePublisher: EventLifecyclePublisher,
  ) {}

  @MessagePattern('test-rabbit')
  async handleTestRabbit(
    @Payload()
    data: { subject?: string; content?: string; correlationId?: string },
    @Ctx() _context: RmqContext,
  ) {
    const correlationId = data.correlationId;
    this.logger.log(
      'Test message received from RabbitMQ',
      MessagingController.name,
      correlationId,
    );

    try {
      const result = await this.messagingService.handleMessageCreation(
        data,
        correlationId,
      );
      this.logger.log(
        'Test message created successfully',
        MessagingController.name,
        correlationId,
      );
      return result;
    } catch (error: unknown) {
      const stack = error instanceof Error ? error.stack : undefined;
      this.logger.error(
        'Failed to create test message',
        stack,
        MessagingController.name,
        correlationId,
      );
      throw error;
    }
  }

  @MessagePattern(CreateMessageEvent.name)
  async handleMessage(@Payload() data: unknown, @Ctx() _context: RmqContext) {
    // RabbitMQ transport here runs with the default noAck:true, meaning
    // the broker removes the message from the queue on delivery
    // regardless of whether this handler succeeds or throws. So "reject,
    // log, drop, do not retry" for an invalid event is already the
    // transport's behavior; this validation step only needs to add the
    // structured rejection log and refuse to act on the bad payload --
    // it does not need to nack or requeue anything.
    const result = validateEvent(CreateMessageEvent.name, data);

    if (!result.valid) {
      const { eventId, correlationId } = extractIdentifiersForRejection(data);
      this.logger.error(
        `Dropped invalid ${CreateMessageEvent.name} event: ${JSON.stringify(result.errors)}`,
        undefined,
        MessagingController.name,
      );
      await this.lifecyclePublisher.publish({
        stage: 'rejected',
        eventType: CreateMessageEvent.name,
        eventId,
        correlationId,
        errors: result.errors,
      });
      // Intentionally not re-thrown as a NestJS RPC exception: there is no
      // caller waiting on a response for an emitted (fire-and-forget)
      // event, and noAck:true means there is nothing left to ack/nack.
      // Returning here is the "drop" in "reject, log, drop, fail fast".
      return;
    }

    const { event } = result;
    const correlationId = event.correlationId;

    await this.lifecyclePublisher.publish({
      stage: 'received',
      eventType: CreateMessageEvent.name,
      eventId: event.eventId,
      correlationId,
    });

    // Append this service to the trace to reflect the hop, matching the
    // lightweight trace model in libs/contracts (gateway -> messaging).
    const trace = [...event.trace, 'messaging'];

    this.logger.log(
      `Validated ${CreateMessageEvent.name} event received from RabbitMQ ` +
        `(eventId=${event.eventId}, trace=${trace.join('->')})`,
      MessagingController.name,
      correlationId,
    );

    await this.lifecyclePublisher.publish({
      stage: 'validated',
      eventType: CreateMessageEvent.name,
      eventId: event.eventId,
      correlationId,
    });

    try {
      const persisted = await this.messagingService.handleMessageCreation(
        event.payload,
        correlationId,
      );
      this.logger.log(
        `Event persisted successfully (eventId=${event.eventId}, messageId=${persisted.id})`,
        MessagingController.name,
        correlationId,
      );
      await this.lifecyclePublisher.publish({
        stage: 'persisted',
        eventType: CreateMessageEvent.name,
        eventId: event.eventId,
        correlationId,
      });
      return persisted;
    } catch (error: unknown) {
      const stack = error instanceof Error ? error.stack : undefined;
      this.logger.error(
        `Failed to persist event (eventId=${event.eventId})`,
        stack,
        MessagingController.name,
        correlationId,
      );
      throw error;
    }
  }
}
