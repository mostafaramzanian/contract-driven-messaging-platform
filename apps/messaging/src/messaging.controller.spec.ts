import { Test, TestingModule } from '@nestjs/testing';
import { MessagingController } from './messaging.controller';
import { MessagingService } from './messaging.service';
import {
  buildCreateMessageEventV1,
  buildCreateMessageEventV2,
  CreateMessageEvent,
  CreateMessageEventNameV2,
  SCHEMA_VERSION_HEADER,
  EVENT_LIFECYCLE_PUBLISHER,
} from '@app/contracts';
import { RetryPublisherService } from './reliability/retry-publisher.service';
import { RetryAttemptTrackerService } from './reliability/retry-attempt-tracker.service';
import { ValidationError } from './reliability/error-classifier';
import { PinoLoggerService, MetricsService, TracingService } from '@app/common';
import type { RmqContext } from '@nestjs/microservices';

// Mock MessagingService to prevent dependency injection errors.
//
// `handleMessageCreation` (the original, non-atomic method, still used
// by the legacy `handleTestRabbit` pattern only) and
// `handleMessageCreationIdempotent` (used by the primary `handleMessage`
// handler, see the production-readiness review's idempotency-atomicity
// fix) are mocked separately, matching the real service's two distinct
// methods.
const mockMessagingService = {
  handleMessageCreation: jest
    .fn()
    .mockResolvedValue({ id: 1, subject: 'Test', content: 'Test content' }),
  // Defaults to "not a duplicate, persisted successfully" so business
  // processing always runs in these tests, matching pre-instrumentation
  // test behavior. Individual tests override this with mockResolvedValueOnce
  // / mockRejectedValueOnce as needed.
  handleMessageCreationIdempotent: jest.fn().mockResolvedValue({
    duplicate: false,
    result: { id: 1, subject: 'Test', content: 'Test content' },
  }),
};

// Mock RetryAttemptTrackerService — defaults to always returning attempt 1
// (well within MAX_ATTEMPTS), so existing retry/DLQ-budget tests that
// don't care about the durable counter specifically keep their original
// AMQP-header-driven attempt numbering as the dominant signal in their
// assertions. Tests that specifically exercise the durable-tracking
// behavior (Requirement 4) override recordAttempt's return value with
// mockResolvedValueOnce.
const mockRetryAttemptTracker = {
  recordAttempt: jest.fn().mockResolvedValue(1),
  getAttemptCount: jest.fn().mockResolvedValue(0),
  clearAttempts: jest.fn().mockResolvedValue(undefined),
};

const mockPinoLoggerChild = {
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn(),
};

const mockPinoLoggerService = {
  child: jest.fn().mockReturnValue(mockPinoLoggerChild),
  log: jest.fn(),
  error: jest.fn(),
  warn: jest.fn(),
  debug: jest.fn(),
};

const mockMetricsService = {
  messagesProcessedTotal: { inc: jest.fn() },
  messagesFailedTotal: { inc: jest.fn() },
  dlqMessagesTotal: { inc: jest.fn() },
  retryCountTotal: { inc: jest.fn() },
  processingDurationSeconds: {
    startTimer: jest.fn().mockReturnValue(jest.fn()),
  },
  outboxPendingEvents: { set: jest.fn() },
};

const mockTracingService = {
  getTraceId: jest.fn().mockReturnValue('noop'),
  withSpan: jest.fn((_name: string, fn: (span: unknown) => unknown) =>
    fn({ setAttribute: jest.fn(), setAttributes: jest.fn() }),
  ),
  setAttributes: jest.fn(),
  addEvent: jest.fn(),
};

/**
 * Builds a fake RmqContext + underlying amqplib channel mock, matching the
 * shape MessagingController now reads via context.getChannelRef() /
 * context.getMessage() for manual ack/nack.
 */
function buildMockContext(headers: Record<string, unknown> = {}) {
  const channel = {
    ack: jest.fn(),
    nack: jest.fn(),
  };
  const message = {
    content: Buffer.from('{}'),
    fields: { routingKey: 'messaging.work' },
    properties: { headers },
  };
  const context = {
    getChannelRef: () => channel,
    getMessage: () => message,
  } as unknown as RmqContext;

  return { context, channel, message };
}

describe('MessagingController', () => {
  let messagingController: MessagingController;
  let mockLifecyclePublisher: { publish: jest.Mock };
  let mockRetryPublisher: { publishToRetry: jest.Mock };

  beforeEach(async () => {
    mockMessagingService.handleMessageCreation.mockClear();
    mockMessagingService.handleMessageCreationIdempotent.mockClear();
    mockMessagingService.handleMessageCreationIdempotent.mockResolvedValue({
      duplicate: false,
      result: { id: 1, subject: 'Test', content: 'Test content' },
    });
    mockRetryAttemptTracker.recordAttempt.mockClear();
    mockRetryAttemptTracker.recordAttempt.mockResolvedValue(1);
    mockRetryAttemptTracker.getAttemptCount.mockClear();
    mockRetryAttemptTracker.clearAttempts.mockClear();
    mockMetricsService.messagesProcessedTotal.inc.mockClear();
    mockMetricsService.messagesFailedTotal.inc.mockClear();
    mockMetricsService.dlqMessagesTotal.inc.mockClear();
    mockMetricsService.retryCountTotal.inc.mockClear();
    mockLifecyclePublisher = {
      publish: jest.fn().mockResolvedValue(undefined),
    };
    mockRetryPublisher = {
      publishToRetry: jest.fn().mockResolvedValue(undefined),
    };

    const app: TestingModule = await Test.createTestingModule({
      controllers: [MessagingController],
      providers: [
        {
          provide: MessagingService,
          useValue: mockMessagingService,
        },
        {
          provide: EVENT_LIFECYCLE_PUBLISHER,
          useValue: mockLifecyclePublisher,
        },
        {
          provide: RetryPublisherService,
          useValue: mockRetryPublisher,
        },
        {
          provide: RetryAttemptTrackerService,
          useValue: mockRetryAttemptTracker,
        },
        {
          provide: PinoLoggerService,
          useValue: mockPinoLoggerService,
        },
        {
          provide: MetricsService,
          useValue: mockMetricsService,
        },
        {
          provide: TracingService,
          useValue: mockTracingService,
        },
      ],
    }).compile();

    messagingController = app.get<MessagingController>(MessagingController);
  });

  describe('handleTestRabbit', () => {
    it('delegates to MessagingService.handleMessageCreation and acks on success', async () => {
      const { context, channel } = buildMockContext();
      const payload = {
        subject: 'Test',
        content: 'Test content',
        correlationId: 'cid',
      };

      await messagingController.handleTestRabbit(payload, context);

      expect(mockMessagingService.handleMessageCreation).toHaveBeenCalledWith(
        payload,
        'cid',
      );
      expect(channel.ack).toHaveBeenCalledTimes(1);
      expect(channel.nack).not.toHaveBeenCalled();
    });

    it('nacks without requeue when the service throws', async () => {
      const { context, channel } = buildMockContext();
      mockMessagingService.handleMessageCreation.mockRejectedValueOnce(
        new Error('db down'),
      );

      await expect(
        messagingController.handleTestRabbit(
          { subject: 'x', content: 'y', correlationId: 'cid' },
          context,
        ),
      ).rejects.toThrow('db down');

      expect(channel.nack).toHaveBeenCalledWith(
        context.getMessage(),
        false,
        false,
      );
      expect(channel.ack).not.toHaveBeenCalled();
    });
  });

  describe('handleMessage', () => {
    const correlationId = '123e4567-e89b-12d3-a456-426614174000';

    it('validates a contract-compliant event, persists, and acks', async () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );
      const { context, channel } = buildMockContext();

      const result = await messagingController.handleMessage(event, context);

      // handleMessageCreationIdempotent now receives the *upcasted* v2
      // payload, not the raw v1 one — the dispatcher normalizes every
      // validated event to a single v2 shape before business processing
      // (see upcastCreateMessageEventV1ToV2's doc comment), so
      // priority/metadata (defaulted) and an explicit recipient: undefined
      // key are expected here even though this test only ever builds a v1
      // event. It also now receives eventId/eventType explicitly, since
      // the idempotency check is folded into this same call (see
      // OutboxTransactionService.runIdempotentWithOutboxEvents).
      expect(
        mockMessagingService.handleMessageCreationIdempotent,
      ).toHaveBeenCalledWith(
        {
          subject: 'Test',
          content: 'Test content',
          recipient: undefined,
          priority: 'normal',
          metadata: {},
        },
        event.eventId,
        CreateMessageEvent.name,
        correlationId,
      );
      expect(result).toEqual({
        id: 1,
        subject: 'Test',
        content: 'Test content',
      });
      expect(channel.ack).toHaveBeenCalledTimes(1);
      expect(channel.nack).not.toHaveBeenCalled();
      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({ stage: 'received', eventId: event.eventId }),
      );
      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({
          stage: 'validated',
          eventId: event.eventId,
        }),
      );
      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({
          stage: 'persisted',
          eventId: event.eventId,
        }),
      );
      // Requirement 4: an attempt is durably recorded for every
      // delivery, and cleared once the outcome is terminal (success).
      expect(mockRetryAttemptTracker.recordAttempt).toHaveBeenCalledWith(
        event.eventId,
      );
      expect(mockRetryAttemptTracker.clearAttempts).toHaveBeenCalledWith(
        event.eventId,
      );
    });

    it('drops an event with an invalid envelope: acks (no requeue) without calling the service', async () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );
      const invalidEvent = { ...event, eventId: 'not-a-uuid' };
      const { context, channel } = buildMockContext();

      const result = await messagingController.handleMessage(
        invalidEvent,
        context,
      );

      expect(result).toBeUndefined();
      expect(
        mockMessagingService.handleMessageCreationIdempotent,
      ).not.toHaveBeenCalled();
      expect(channel.ack).toHaveBeenCalledTimes(1);
      expect(channel.nack).not.toHaveBeenCalled();
      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({ stage: 'rejected' }),
      );
    });

    it('drops an event with a wrong type discriminator without calling the service', async () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );
      const wrongType = { ...event, type: CreateMessageEvent.name + '.fake' };
      const { context, channel } = buildMockContext();

      const result = await messagingController.handleMessage(
        wrongType,
        context,
      );

      expect(result).toBeUndefined();
      expect(
        mockMessagingService.handleMessageCreationIdempotent,
      ).not.toHaveBeenCalled();
      expect(channel.ack).toHaveBeenCalledTimes(1);
    });

    it('routes to retry queue on a transient error within retry budget', async () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );
      const { context, channel } = buildMockContext({ 'x-retry-count': 0 });

      const transientError = Object.assign(new Error('connection refused'), {
        code: 'ECONNREFUSED',
      });
      mockMessagingService.handleMessageCreationIdempotent.mockRejectedValueOnce(
        transientError,
      );

      await messagingController.handleMessage(event, context);

      // The original message is acked only AFTER the retry copy is
      // confirmed durably published (see RetryPublisherService.publishToRetry's
      // doc comment / the production-readiness fix) — publishToRetry
      // resolving successfully here is what allows the subsequent ack.
      expect(mockRetryPublisher.publishToRetry).toHaveBeenCalledWith(
        expect.any(Buffer),
        expect.objectContaining({ 'x-retry-count': 1 }),
        1,
        correlationId,
      );
      expect(channel.ack).toHaveBeenCalledTimes(1); // original message acked
      expect(channel.nack).not.toHaveBeenCalled();
    });

    it('requeues (does not ack) the original message when the retry publish itself fails', async () => {
      // This is the regression test for the ack-ordering bug found in the
      // production-readiness review: previously, channel.ack(msg) ran
      // BEFORE awaiting publishToRetry(), so a publish failure occurred
      // after the original message was already permanently gone from the
      // broker. Now, a publish failure must result in nack(requeue=true),
      // never ack.
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );
      const { context, channel } = buildMockContext({ 'x-retry-count': 0 });

      const transientError = Object.assign(new Error('connection refused'), {
        code: 'ECONNREFUSED',
      });
      mockMessagingService.handleMessageCreationIdempotent.mockRejectedValueOnce(
        transientError,
      );
      mockRetryPublisher.publishToRetry.mockRejectedValueOnce(
        new Error('retry publish back-pressured'),
      );

      await messagingController.handleMessage(event, context);

      expect(channel.ack).not.toHaveBeenCalled();
      expect(channel.nack).toHaveBeenCalledWith(
        context.getMessage(),
        false,
        true, // requeue=true: deliberate exception, see controller's comment
      );
    });

    it('routes to DLQ via nack when the DURABLE retry budget is exhausted', async () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );
      // MAX_ATTEMPTS is 5. The AMQP header alone is intentionally NOT
      // what drives this decision anymore (see Requirement 4 / durable
      // retry tracking) -- recordAttempt's durable count is what
      // governs the retry-vs-DLQ decision, set here to 5 (the 5th,
      // budget-exhausting attempt) regardless of what the header says.
      const { context, channel } = buildMockContext({ 'x-retry-count': 4 });
      mockRetryAttemptTracker.recordAttempt.mockResolvedValueOnce(5);

      const transientError = Object.assign(new Error('connection refused'), {
        code: 'ECONNREFUSED',
      });
      mockMessagingService.handleMessageCreationIdempotent.mockRejectedValueOnce(
        transientError,
      );

      await messagingController.handleMessage(event, context);

      expect(channel.nack).toHaveBeenCalledWith(
        context.getMessage(),
        false,
        false,
      );
      expect(mockRetryPublisher.publishToRetry).not.toHaveBeenCalled();
      // DLQ is terminal but the durable counter is deliberately NOT
      // cleared here -- see the controller's comment on this exact
      // point: clearing on DLQ would let a manual replay silently reset
      // the budget, which is the one redelivery path the requirement
      // explicitly says must NOT reset it.
      expect(mockRetryAttemptTracker.clearAttempts).not.toHaveBeenCalled();
    });

    it('routes to DLQ even when the AMQP header suggests budget remains, if the durable count says otherwise', async () => {
      // This is the actual regression test for the bug Requirement 4
      // exists to close: a message redelivered via a path that does NOT
      // carry x-retry-count forward (manual requeue, relay replay) looks
      // like a "first attempt" by the header alone, but the durable
      // count (keyed on eventId, not on anything attached to the AMQP
      // message) correctly remembers this event has already been tried
      // 5 times.
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );
      // No x-retry-count header at all -- exactly what a manually
      // requeued or relay-replayed message looks like.
      const { context, channel } = buildMockContext();
      mockRetryAttemptTracker.recordAttempt.mockResolvedValueOnce(5);

      const transientError = Object.assign(new Error('connection refused'), {
        code: 'ECONNREFUSED',
      });
      mockMessagingService.handleMessageCreationIdempotent.mockRejectedValueOnce(
        transientError,
      );

      await messagingController.handleMessage(event, context);

      // Without the durable-tracking fix, a header-less redelivery would
      // read retryCount=0 and retry yet again, indefinitely, regardless
      // of how many times this eventId has actually been attempted.
      expect(channel.nack).toHaveBeenCalledWith(
        context.getMessage(),
        false,
        false,
      );
      expect(mockRetryPublisher.publishToRetry).not.toHaveBeenCalled();
    });

    it('routes permanent errors straight to DLQ without retrying', async () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );
      const { context, channel } = buildMockContext({ 'x-retry-count': 0 });

      mockMessagingService.handleMessageCreationIdempotent.mockRejectedValueOnce(
        new ValidationError('business rule violated'),
      );

      await messagingController.handleMessage(event, context);

      expect(channel.nack).toHaveBeenCalledWith(
        context.getMessage(),
        false,
        false,
      );
      expect(mockRetryPublisher.publishToRetry).not.toHaveBeenCalled();
    });
    it('acks without republishing when handleMessageCreationIdempotent reports a duplicate', async () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );
      const { context, channel } = buildMockContext();

      mockMessagingService.handleMessageCreationIdempotent.mockResolvedValueOnce(
        { duplicate: true },
      );

      const result = await messagingController.handleMessage(event, context);

      expect(result).toBeNull();
      expect(channel.ack).toHaveBeenCalledTimes(1);
      expect(channel.nack).not.toHaveBeenCalled();
      expect(mockRetryPublisher.publishToRetry).not.toHaveBeenCalled();
      // No 'persisted' lifecycle record for a duplicate -- it was never
      // (re-)persisted.
      expect(mockLifecyclePublisher.publish).not.toHaveBeenCalledWith(
        expect.objectContaining({ stage: 'persisted' }),
      );
      // A confirmed duplicate is also a terminal outcome for this
      // eventId (it was already fully processed by an earlier delivery)
      // -- the durable counter is cleared here too.
      expect(mockRetryAttemptTracker.clearAttempts).toHaveBeenCalledWith(
        event.eventId,
      );
    });
  });

  describe('handleMessage — v2 dispatch', () => {
    const correlationId = '123e4567-e89b-12d3-a456-426614174000';

    it('validates a v2 event (resolved via the AMQP header) and persists its native payload unchanged', async () => {
      const event = buildCreateMessageEventV2(
        {
          subject: 'V2 subject',
          content: 'V2 content',
          priority: 'high',
          metadata: { campaignId: 'spring-2026' },
        },
        correlationId,
      );
      const { context, channel } = buildMockContext({
        [SCHEMA_VERSION_HEADER]: '2',
      });

      const result = await messagingController.handleMessage(event, context);

      // A v2 event is NOT upcasted (it already is v2) -- its payload,
      // including priority/metadata, passes through to business logic
      // exactly as built.
      expect(
        mockMessagingService.handleMessageCreationIdempotent,
      ).toHaveBeenCalledWith(
        {
          subject: 'V2 subject',
          content: 'V2 content',
          recipient: undefined,
          priority: 'high',
          metadata: { campaignId: 'spring-2026' },
        },
        event.eventId,
        CreateMessageEventNameV2.name,
        correlationId,
      );
      expect(result).toEqual({
        id: 1,
        subject: 'Test',
        content: 'Test content',
      });
      expect(channel.ack).toHaveBeenCalledTimes(1);
      expect(channel.nack).not.toHaveBeenCalled();
    });

    it('resolves v2 from the envelope schemaVersion field alone, with no AMQP header present', async () => {
      // resolveSchemaVersion's precedence puts the envelope field first --
      // this confirms dispatch still works correctly for a v2 message
      // whose header was stripped or never set (e.g. a future producer
      // that only sets the envelope field).
      const event = buildCreateMessageEventV2(
        { subject: 'No header', content: 'Still v2' },
        correlationId,
      );
      const { context, channel } = buildMockContext(); // no headers at all

      await messagingController.handleMessage(event, context);

      expect(
        mockMessagingService.handleMessageCreationIdempotent,
      ).toHaveBeenCalledWith(
        expect.objectContaining({ subject: 'No header', content: 'Still v2' }),
        event.eventId,
        CreateMessageEventNameV2.name,
        correlationId,
      );
      expect(channel.ack).toHaveBeenCalledTimes(1);
    });

    it('reports CreateMessageEvent.v2 as the eventType in lifecycle records for a v2 message', async () => {
      const event = buildCreateMessageEventV2(
        { subject: 'V2', content: 'content' },
        correlationId,
      );
      const { context } = buildMockContext({ [SCHEMA_VERSION_HEADER]: '2' });

      await messagingController.handleMessage(event, context);

      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({
          stage: 'persisted',
          eventType: CreateMessageEventNameV2.name,
          eventId: event.eventId,
        }),
      );
    });

    it('still reports CreateMessageEvent.v1 as the eventType for a v1 message, even though the internal event shape is upcasted', async () => {
      const event = buildCreateMessageEventV1(
        { subject: 'V1', content: 'content' },
        correlationId,
      );
      const { context } = buildMockContext();

      await messagingController.handleMessage(event, context);

      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({
          stage: 'persisted',
          eventType: CreateMessageEvent.name,
          eventId: event.eventId,
        }),
      );
    });

    it('rejects a v2-typed event missing its required schemaVersion field', async () => {
      const event = buildCreateMessageEventV2(
        { subject: 'Test', content: 'content' },
        correlationId,
      );
      const { schemaVersion: _omit, ...withoutVersion } = event;
      const { context, channel } = buildMockContext({
        [SCHEMA_VERSION_HEADER]: '2',
      });

      const result = await messagingController.handleMessage(
        withoutVersion,
        context,
      );

      expect(result).toBeUndefined();
      expect(
        mockMessagingService.handleMessageCreationIdempotent,
      ).not.toHaveBeenCalled();
      expect(channel.ack).toHaveBeenCalledTimes(1);
      expect(channel.nack).not.toHaveBeenCalled();
      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({ stage: 'rejected' }),
      );
    });

    it('routes a v2 message to retry on a transient error, preserving the schema-version header across the retry republish', async () => {
      const event = buildCreateMessageEventV2(
        { subject: 'Test', content: 'content' },
        correlationId,
      );
      const { context, channel } = buildMockContext({
        [SCHEMA_VERSION_HEADER]: '2',
        'x-retry-count': 0,
      });

      const transientError = Object.assign(new Error('connection refused'), {
        code: 'ECONNREFUSED',
      });
      mockMessagingService.handleMessageCreationIdempotent.mockRejectedValueOnce(
        transientError,
      );

      await messagingController.handleMessage(event, context);

      expect(mockRetryPublisher.publishToRetry).toHaveBeenCalledWith(
        expect.any(Buffer),
        expect.objectContaining({
          'x-retry-count': 1,
          [SCHEMA_VERSION_HEADER]: '2',
        }),
        1,
        correlationId,
      );
      expect(channel.ack).toHaveBeenCalledTimes(1);
      expect(channel.nack).not.toHaveBeenCalled();
    });

    it('does not affect v1 dispatch when v1 and v2 messages are handled by the same instance in sequence', async () => {
      const v2Event = buildCreateMessageEventV2(
        { subject: 'V2 first', content: 'content' },
        correlationId,
      );
      const { context: v2Context } = buildMockContext({
        [SCHEMA_VERSION_HEADER]: '2',
      });
      await messagingController.handleMessage(v2Event, v2Context);

      mockMessagingService.handleMessageCreationIdempotent.mockClear();

      const v1Event = buildCreateMessageEventV1(
        { subject: 'V1 second', content: 'content' },
        correlationId,
      );
      const { context: v1Context, channel: v1Channel } = buildMockContext();
      await messagingController.handleMessage(v1Event, v1Context);

      expect(
        mockMessagingService.handleMessageCreationIdempotent,
      ).toHaveBeenCalledWith(
        expect.objectContaining({ subject: 'V1 second' }),
        v1Event.eventId,
        CreateMessageEvent.name,
        correlationId,
      );
      expect(v1Channel.ack).toHaveBeenCalledTimes(1);
      expect(v1Channel.nack).not.toHaveBeenCalled();
    });
  });
});
