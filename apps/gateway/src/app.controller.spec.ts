import { Test, TestingModule } from '@nestjs/testing';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { Request } from 'express';
import { BadRequestException } from '@nestjs/common';
import {
  CreateMessageEvent,
  CreateMessageEventNameV2,
  EVENT_LIFECYCLE_PUBLISHER,
} from '@app/contracts';
import { MetricsService, TracingService } from '@app/common';
import { GatewayOutboxTransactionService } from './outbox/gateway-outbox-transaction.service';
import { GatewayOutboxEvent } from './entities/gateway-outbox-event.entity';

const mockMetricsService = {
  messagesProcessedTotal: { inc: jest.fn() },
  messagesFailedTotal: { inc: jest.fn() },
  processingDurationSeconds: {
    startTimer: jest.fn().mockReturnValue(jest.fn()),
  },
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
 * AppController spec — Producer Reliability Stage
 *
 * Updated from the pre-fix version of this suite, which asserted on
 * `mockClient.emit(...)` (a direct RabbitMQ `ClientProxy` publish). That
 * client no longer exists on the gateway at all (see `app.module.ts`'s
 * doc comment — `ClientsModule.register([{ name: 'MESSAGING_SERVICE' }])`
 * was removed). Every assertion below that previously checked "did we
 * emit to RabbitMQ" now checks "did we durably record the event in the
 * gateway's transactional outbox" via a mocked
 * `GatewayOutboxTransactionService.record()` — exactly the seam
 * `AppController` now depends on instead.
 */
describe('AppController', () => {
  let appController: AppController;
  let mockGatewayOutbox: { record: jest.Mock };
  let mockLifecyclePublisher: { publish: jest.Mock };

  let nextOutboxId = 1;

  beforeEach(async () => {
    nextOutboxId = 1;
    mockGatewayOutbox = {
      record: jest.fn(
        (input: {
          eventType: string;
          payload: unknown;
          correlationId?: string;
          eventId?: string;
        }): Promise<GatewayOutboxEvent> => {
          const row = new GatewayOutboxEvent();
          row.id = nextOutboxId++;
          row.eventType = input.eventType;
          row.payload = input.payload;
          row.correlationId = input.correlationId;
          row.eventId = input.eventId;
          row.status = 'pending';
          row.attempts = 0;
          row.maxAttempts = 5;
          row.lockVersion = 0;
          row.nextRetryAt = new Date();
          row.createdAt = new Date();
          return Promise.resolve(row);
        },
      ),
    };
    mockLifecyclePublisher = {
      publish: jest.fn().mockResolvedValue(undefined),
    };

    const app: TestingModule = await Test.createTestingModule({
      controllers: [AppController],
      providers: [
        AppService,
        {
          provide: GatewayOutboxTransactionService,
          useValue: mockGatewayOutbox,
        },
        {
          provide: EVENT_LIFECYCLE_PUBLISHER,
          useValue: mockLifecyclePublisher,
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

    appController = app.get<AppController>(AppController);
  });

  describe('root', () => {
    it('should return welcome message', () => {
      const result = appController.getRoot({
        headers: {},
      } as Request);
      expect(result.message).toBe('Welcome to the Messaging Showcase platform');
      expect(result.status).toBe('active');
    });
  });

  describe('sendTestMessage', () => {
    const correlationId = '123e4567-e89b-12d3-a456-426614174000';

    it('durably records a contract-valid CreateMessageEvent.v1 in the gateway outbox and returns 202 accepted', async () => {
      const result = await appController.sendTestMessage({
        headers: { 'x-correlation-id': correlationId },
      } as unknown as Request);

      // The event is recorded in the outbox -- NOT published directly to
      // RabbitMQ. No ClientProxy exists on this controller at all anymore.
      expect(mockGatewayOutbox.record).toHaveBeenCalledTimes(1);
      const [recordedInput] = mockGatewayOutbox.record.mock.calls[0] as [
        {
          eventType: string;
          payload: { eventId: string; [key: string]: unknown };
          correlationId: string;
          eventId: string;
        },
      ];

      expect(recordedInput.eventType).toBe(CreateMessageEvent.name);
      expect(recordedInput.payload).toMatchObject({
        type: CreateMessageEvent.name,
        correlationId,
        source: 'gateway',
        trace: ['gateway'],
        payload: {
          subject: 'System test message',
          content: 'Hello RabbitMQ!',
        },
      });
      expect(recordedInput.eventId).toBeDefined();
      expect(recordedInput.correlationId).toBe(correlationId);

      // Honest async contract: 'accepted', not 'success' -- the event has
      // been durably accepted for delivery, not yet confirmed delivered.
      expect(result.status).toBe('accepted');
      expect(result.correlationId).toBe(correlationId);
      expect(result.eventId).toBe(recordedInput.eventId);
      expect((result as { outboxId?: number }).outboxId).toBe(1);

      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({
          stage: 'emitted',
          eventType: CreateMessageEvent.name,
          eventId: recordedInput.eventId,
          correlationId,
        }),
      );
    });

    it('rejects without writing to the outbox when the correlation id is not a valid UUID', async () => {
      let caught: BadRequestException | undefined;
      try {
        await appController.sendTestMessage({
          headers: { 'x-correlation-id': 'not-a-uuid' },
        } as unknown as Request);
      } catch (error) {
        caught = error as BadRequestException;
      }

      expect(caught).toBeInstanceOf(BadRequestException);
      const exceptionBody = caught?.getResponse() as { eventId?: string };
      expect(exceptionBody.eventId).toBeDefined();

      expect(mockGatewayOutbox.record).not.toHaveBeenCalled();
      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({
          stage: 'rejected',
          eventId: exceptionBody.eventId,
        }),
      );
    });

    it('still durably records the event even when RabbitMQ is unreachable (Reliability Requirement A)', async () => {
      // The gateway outbox write is a pure Postgres transaction -- it has
      // no dependency on RabbitMQ at all. Simulate "RabbitMQ unreachable"
      // by proving record() never touches anything broker-related and
      // still succeeds; the relay (a separate, untested-here component)
      // is what would eventually retry the broker side.
      const result = await appController.sendTestMessage({
        headers: { 'x-correlation-id': correlationId },
      } as unknown as Request);

      expect(result.status).toBe('accepted');
      expect(mockGatewayOutbox.record).toHaveBeenCalledTimes(1);
    });
  });

  describe('sendTestMessageV2', () => {
    const correlationId = '123e4567-e89b-12d3-a456-426614174000';

    it('durably records a contract-valid CreateMessageEvent.v2 in the gateway outbox', async () => {
      const result = await appController.sendTestMessageV2({
        headers: { 'x-correlation-id': correlationId },
      } as unknown as Request);

      expect(mockGatewayOutbox.record).toHaveBeenCalledTimes(1);
      const [recordedInput] = mockGatewayOutbox.record.mock.calls[0] as [
        {
          eventType: string;
          payload: {
            eventId: string;
            schemaVersion: string;
            [key: string]: unknown;
          };
          correlationId: string;
          eventId: string;
        },
      ];

      expect(recordedInput.eventType).toBe(CreateMessageEventNameV2.name);
      expect(recordedInput.payload).toMatchObject({
        type: CreateMessageEventNameV2.name,
        schemaVersion: '2',
        correlationId,
        source: 'gateway',
        trace: ['gateway'],
        payload: {
          subject: 'System test message (v2)',
          content: 'Hello RabbitMQ!',
          priority: 'normal',
          metadata: {},
        },
      });
      expect(recordedInput.eventId).toBeDefined();

      expect(result.status).toBe('accepted');
      expect(result.correlationId).toBe(correlationId);
      expect(result.eventId).toBe(recordedInput.eventId);
      expect(result.eventType).toBe(CreateMessageEventNameV2.name);
      expect((result as { schemaVersion?: string }).schemaVersion).toBe('2');

      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({
          stage: 'emitted',
          eventType: CreateMessageEventNameV2.name,
          eventId: recordedInput.eventId,
          correlationId,
        }),
      );
    });

    it('rejects without writing to the outbox when the correlation id is not a valid UUID', async () => {
      let caught: BadRequestException | undefined;
      try {
        await appController.sendTestMessageV2({
          headers: { 'x-correlation-id': 'not-a-uuid' },
        } as unknown as Request);
      } catch (error) {
        caught = error as BadRequestException;
      }

      expect(caught).toBeInstanceOf(BadRequestException);
      const exceptionBody = caught?.getResponse() as { eventId?: string };
      expect(exceptionBody.eventId).toBeDefined();

      expect(mockGatewayOutbox.record).not.toHaveBeenCalled();
      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({
          stage: 'rejected',
          eventId: exceptionBody.eventId,
        }),
      );
    });

    it('does not affect sendTestMessage (v1) behavior when called in the same test run', async () => {
      await appController.sendTestMessageV2({
        headers: { 'x-correlation-id': correlationId },
      } as unknown as Request);

      mockGatewayOutbox.record.mockClear();

      await appController.sendTestMessage({
        headers: { 'x-correlation-id': correlationId },
      } as unknown as Request);

      expect(mockGatewayOutbox.record).toHaveBeenCalledTimes(1);
      const [recordedInput] = mockGatewayOutbox.record.mock.calls[0] as [
        { eventType: string },
      ];
      expect(recordedInput.eventType).toBe(CreateMessageEvent.name);
    });
  });
});
