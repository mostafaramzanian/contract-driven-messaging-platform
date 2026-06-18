import { Test, TestingModule } from '@nestjs/testing';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { Request } from 'express';
import { of } from 'rxjs';
import { BadRequestException } from '@nestjs/common';
import { CreateMessageEvent, EVENT_LIFECYCLE_PUBLISHER } from '@app/contracts';

describe('AppController', () => {
  let appController: AppController;
  let mockClient: {
    emit: jest.Mock;
    send: jest.Mock;
    connect: jest.Mock;
  };
  let mockLifecyclePublisher: { publish: jest.Mock };

  beforeEach(async () => {
    mockClient = {
      emit: jest.fn(),
      send: jest.fn().mockReturnValue(of({})),
      connect: jest.fn().mockResolvedValue(undefined),
    };
    mockLifecyclePublisher = {
      publish: jest.fn().mockResolvedValue(undefined),
    };

    const app: TestingModule = await Test.createTestingModule({
      controllers: [AppController],
      providers: [
        AppService,
        // Mock MESSAGING_SERVICE to prevent dependency injection errors
        {
          provide: 'MESSAGING_SERVICE',
          useValue: mockClient,
        },
        {
          provide: EVENT_LIFECYCLE_PUBLISHER,
          useValue: mockLifecyclePublisher,
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

    it('emits a contract-valid CreateMessageEvent.v1 and returns its eventId', async () => {
      const result = await appController.sendTestMessage({
        headers: { 'x-correlation-id': correlationId },
      } as unknown as Request);

      expect(mockClient.emit).toHaveBeenCalledTimes(1);
      const [emittedPattern, emittedPayload] = mockClient.emit.mock
        .calls[0] as [string, { eventId: string; [key: string]: unknown }];
      expect(emittedPattern).toBe(CreateMessageEvent.name);
      expect(emittedPayload).toMatchObject({
        type: CreateMessageEvent.name,
        correlationId,
        source: 'gateway',
        trace: ['gateway'],
        payload: {
          subject: 'System test message',
          content: 'Hello RabbitMQ!',
        },
      });
      expect(emittedPayload.eventId).toBeDefined();

      expect(result.status).toBe('success');
      expect(result.correlationId).toBe(correlationId);
      expect(result.eventId).toBe(emittedPayload.eventId);

      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({
          stage: 'emitted',
          eventType: CreateMessageEvent.name,
          eventId: emittedPayload.eventId,
          correlationId,
        }),
      );
    });

    it('rejects without emitting when the correlation id is not a valid UUID', async () => {
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

      expect(mockClient.emit).not.toHaveBeenCalled();
      expect(mockLifecyclePublisher.publish).toHaveBeenCalledWith(
        expect.objectContaining({
          stage: 'rejected',
          eventId: exceptionBody.eventId,
        }),
      );
    });
  });
});
