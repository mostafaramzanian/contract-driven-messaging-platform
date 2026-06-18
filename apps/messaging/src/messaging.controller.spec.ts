import { Test, TestingModule } from '@nestjs/testing';
import { MessagingController } from './messaging.controller';
import { MessagingService } from './messaging.service';
import {
  buildCreateMessageEventV1,
  CreateMessageEvent,
  EVENT_LIFECYCLE_PUBLISHER,
} from '@app/contracts';

// Mock MessagingService to prevent dependency injection errors
const mockMessagingService = {
  handleMessageCreation: jest
    .fn()
    .mockResolvedValue({ id: 1, subject: 'Test', content: 'Test content' }),
};

describe('MessagingController', () => {
  let messagingController: MessagingController;
  let mockLifecyclePublisher: { publish: jest.Mock };

  beforeEach(async () => {
    mockMessagingService.handleMessageCreation.mockClear();
    mockLifecyclePublisher = {
      publish: jest.fn().mockResolvedValue(undefined),
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
      ],
    }).compile();

    messagingController = app.get<MessagingController>(MessagingController);
  });

  // basic test to ensure controller delegates to the service
  describe('handleTestRabbit', () => {
    it('should delegate to MessagingService.handleMessageCreation', async () => {
      const payload = {
        subject: 'Test',
        content: 'Test content',
        correlationId: 'cid',
      };
      await messagingController.handleTestRabbit(payload, {} as any);
      expect(mockMessagingService.handleMessageCreation).toHaveBeenCalledWith(
        payload,
        'cid',
      );
    });
  });

  describe('handleMessage', () => {
    const correlationId = '123e4567-e89b-12d3-a456-426614174000';

    it('validates a contract-compliant event and delegates payload + correlationId to the service', async () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );

      const result = await messagingController.handleMessage(event, {} as any);

      expect(mockMessagingService.handleMessageCreation).toHaveBeenCalledWith(
        event.payload,
        correlationId,
      );
      expect(result).toEqual({
        id: 1,
        subject: 'Test',
        content: 'Test content',
      });
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
    });

    it('drops an event with an invalid envelope without calling the service', async () => {
      const event = buildCreateMessageEventV1(
        { subject: 'Test', content: 'Test content' },
        correlationId,
      );
      const invalidEvent = { ...event, eventId: 'not-a-uuid' };

      const result = await messagingController.handleMessage(
        invalidEvent,
        {} as any,
      );

      expect(result).toBeUndefined();
      expect(mockMessagingService.handleMessageCreation).not.toHaveBeenCalled();
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

      const result = await messagingController.handleMessage(
        wrongType,
        {} as any,
      );

      expect(result).toBeUndefined();
      expect(mockMessagingService.handleMessageCreation).not.toHaveBeenCalled();
    });
  });
});
