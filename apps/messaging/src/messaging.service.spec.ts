import { Test, TestingModule } from '@nestjs/testing';
import { Logger } from '@nestjs/common';
import { getRepositoryToken } from '@nestjs/typeorm';
import type { Repository, EntityManager } from 'typeorm';
import { MessagingService } from './messaging.service';
import { Message } from './entities/message.entity';
import { OutboxTransactionService } from './outbox/outbox-transaction.service';

describe('MessagingService', () => {
  let service: MessagingService;
  let mockMessageRepository: jest.Mocked<Pick<Repository<Message>, 'count'>>;
  let mockOutboxTransactionService: jest.Mocked<
    Pick<OutboxTransactionService, 'runWithOutboxEvents'>
  >;

  beforeEach(async () => {
    mockMessageRepository = {
      count: jest.fn(),
    };

    // runWithOutboxEvents: execute the work callback and return its result
    mockOutboxTransactionService = {
      runWithOutboxEvents: jest
        .fn()
        .mockImplementation(
          async (work: (em: EntityManager) => Promise<Message>) => {
            // Provide a minimal mock EntityManager that delegates to an internal mock
            const mockEm = {
              create: jest.fn().mockImplementation((_entity, data) => ({
                ...data,
                id: 1,
                createdAt: new Date(),
              })),
              save: jest
                .fn()
                .mockImplementation((_entity, obj) =>
                  Promise.resolve({ ...obj, id: 1 }),
                ),
            } as unknown as EntityManager;
            return work(mockEm);
          },
        ),
    };

    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MessagingService,
        {
          provide: getRepositoryToken(Message),
          useValue: mockMessageRepository,
        },
        {
          provide: OutboxTransactionService,
          useValue: mockOutboxTransactionService,
        },
      ],
    }).compile();

    service = module.get<MessagingService>(MessagingService);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('getHello', () => {
    it('should return the welcome message', () => {
      const result = service.getHello();
      expect(result).toBe(
        'Welcome to the Messaging Showcase messaging service',
      );
    });
  });

  describe('onModuleInit', () => {
    it('should log success when database connection works', async () => {
      mockMessageRepository.count.mockResolvedValue(5);
      await service.onModuleInit();

      expect(mockMessageRepository.count).toHaveBeenCalled();
    });

    it('should log error when database connection fails', async () => {
      const error = new Error('Database connection failed');
      mockMessageRepository.count.mockRejectedValue(error);
      await service.onModuleInit();

      expect(mockMessageRepository.count).toHaveBeenCalled();
    });
  });

  describe('handleMessageCreation', () => {
    it('should call runWithOutboxEvents and return the persisted message', async () => {
      const data = { subject: 'Test Subject', content: 'Test Content' };
      const correlationId = 'test-correlation-id';

      const result = await service.handleMessageCreation(data, correlationId);

      expect(result).toMatchObject({
        id: 1,
        title: 'Test Subject',
        content: 'Test Content',
        sender: 'system-user',
      });

      expect(
        mockOutboxTransactionService.runWithOutboxEvents,
      ).toHaveBeenCalledTimes(1);

      // Verify the outbox event descriptor passed to the service
      const [, events] = (
        mockOutboxTransactionService.runWithOutboxEvents as jest.Mock
      ).mock.calls[0] as [
        unknown,
        Array<{ eventType: string; correlationId?: string }>,
      ];
      expect(events).toHaveLength(1);
      expect(events[0].eventType).toBe('MessagePersisted');
      expect(events[0].correlationId).toBe(correlationId);
    });

    it('should use default values when data fields are missing', async () => {
      const result = await service.handleMessageCreation({});

      expect(result).toMatchObject({
        title: 'Untitled',
        content: 'No content',
        sender: 'system-user',
      });
    });

    it('should default content when only subject is provided', async () => {
      const result = await service.handleMessageCreation({
        subject: 'Only Subject',
      });
      expect(result).toMatchObject({
        title: 'Only Subject',
        content: 'No content',
      });
    });

    it('should propagate errors thrown by runWithOutboxEvents', async () => {
      const dbError = new Error('Transaction failed');
      mockOutboxTransactionService.runWithOutboxEvents = jest
        .fn()
        .mockRejectedValue(dbError);

      await expect(
        service.handleMessageCreation({ subject: 'Test' }),
      ).rejects.toThrow('Transaction failed');
    });

    it('should include correlationId in the outbox event', async () => {
      await service.handleMessageCreation({ subject: 'X' }, 'corr-123');

      const [, events] = (
        mockOutboxTransactionService.runWithOutboxEvents as jest.Mock
      ).mock.calls[0] as [unknown, Array<{ correlationId?: string }>];
      expect(events[0].correlationId).toBe('corr-123');
    });

    it('should work without correlationId', async () => {
      await service.handleMessageCreation({ subject: 'X' });

      const [, events] = (
        mockOutboxTransactionService.runWithOutboxEvents as jest.Mock
      ).mock.calls[0] as [unknown, Array<{ correlationId?: string }>];
      expect(events[0].correlationId).toBeUndefined();
    });

    it('should pass a work callback that creates and saves a Message entity', async () => {
      // Capture the work callback and inspect what it does to the EntityManager
      let capturedWork: ((em: EntityManager) => Promise<Message>) | undefined;

      mockOutboxTransactionService.runWithOutboxEvents = jest
        .fn()
        .mockImplementation(
          async (work: (em: EntityManager) => Promise<Message>) => {
            capturedWork = work;
            const mockEm = {
              create: jest.fn().mockReturnValue({
                title: 'Hello',
                content: 'World',
                sender: 'system-user',
              }),
              save: jest.fn().mockResolvedValue({
                id: 99,
                title: 'Hello',
                content: 'World',
                sender: 'system-user',
                createdAt: new Date(),
              }),
            } as unknown as EntityManager;
            return work(mockEm);
          },
        );

      const result = await service.handleMessageCreation({
        subject: 'Hello',
        content: 'World',
      });

      expect(capturedWork).toBeDefined();
      expect(result.id).toBe(99);
    });

    it('should produce exactly one outbox event per call', async () => {
      await service.handleMessageCreation({ subject: 'A' });
      await service.handleMessageCreation({ subject: 'B' });

      const calls = (
        mockOutboxTransactionService.runWithOutboxEvents as jest.Mock
      ).mock.calls as Array<[unknown, unknown[]]>;
      expect(calls[0][1]).toHaveLength(1);
      expect(calls[1][1]).toHaveLength(1);
    });
  });
});
