import { Test, TestingModule } from '@nestjs/testing';
import { Logger } from '@nestjs/common';
import { getRepositoryToken } from '@nestjs/typeorm';
import type { Repository } from 'typeorm';
import { MessagingService } from './messaging.service';
import { Message } from './entities/message.entity';

describe('MessagingService', () => {
  let service: MessagingService;
  let mockMessageRepository: jest.Mocked<Repository<Message>>;

  beforeEach(async () => {
    mockMessageRepository = {
      count: jest.fn(),
      create: jest.fn(),
      save: jest.fn(),
    } as unknown as jest.Mocked<Repository<Message>>;

    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MessagingService,
        {
          provide: getRepositoryToken(Message),
          useValue: mockMessageRepository,
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

      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(mockMessageRepository.count).toHaveBeenCalled();
    });

    it('should log error when database connection fails', async () => {
      const error = new Error('Database connection failed');
      mockMessageRepository.count.mockRejectedValue(error);

      // Should not throw error, just log it
      await service.onModuleInit();
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(mockMessageRepository.count).toHaveBeenCalled();
    });
  });

  describe('handleMessageCreation', () => {
    it('should create and save message with provided data', async () => {
      const data = {
        subject: 'Test Subject',
        content: 'Test Content',
      };
      const correlationId = 'test-correlation-id';

      const mockMessage = {
        id: 1,
        title: 'Test Subject',
        content: 'Test Content',
        sender: 'system-user',
        createdAt: new Date(),
      };

      mockMessageRepository.create.mockReturnValue(mockMessage);
      mockMessageRepository.save.mockResolvedValue({ ...mockMessage, id: 1 });

      const result = await service.handleMessageCreation(data, correlationId);

      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(mockMessageRepository.create).toHaveBeenCalledWith({
        title: 'Test Subject',
        content: 'Test Content',
        sender: 'system-user',
      });
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(mockMessageRepository.save).toHaveBeenCalledWith(mockMessage);
      expect(result).toEqual({ ...mockMessage, id: 1 });
    });

    it('should create message with default values when data is missing', async () => {
      const data = {};
      const correlationId = 'test-correlation-id';

      const mockMessage = {
        id: 1,
        title: 'Untitled',
        content: 'No content',
        sender: 'system-user',
        createdAt: new Date(),
      };

      mockMessageRepository.create.mockReturnValue(mockMessage);
      mockMessageRepository.save.mockResolvedValue({ ...mockMessage, id: 1 });

      const result = await service.handleMessageCreation(data, correlationId);

      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(mockMessageRepository.create).toHaveBeenCalledWith({
        title: 'Untitled',
        content: 'No content',
        sender: 'system-user',
      });
      expect(result).toEqual({ ...mockMessage, id: 1 });
    });

    it('should handle partial data correctly', async () => {
      const data = {
        subject: 'Partial Subject',
        // content missing
      };
      const correlationId = 'test-correlation-id';

      const mockMessage = {
        id: 1,
        title: 'Partial Subject',
        content: 'No content',
        sender: 'system-user',
        createdAt: new Date(),
      };

      mockMessageRepository.create.mockReturnValue(mockMessage);
      mockMessageRepository.save.mockResolvedValue({ ...mockMessage, id: 1 });

      const result = await service.handleMessageCreation(data, correlationId);

      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(mockMessageRepository.create).toHaveBeenCalledWith({
        title: 'Partial Subject',
        content: 'No content',
        sender: 'system-user',
      });
      expect(result).toEqual({ ...mockMessage, id: 1 });
    });

    it('should throw error when repository save fails', async () => {
      const data = {
        subject: 'Test Subject',
        content: 'Test Content',
      };
      const correlationId = 'test-correlation-id';

      const error = new Error('Database save failed');
      mockMessageRepository.create.mockReturnValue({
        id: 1,
        title: '',
        content: '',
        sender: '',
        createdAt: new Date(),
      });
      mockMessageRepository.save.mockRejectedValue(error);

      await expect(
        service.handleMessageCreation(data, correlationId),
      ).rejects.toThrow(error);
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(mockMessageRepository.create).toHaveBeenCalled();
      // eslint-disable-next-line @typescript-eslint/unbound-method
      expect(mockMessageRepository.save).toHaveBeenCalled();
    });
  });
});
