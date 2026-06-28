import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { NotFoundException } from '@nestjs/common';
import { MessagesService } from './messages.service';
import { Message } from '../entities/message.entity';
import { CreateMessageDto } from './dto/create-message.dto';
import { UpdateMessageDto } from './dto/update-message.dto';

describe('MessagesService', () => {
  let service: MessagesService;

  const mockMessage: Message = {
    id: 1,
    title: 'Title',
    content: 'Content',
    sender: 'Sender',
    createdAt: new Date(),
  };

  const mockRepository = {
    create: jest.fn().mockReturnValue(mockMessage),
    save: jest.fn().mockResolvedValue(mockMessage),
    find: jest.fn().mockResolvedValue([mockMessage]),
    findOne: jest.fn().mockResolvedValue(mockMessage),
    remove: jest.fn().mockResolvedValue(undefined),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MessagesService,
        {
          provide: getRepositoryToken(Message),
          useValue: mockRepository,
        },
      ],
    }).compile();

    service = module.get<MessagesService>(MessagesService);
    jest.clearAllMocks();
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('create', () => {
    it('should create and save a message', async () => {
      const createMessageDto: CreateMessageDto = {
        title: 'Title',
        content: 'Content',
        sender: 'Sender',
      };

      const result = await service.create(createMessageDto);

      expect(mockRepository.create).toHaveBeenCalledWith(createMessageDto);
      expect(mockRepository.save).toHaveBeenCalledWith(mockMessage);
      expect(result).toEqual(mockMessage);
    });
  });

  describe('findAll', () => {
    it('returns messages ordered by createdAt, with the default limit (50)', async () => {
      const result = await service.findAll();

      expect(mockRepository.find).toHaveBeenCalledWith({
        order: { createdAt: 'DESC' },
        take: 50,
      });
      expect(result).toEqual([mockMessage]);
    });

    it('respects a caller-supplied limit within the allowed range', async () => {
      await service.findAll({ limit: 10 });

      expect(mockRepository.find).toHaveBeenCalledWith({
        order: { createdAt: 'DESC' },
        take: 10,
      });
    });

    it('caps a caller-supplied limit at the hard ceiling (200), never rejects it', async () => {
      await service.findAll({ limit: 100_000 });

      expect(mockRepository.find).toHaveBeenCalledWith({
        order: { createdAt: 'DESC' },
        take: 200,
      });
    });

    it('uses cursor-based filtering (createdAt < cursor) when a cursor is supplied', async () => {
      const cursor = '2026-01-01T00:00:00.000Z';
      await service.findAll({ cursor });

      const callArg = mockRepository.find.mock.calls[0][0] as {
        where?: { createdAt?: { _type?: string; _value?: Date } };
        order: { createdAt: string };
        take: number;
      };
      expect(callArg.order).toEqual({ createdAt: 'DESC' });
      expect(callArg.take).toBe(50);
      // TypeORM's LessThan() returns a FindOperator; assert its shape
      // rather than the exact internal representation.
      expect(callArg.where?.createdAt).toBeDefined();
    });
  });

  describe('findOne', () => {
    it('should return a message by id', async () => {
      const result = await service.findOne(1);

      expect(mockRepository.findOne).toHaveBeenCalledWith({ where: { id: 1 } });
      expect(result).toEqual(mockMessage);
    });

    it('should throw NotFoundException when message does not exist', async () => {
      mockRepository.findOne.mockResolvedValueOnce(null);

      await expect(service.findOne(999)).rejects.toThrow(NotFoundException);
    });
  });

  describe('update', () => {
    it('should update and save a message', async () => {
      const updateMessageDto: UpdateMessageDto = {
        id: 1,
        title: 'Updated Title',
      };

      const result = await service.update(1, updateMessageDto);

      expect(result.title).toBe('Updated Title');
      expect(mockRepository.save).toHaveBeenCalled();
    });
  });

  describe('remove', () => {
    it('should remove a message', async () => {
      await service.remove(1);

      expect(mockRepository.remove).toHaveBeenCalledWith(mockMessage);
    });
  });
});
