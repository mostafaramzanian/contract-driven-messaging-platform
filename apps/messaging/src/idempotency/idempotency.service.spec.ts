import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import type { Repository } from 'typeorm';
import { IdempotencyService } from './idempotency.service';
import { ProcessedEvent } from '../entities/processed-event.entity';

/** Minimal factory for a mock TypeORM repository. */
function mockRepository(): jest.Mocked<
  Pick<Repository<ProcessedEvent>, 'create' | 'save' | 'findOne'>
> {
  return {
    create: jest.fn(),
    save: jest.fn(),
    findOne: jest.fn(),
  };
}

describe('IdempotencyService', () => {
  let service: IdempotencyService;
  let repo: ReturnType<typeof mockRepository>;

  beforeEach(async () => {
    repo = mockRepository();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        IdempotencyService,
        {
          provide: getRepositoryToken(ProcessedEvent),
          useValue: repo,
        },
      ],
    }).compile();

    service = module.get<IdempotencyService>(IdempotencyService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  // ── checkAndMark ──────────────────────────────────────────────────────────

  describe('checkAndMark', () => {
    const options = {
      eventId: '550e8400-e29b-41d4-a716-446655440000',
      eventType: 'CreateMessageEvent.v1',
      correlationId: '660e8400-e29b-41d4-a716-446655440001',
    };

    it('returns { isDuplicate: false } when the INSERT succeeds', async () => {
      const processedAt = new Date('2025-01-01T00:00:00Z');
      const entity = { ...options, processedAt, id: 1, result: undefined };

      (repo.create as jest.Mock).mockReturnValue(entity);
      (repo.save as jest.Mock).mockResolvedValue(entity);

      const result = await service.checkAndMark(options);

      expect(result.isDuplicate).toBe(false);
      expect(result.processedAt).toEqual(processedAt);
      expect(repo.create).toHaveBeenCalledWith({
        eventId: options.eventId,
        eventType: options.eventType,
        correlationId: options.correlationId,
        result: undefined,
      });
      expect(repo.save).toHaveBeenCalledWith(entity);
    });

    it('returns { isDuplicate: true } when PostgreSQL raises a 23505 unique violation', async () => {
      const existingRecord = {
        ...options,
        id: 1,
        processedAt: new Date('2024-12-01T00:00:00Z'),
        result: { messageId: 42 },
      };

      // Simulate the TypeORM unique-constraint error
      const uniqueViolationError = Object.assign(new Error('duplicate key'), {
        code: '23505',
      });

      (repo.create as jest.Mock).mockReturnValue({});
      (repo.save as jest.Mock).mockRejectedValue(uniqueViolationError);
      (repo.findOne as jest.Mock).mockResolvedValue(existingRecord);

      const result = await service.checkAndMark(options);

      expect(result.isDuplicate).toBe(true);
      if (result.isDuplicate) {
        expect(result.processedAt).toEqual(existingRecord.processedAt);
        expect(result.result).toEqual({ messageId: 42 });
      }
      expect(repo.findOne).toHaveBeenCalledWith({
        where: { eventId: options.eventId },
      });
    });

    it('also handles driverError.code shape for 23505', async () => {
      const driverError = { code: '23505' };
      const wrappedError = Object.assign(new Error('QueryFailedError'), {
        driverError,
      });

      (repo.create as jest.Mock).mockReturnValue({});
      (repo.save as jest.Mock).mockRejectedValue(wrappedError);
      (repo.findOne as jest.Mock).mockResolvedValue({
        ...options,
        id: 2,
        processedAt: new Date(),
        result: null,
      });

      const result = await service.checkAndMark(options);

      expect(result.isDuplicate).toBe(true);
    });

    it('propagates non-unique-violation errors', async () => {
      const connectionError = Object.assign(new Error('connection refused'), {
        code: 'ECONNREFUSED',
      });

      (repo.create as jest.Mock).mockReturnValue({});
      (repo.save as jest.Mock).mockRejectedValue(connectionError);

      await expect(service.checkAndMark(options)).rejects.toThrow(
        'connection refused',
      );
      // findOne must NOT be called — we only query for the existing record
      // when we know it's a duplicate (23505), not for other errors
      expect(repo.findOne).not.toHaveBeenCalled();
    });

    it('includes the optional result field when provided', async () => {
      const optionsWithResult = {
        ...options,
        result: { messageId: 7 },
      };
      const entity = {
        ...optionsWithResult,
        id: 10,
        processedAt: new Date(),
      };

      (repo.create as jest.Mock).mockReturnValue(entity);
      (repo.save as jest.Mock).mockResolvedValue(entity);

      await service.checkAndMark(optionsWithResult);

      expect(repo.create).toHaveBeenCalledWith(
        expect.objectContaining({ result: { messageId: 7 } }),
      );
    });
  });

  // ── findByEventId ─────────────────────────────────────────────────────────

  describe('findByEventId', () => {
    it('delegates to findOne with correct where clause', async () => {
      const eventId = '550e8400-e29b-41d4-a716-446655440000';
      const entity = {
        id: 1,
        eventId,
        eventType: 'CreateMessageEvent.v1',
        correlationId: null,
        processedAt: new Date(),
        result: null,
      };

      (repo.findOne as jest.Mock).mockResolvedValue(entity);

      const result = await service.findByEventId(eventId);

      expect(result).toEqual(entity);
      expect(repo.findOne).toHaveBeenCalledWith({ where: { eventId } });
    });

    it('returns null when event has not been processed', async () => {
      (repo.findOne as jest.Mock).mockResolvedValue(null);
      const result = await service.findByEventId('non-existent-id');
      expect(result).toBeNull();
    });
  });
});
