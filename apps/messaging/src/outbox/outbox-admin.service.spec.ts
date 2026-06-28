import { NotFoundException } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { getDataSourceToken } from '@nestjs/typeorm';
import { OutboxAdminService } from './outbox-admin.service';

// ── Helpers ────────────────────────────────────────────────────────────────

/** Build a minimal mock DataSource with a configurable query() response. */
function buildDataSourceMock(queryResult: unknown = []) {
  return {
    query: jest.fn().mockResolvedValue(queryResult),
  };
}

// ── Tests ──────────────────────────────────────────────────────────────────

describe('OutboxAdminService', () => {
  let service: OutboxAdminService;
  let mockDataSource: { query: jest.Mock };

  beforeEach(async () => {
    mockDataSource = buildDataSourceMock();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OutboxAdminService,
        {
          provide: getDataSourceToken(),
          useValue: mockDataSource,
        },
      ],
    }).compile();

    service = module.get<OutboxAdminService>(OutboxAdminService);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  // ── replayById ────────────────────────────────────────────────────────

  describe('replayById', () => {
    it('issues the correct UPDATE SQL and returns replayed=1 when the row exists', async () => {
      mockDataSource.query.mockResolvedValue([{ id: 42 }]);

      const result = await service.replayById(42);

      expect(result).toEqual({ replayed: 1, ids: [42] });

      // Verify the query was called once with the right id parameter

      expect(mockDataSource.query).toHaveBeenCalledTimes(1);

      const [sql, params] = mockDataSource.query.mock.calls[0] as [
        string,
        unknown[],
      ];

      // Must filter on both id and status='failed'
      expect(sql).toMatch(/WHERE\s+id\s+=\s+\$1/i);
      expect(sql).toMatch(/status\s*=\s*'failed'/i);

      // Must reset the key columns
      expect(sql).toMatch(/status\s*=\s*'pending'/i);
      expect(sql).toMatch(/attempts\s*=\s*0/i);
      expect(sql).toMatch(/next_retry_at\s*=\s*now\(\)/i);
      expect(sql).toMatch(/locked_at\s*=\s*NULL/i);
      expect(sql).toMatch(/locked_by\s*=\s*NULL/i);

      // Must RETURNING id so we know which rows were affected
      expect(sql).toMatch(/RETURNING\s+id/i);

      // id bound as $1
      expect(params).toEqual([42]);
    });

    it('throws NotFoundException when the row does not exist', async () => {
      mockDataSource.query.mockResolvedValue([]);

      await expect(service.replayById(99)).rejects.toThrow(NotFoundException);
      await expect(service.replayById(99)).rejects.toThrow(/id=99/);
    });

    it('throws NotFoundException when the row exists but is not failed', async () => {
      // The WHERE clause filters status='failed'; if the row is 'pending'
      // Postgres returns 0 rows → same empty-array result
      mockDataSource.query.mockResolvedValue([]);

      await expect(service.replayById(7)).rejects.toThrow(NotFoundException);
    });

    it('propagates DataSource errors', async () => {
      const dbErr = new Error('connection lost');
      mockDataSource.query.mockRejectedValue(dbErr);

      await expect(service.replayById(1)).rejects.toThrow('connection lost');
    });
  });

  // ── replayAllFailed ───────────────────────────────────────────────────

  describe('replayAllFailed', () => {
    it('resets all failed rows when called without options', async () => {
      mockDataSource.query.mockResolvedValue([{ id: 1 }, { id: 2 }, { id: 3 }]);

      const result = await service.replayAllFailed();

      expect(result).toEqual({ replayed: 3, ids: [1, 2, 3] });

      const [sql, params] = mockDataSource.query.mock.calls[0] as [
        string,
        unknown[] | undefined,
      ];

      // Must reset key columns
      expect(sql).toMatch(/status\s*=\s*'pending'/i);
      expect(sql).toMatch(/attempts\s*=\s*0/i);
      expect(sql).toMatch(/next_retry_at\s*=\s*now\(\)/i);

      // Must filter on status='failed'
      expect(sql).toMatch(/WHERE\s+status\s*=\s*'failed'/i);

      // RETURNING id
      expect(sql).toMatch(/RETURNING\s+id/i);

      // No event_type parameter when not filtered
      expect(params).toBeUndefined();
    });

    it('filters by eventType when provided', async () => {
      mockDataSource.query.mockResolvedValue([{ id: 5 }, { id: 8 }]);

      const result = await service.replayAllFailed({
        eventType: 'MessagePersisted',
      });

      expect(result).toEqual({ replayed: 2, ids: [5, 8] });

      const [sql, params] = mockDataSource.query.mock.calls[0] as [
        string,
        unknown[],
      ];

      // Must include event_type filter
      expect(sql).toMatch(/event_type\s*=\s*\$1/i);

      // eventType bound as $1
      expect(params).toEqual(['MessagePersisted']);
    });

    it('returns replayed=0 when no failed rows exist', async () => {
      mockDataSource.query.mockResolvedValue([]);

      const result = await service.replayAllFailed();

      expect(result).toEqual({ replayed: 0, ids: [] });
    });

    it('returns replayed=0 when no failed rows match the eventType filter', async () => {
      mockDataSource.query.mockResolvedValue([]);

      const result = await service.replayAllFailed({
        eventType: 'NonExistentEvent',
      });

      expect(result).toEqual({ replayed: 0, ids: [] });
    });

    it('propagates DataSource errors', async () => {
      const dbErr = new Error('query timeout');
      mockDataSource.query.mockRejectedValue(dbErr);

      await expect(service.replayAllFailed()).rejects.toThrow('query timeout');
    });
  });
});
