import { Test, TestingModule } from '@nestjs/testing';
import {
  HealthCheckService,
  TypeOrmHealthIndicator,
  HealthCheckError,
} from '@nestjs/terminus';
import { MessagingHealthController } from './health.controller';
import { RabbitMQHealthIndicator } from './rabbitmq-health.indicator';

// ── Shared mocks ──────────────────────────────────────────────────────────────

const mockDbPingCheck = jest.fn();
const mockRmqIsHealthy = jest.fn();
const mockHealthCheck = jest.fn();

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('MessagingHealthController', () => {
  let controller: MessagingHealthController;

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      controllers: [MessagingHealthController],
      providers: [
        {
          provide: HealthCheckService,
          useValue: { check: mockHealthCheck },
        },
        {
          provide: TypeOrmHealthIndicator,
          useValue: { pingCheck: mockDbPingCheck },
        },
        {
          provide: RabbitMQHealthIndicator,
          useValue: { isHealthy: mockRmqIsHealthy },
        },
      ],
    }).compile();

    controller = module.get<MessagingHealthController>(
      MessagingHealthController,
    );
  });

  // ── readiness ─────────────────────────────────────────────────────────────

  describe('readiness()', () => {
    it('delegates to health.check with database and rabbitmq indicators', async () => {
      const healthResponse = {
        status: 'ok',
        info: {
          database: { status: 'up' },
          rabbitmq: { status: 'up' },
        },
        error: {},
        details: {
          database: { status: 'up' },
          rabbitmq: { status: 'up' },
        },
      };

      mockHealthCheck.mockResolvedValue(healthResponse);
      mockDbPingCheck.mockResolvedValue({ database: { status: 'up' } });
      mockRmqIsHealthy.mockResolvedValue({ rabbitmq: { status: 'up' } });

      const result = await controller.readiness();

      expect(result).toEqual(healthResponse);
      // health.check was called with an array of two thunks
      expect(mockHealthCheck).toHaveBeenCalledWith(
        expect.arrayContaining([expect.any(Function), expect.any(Function)]),
      );
    });

    it('surfaces a HealthCheckError when the database is down', async () => {
      const dbError = new HealthCheckError('database failed', {
        database: { status: 'down' },
      });
      mockHealthCheck.mockRejectedValue(dbError);

      await expect(controller.readiness()).rejects.toBeInstanceOf(
        HealthCheckError,
      );
    });

    it('surfaces a HealthCheckError when RabbitMQ is down', async () => {
      const rmqError = new HealthCheckError('rabbitmq failed', {
        rabbitmq: { status: 'down' },
      });
      mockHealthCheck.mockRejectedValue(rmqError);

      await expect(controller.readiness()).rejects.toBeInstanceOf(
        HealthCheckError,
      );
    });

    it('calls the database indicator thunk with key "database"', async () => {
      mockHealthCheck.mockImplementation(
        async (checks: Array<() => unknown>) => {
          // Execute each thunk so we can assert on the indicator calls
          for (const check of checks) await check();
          return { status: 'ok', info: {}, error: {}, details: {} };
        },
      );

      mockDbPingCheck.mockResolvedValue({ database: { status: 'up' } });
      mockRmqIsHealthy.mockResolvedValue({ rabbitmq: { status: 'up' } });

      await controller.readiness();

      expect(mockDbPingCheck).toHaveBeenCalledWith('database');
    });

    it('calls the RabbitMQ indicator thunk with key "rabbitmq"', async () => {
      mockHealthCheck.mockImplementation(
        async (checks: Array<() => unknown>) => {
          for (const check of checks) await check();
          return { status: 'ok', info: {}, error: {}, details: {} };
        },
      );

      mockDbPingCheck.mockResolvedValue({ database: { status: 'up' } });
      mockRmqIsHealthy.mockResolvedValue({ rabbitmq: { status: 'up' } });

      await controller.readiness();

      expect(mockRmqIsHealthy).toHaveBeenCalledWith('rabbitmq');
    });
  });

  // ── liveness ──────────────────────────────────────────────────────────────

  describe('liveness()', () => {
    it('delegates to health.check with an empty checks array', async () => {
      const liveResponse = {
        status: 'ok',
        info: {},
        error: {},
        details: {},
      };
      mockHealthCheck.mockResolvedValue(liveResponse);

      const result = await controller.liveness();

      expect(result).toEqual(liveResponse);
      // The empty array ensures no I/O is performed for liveness
      expect(mockHealthCheck).toHaveBeenCalledWith([]);
    });

    it('does NOT call database or RabbitMQ indicators', async () => {
      mockHealthCheck.mockImplementation(
        async (checks: Array<() => unknown>) => {
          for (const check of checks) await check();
          return { status: 'ok', info: {}, error: {}, details: {} };
        },
      );

      await controller.liveness();

      expect(mockDbPingCheck).not.toHaveBeenCalled();
      expect(mockRmqIsHealthy).not.toHaveBeenCalled();
    });
  });
});
