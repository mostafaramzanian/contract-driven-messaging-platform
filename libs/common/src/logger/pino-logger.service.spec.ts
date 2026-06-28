import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { PinoLoggerService } from './pino-logger.service';

// ── pino mock ─────────────────────────────────────────────────────────────────
// We mock pino at the module level so we can spy on the logger methods
// without producing real log output during tests.

const mockChild = jest.fn();
const mockLoggerMethods = {
  info: jest.fn(),
  error: jest.fn(),
  warn: jest.fn(),
  debug: jest.fn(),
  trace: jest.fn(),
  fatal: jest.fn(),
  child: mockChild,
};

// child() returns the same mock so nested child logger calls are observable
mockChild.mockReturnValue(mockLoggerMethods);

jest.mock('pino', () => {
  const pinoFn = jest.fn(() => mockLoggerMethods);
  // Pino exports stdTimeFunctions and stdSerializers as named properties
  (pinoFn as unknown as Record<string, unknown>)['stdTimeFunctions'] = {
    isoTime: () => '',
  };
  (pinoFn as unknown as Record<string, unknown>)['stdSerializers'] = {
    err: (e: Error) => ({ message: e.message }),
    req: (r: unknown) => r,
    res: (r: unknown) => r,
  };
  return { default: pinoFn, ...pinoFn };
});

// ── Helpers ───────────────────────────────────────────────────────────────────

function buildModule(
  env: Record<string, string> = {},
): Promise<TestingModule> {
  return Test.createTestingModule({
    providers: [
      PinoLoggerService,
      {
        provide: ConfigService,
        useValue: {
          get: (key: string, defaultValue?: string) =>
            env[key] ?? defaultValue,
        },
      },
    ],
  }).compile();
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('PinoLoggerService', () => {
  let service: PinoLoggerService;

  beforeEach(async () => {
    jest.clearAllMocks();
    mockChild.mockReturnValue(mockLoggerMethods);

    const module = await buildModule({
      LOG_LEVEL: 'debug',
      NODE_ENV: 'test',
      SERVICE_NAME: 'messaging',
    });

    service = module.get<PinoLoggerService>(PinoLoggerService);
  });

  // ── NestLoggerService compatibility ────────────────────────────────────────

  describe('NestLoggerService interface', () => {
    it('log() calls pino.info with the message', () => {
      service.log('hello world');
      expect(mockLoggerMethods.info).toHaveBeenCalledWith(
        expect.any(Object),
        'hello world',
      );
    });

    it('log() accepts a string context as second arg (NestJS compat)', () => {
      service.log('handler started', 'MessagingController');
      expect(mockLoggerMethods.info).toHaveBeenCalledWith(
        expect.objectContaining({ context: 'MessagingController' }),
        'handler started',
      );
    });

    it('log() accepts a PinoBaseFields object as second arg', () => {
      service.log('event received', {
        correlationId: 'cid-123',
        eventId: 'eid-456',
        service: 'messaging',
        operation: 'handleMessage',
      });
      expect(mockLoggerMethods.info).toHaveBeenCalledWith(
        expect.objectContaining({
          correlationId: 'cid-123',
          eventId: 'eid-456',
          service: 'messaging',
          operation: 'handleMessage',
        }),
        'event received',
      );
    });

    it('error() calls pino.error', () => {
      service.error('something broke');
      expect(mockLoggerMethods.error).toHaveBeenCalledWith(
        expect.any(Object),
        'something broke',
      );
    });

    it('error() with stack and context (NestJS compat: error(msg, stack, context))', () => {
      service.error('db failed', 'Error: stack trace', 'MessagingService');
      expect(mockLoggerMethods.error).toHaveBeenCalledWith(
        expect.objectContaining({
          stack: 'Error: stack trace',
          context: 'MessagingService',
        }),
        'db failed',
      );
    });

    it('error() with structured fields object', () => {
      service.error('fatal error', {
        correlationId: 'cid-789',
        errorMessage: 'ECONNREFUSED',
      });
      expect(mockLoggerMethods.error).toHaveBeenCalledWith(
        expect.objectContaining({
          correlationId: 'cid-789',
          errorMessage: 'ECONNREFUSED',
        }),
        'fatal error',
      );
    });

    it('warn() calls pino.warn', () => {
      service.warn('retry scheduled', { attempt: 2, maxAttempts: 5 });
      expect(mockLoggerMethods.warn).toHaveBeenCalledWith(
        expect.objectContaining({ attempt: 2 }),
        'retry scheduled',
      );
    });

    it('debug() calls pino.debug', () => {
      service.debug('handler invoked', { operation: 'test' });
      expect(mockLoggerMethods.debug).toHaveBeenCalledWith(
        expect.objectContaining({ operation: 'test' }),
        'handler invoked',
      );
    });

    it('verbose() maps to pino.trace', () => {
      service.verbose('trace level message');
      expect(mockLoggerMethods.trace).toHaveBeenCalledWith(
        expect.any(Object),
        'trace level message',
      );
    });

    it('fatal() calls pino.fatal', () => {
      service.fatal('process shutting down');
      expect(mockLoggerMethods.fatal).toHaveBeenCalledWith(
        expect.any(Object),
        'process shutting down',
      );
    });
  });

  // ── child() ───────────────────────────────────────────────────────────────

  describe('child()', () => {
    it('returns a child pino logger bound with the supplied fields', () => {
      const child = service.child({
        correlationId: 'cid-abc',
        eventId: 'eid-xyz',
        traceId: 'noop',
        service: 'messaging',
        operation: 'handleMessage',
      });

      expect(mockChild).toHaveBeenCalledWith(
        expect.objectContaining({
          correlationId: 'cid-abc',
          eventId: 'eid-xyz',
        }),
      );
      expect(child).toBeDefined();
    });

    it('child loggers share the parent logger instance', () => {
      const child1 = service.child({ correlationId: 'c1' });
      const child2 = service.child({ correlationId: 'c2' });

      // Both should have been produced by the same underlying pino instance
      expect(mockChild).toHaveBeenCalledTimes(2);
      expect(child1).toBeDefined();
      expect(child2).toBeDefined();
    });
  });

  // ── Structured field correctness ──────────────────────────────────────────

  describe('structured fields', () => {
    it('includes all required structured fields when passed as an object', () => {
      service.log('event processed', {
        correlationId: 'cid-123',
        eventId: 'eid-456',
        messageId: '7',
        traceId: 'noop',
        service: 'messaging',
        operation: 'handleMessage',
      });

      const [fields] = (mockLoggerMethods.info as jest.Mock).mock.calls[0];
      expect(fields).toMatchObject({
        correlationId: 'cid-123',
        eventId: 'eid-456',
        messageId: '7',
        traceId: 'noop',
        service: 'messaging',
        operation: 'handleMessage',
      });
    });

    it('emits an empty bindings object when called with no context', () => {
      service.log('bare message');
      const [fields] = (mockLoggerMethods.info as jest.Mock).mock.calls[0];
      expect(fields).toEqual({});
    });
  });
});
