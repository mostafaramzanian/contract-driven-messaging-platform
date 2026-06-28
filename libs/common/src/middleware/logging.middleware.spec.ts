import { Test, TestingModule } from '@nestjs/testing';
import { LoggingMiddleware } from './logging.middleware';
import { PinoLoggerService } from '../logger/pino-logger.service';

// ── Mocks ─────────────────────────────────────────────────────────────────────

type MockPino = {
  log: jest.Mock;
  warn: jest.Mock;
  error: jest.Mock;
  debug: jest.Mock;
};

function mockPino(): MockPino {
  return {
    log: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  };
}

function makeReq(
  overrides: Partial<{
    method: string;
    originalUrl: string;
    headers: Record<string, string>;
    socket: { remoteAddress: string };
  }> = {},
) {
  return {
    method: 'GET',
    originalUrl: '/api/test-rabbit',
    headers: {
      'x-correlation-id': 'cid-req-001',
      'user-agent': 'jest-supertest',
    },
    socket: { remoteAddress: '127.0.0.1' },
    ...overrides,
  };
}

function makeRes(statusCode = 200) {
  const listeners: Record<string, Array<() => void>> = {};
  return {
    statusCode,
    setHeader: jest.fn(),
    getHeader: jest.fn(),
    on: jest.fn((event: string, cb: () => void) => {
      listeners[event] = listeners[event] ?? [];
      listeners[event].push(cb);
    }),
    /** Test helper: simulate the response finishing */
    emit: (event: string) => listeners[event]?.forEach((fn) => fn()),
  };
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('LoggingMiddleware', () => {
  let middleware: LoggingMiddleware;
  let pino: MockPino;

  beforeEach(async () => {
    pino = mockPino();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        LoggingMiddleware,
        { provide: PinoLoggerService, useValue: pino },
      ],
    }).compile();

    middleware = module.get<LoggingMiddleware>(LoggingMiddleware);
  });

  afterEach(() => jest.clearAllMocks());

  // ── Request logging ───────────────────────────────────────────────────────

  it('logs the incoming request at debug level', () => {
    const req = makeReq();
    const res = makeRes(200);
    const next = jest.fn();

    middleware.use(req as never, res as never, next);

    expect(pino.debug).toHaveBeenCalledWith(
      'Incoming HTTP request',
      expect.objectContaining({
        correlationId: 'cid-req-001',
        method: 'GET',
        url: '/api/test-rabbit',
        userAgent: 'jest-supertest',
        traceId: 'noop',
        operation: 'http_request',
      }),
    );
    expect(next).toHaveBeenCalled();
  });

  // ── Response logging ──────────────────────────────────────────────────────

  it('logs a 200 response at info level on finish', () => {
    const req = makeReq();
    const res = makeRes(200);
    const next = jest.fn();

    middleware.use(req as never, res as never, next);
    res.emit('finish');

    expect(pino.log).toHaveBeenCalledWith(
      'HTTP response',
      expect.objectContaining({
        statusCode: 200,
        durationMs: expect.any(Number),
      }),
    );
    expect(pino.warn).not.toHaveBeenCalled();
    expect(pino.error).not.toHaveBeenCalled();
  });

  it('logs a 4xx response at warn level', () => {
    const req = makeReq();
    const res = makeRes(400);
    const next = jest.fn();

    middleware.use(req as never, res as never, next);
    res.emit('finish');

    expect(pino.warn).toHaveBeenCalledWith(
      'HTTP response 4xx',
      expect.objectContaining({ statusCode: 400 }),
    );
  });

  it('logs a 5xx response at error level', () => {
    const req = makeReq();
    const res = makeRes(500);
    const next = jest.fn();

    middleware.use(req as never, res as never, next);
    res.emit('finish');

    expect(pino.error).toHaveBeenCalledWith(
      'HTTP response 5xx',
      expect.objectContaining({ statusCode: 500 }),
    );
  });

  // ── correlationId extraction ──────────────────────────────────────────────

  it('reads correlationId from x-correlation-id header', () => {
    const req = makeReq({
      headers: { 'x-correlation-id': 'my-cid', 'user-agent': 'test' },
    });
    const res = makeRes(200);

    middleware.use(req as never, res as never, jest.fn());

    expect(pino.debug).toHaveBeenCalledWith(
      'Incoming HTTP request',
      expect.objectContaining({ correlationId: 'my-cid' }),
    );
  });

  it('falls back to "unknown" correlationId when header is absent', () => {
    const req = makeReq({ headers: { 'user-agent': 'test' } });
    const res = makeRes(200);

    middleware.use(req as never, res as never, jest.fn());

    expect(pino.debug).toHaveBeenCalledWith(
      'Incoming HTTP request',
      expect.objectContaining({ correlationId: 'unknown' }),
    );
  });

  // ── durationMs ────────────────────────────────────────────────────────────

  it('reports a non-negative durationMs in the response log', () => {
    const req = makeReq();
    const res = makeRes(200);

    middleware.use(req as never, res as never, jest.fn());
    res.emit('finish');

    const [, fields] = (pino.log as jest.Mock).mock.calls[0] as [
      string,
      Record<string, unknown>,
    ];
    expect(typeof fields['durationMs']).toBe('number');
    expect(fields['durationMs'] as number).toBeGreaterThanOrEqual(0);
  });

  // ── No-op without logger ──────────────────────────────────────────────────

  it('calls next() and does not throw when PinoLoggerService is not provided', () => {
    const noLogMiddleware = new LoggingMiddleware(
      undefined as unknown as PinoLoggerService,
    );
    const next = jest.fn();

    expect(() =>
      noLogMiddleware.use(makeReq() as never, makeRes() as never, next),
    ).not.toThrow();
    expect(next).toHaveBeenCalled();
  });
});
