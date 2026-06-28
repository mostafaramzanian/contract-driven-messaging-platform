import { Test, TestingModule } from '@nestjs/testing';
import { ExecutionContext } from '@nestjs/common';
import { of, throwError } from 'rxjs';
import { LoggingInterceptor } from './logging.interceptor';
import { PinoLoggerService } from '@app/common';

// ── Mock PinoLoggerService ────────────────────────────────────────────────────

function mockPinoService(): jest.Mocked<
  Pick<PinoLoggerService, 'log' | 'error' | 'warn' | 'debug' | 'child'>
> {
  const childMock = jest.fn().mockReturnThis();
  return {
    log: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    debug: jest.fn(),
    child: childMock,
  };
}

// ── Mock ExecutionContext ─────────────────────────────────────────────────────

function rpcContext(
  data: Record<string, unknown> = {},
  pattern: string = 'CreateMessageEvent.v1',
): ExecutionContext {
  return {
    getType: () => 'rpc',
    getClass: () => ({ name: 'MessagingController' }),
    getHandler: () => ({ name: 'handleMessage' }),
    switchToRpc: () => ({
      getData: () => data,
      getContext: () => ({
        getPattern: () => pattern,
        getMessage: () => ({
          fields: { deliveryTag: 42 },
          properties: {
            headers: { 'x-correlation-id': data['correlationId'] ?? 'unknown' },
          },
        }),
      }),
    }),
  } as unknown as ExecutionContext;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('LoggingInterceptor', () => {
  let interceptor: LoggingInterceptor;
  let pinoService: ReturnType<typeof mockPinoService>;

  beforeEach(async () => {
    pinoService = mockPinoService();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        LoggingInterceptor,
        { provide: PinoLoggerService, useValue: pinoService },
      ],
    }).compile();

    interceptor = module.get<LoggingInterceptor>(LoggingInterceptor);
  });

  afterEach(() => jest.clearAllMocks());

  // ── Happy path ────────────────────────────────────────────────────────────

  it('emits a debug log before the handler and an info log after success', (done) => {
    const context = rpcContext({
      correlationId: 'cid-111',
      eventId: 'eid-222',
    });

    const next = { handle: () => of({ id: 1 }) };

    interceptor.intercept(context, next).subscribe({
      next: (value) => {
        expect(value).toEqual({ id: 1 });
      },
      complete: () => {
        expect(pinoService.debug).toHaveBeenCalledWith(
          'RMQ handler invoked',
          expect.objectContaining({
            correlationId: 'cid-111',
            eventId: 'eid-222',
            messageId: '42',
            traceId: 'noop',
            service: 'messaging',
            operation: 'MessagingController.handleMessage',
            pattern: 'CreateMessageEvent.v1',
          }),
        );

        expect(pinoService.log).toHaveBeenCalledWith(
          'RMQ handler completed',
          expect.objectContaining({
            success: true,
            durationMs: expect.any(Number),
          }),
        );
        done();
      },
    });
  });

  // ── Error path ────────────────────────────────────────────────────────────

  it('emits an error log and re-throws when the handler fails', (done) => {
    const context = rpcContext({
      correlationId: 'cid-333',
      eventId: 'eid-444',
    });
    const boom = new Error('database connection lost');
    const next = { handle: () => throwError(() => boom) };

    interceptor.intercept(context, next).subscribe({
      error: (err: Error) => {
        expect(err).toBe(boom);

        expect(pinoService.error).toHaveBeenCalledWith(
          'RMQ handler threw an error',
          expect.objectContaining({
            success: false,
            errorMessage: 'database connection lost',
            durationMs: expect.any(Number),
          }),
        );
        done();
      },
    });
  });

  // ── Fields extracted from RPC context ────────────────────────────────────

  it('extracts eventId and correlationId from the RPC data payload', (done) => {
    const context = rpcContext({
      eventId: 'my-event-id',
      correlationId: 'my-cid',
    });
    const next = { handle: () => of(null) };

    interceptor.intercept(context, next).subscribe({
      complete: () => {
        expect(pinoService.debug).toHaveBeenCalledWith(
          'RMQ handler invoked',
          expect.objectContaining({
            eventId: 'my-event-id',
            correlationId: 'my-cid',
          }),
        );
        done();
      },
    });
  });

  it('uses messageId from AMQP deliveryTag', (done) => {
    const context = rpcContext({});
    const next = { handle: () => of(null) };

    interceptor.intercept(context, next).subscribe({
      complete: () => {
        expect(pinoService.debug).toHaveBeenCalledWith(
          'RMQ handler invoked',
          expect.objectContaining({ messageId: '42' }),
        );
        done();
      },
    });
  });

  // ── No-op when PinoLoggerService not injected ─────────────────────────────

  it('passes through without logging when PinoLoggerService is not provided', (done) => {
    const rawInterceptor = new LoggingInterceptor(
      undefined as unknown as PinoLoggerService,
    );

    const context = rpcContext({});
    const next = { handle: () => of('value') };

    rawInterceptor.intercept(context, next).subscribe({
      next: (v) => {
        expect(v).toBe('value');
      },
      complete: done,
    });
  });

  // ── durationMs is a non-negative number ──────────────────────────────────

  it('records a non-negative durationMs on success', (done) => {
    const context = rpcContext({});
    const next = { handle: () => of(null) };

    interceptor.intercept(context, next).subscribe({
      complete: () => {
        const [, fields] = (pinoService.log as jest.Mock).mock.calls[0] as [
          string,
          Record<string, unknown>,
        ];
        expect(typeof fields['durationMs']).toBe('number');
        expect(fields['durationMs'] as number).toBeGreaterThanOrEqual(0);
        done();
      },
    });
  });
});
