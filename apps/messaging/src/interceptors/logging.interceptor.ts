import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
  Inject,
  Optional,
} from '@nestjs/common';
import type { Observable } from 'rxjs';
import { tap, catchError } from 'rxjs/operators';
import { throwError } from 'rxjs';
import type * as amqplib from 'amqplib';
import { PinoLoggerService } from '@app/common';
import { CORRELATION_ID_HEADER } from '@app/common';

/**
 * LoggingInterceptor
 *
 * Applied globally in the messaging service (registered in main.ts via
 * `app.useGlobalInterceptors(new LoggingInterceptor(pinoLoggerService))`).
 *
 * ## What it logs
 *
 * BEFORE each handler invocation (debug level):
 *   - correlationId   from AMQP headers or request
 *   - eventId         extracted from the message payload (best-effort)
 *   - messageId       AMQP delivery tag, cast to string
 *   - traceId         'noop' placeholder
 *   - service         'messaging'
 *   - operation       handler class + method name
 *   - pattern         the RMQ pattern / route matched
 *
 * AFTER successful completion (info level):
 *   - All above fields, plus:
 *   - durationMs      wall-clock handler time
 *   - success: true
 *
 * ON ERROR (error level):
 *   - All above fields, plus:
 *   - durationMs
 *   - success: false
 *   - errorMessage
 *   - errorStack
 *
 * ## Design notes
 *
 * We deliberately do NOT re-throw errors in this interceptor beyond what
 * the RxJS `catchError` provides; the interceptor's role is observation,
 * not recovery.  The controller is still responsible for acking/nacking
 * AMQP messages.
 *
 * The `messageId` is the AMQP delivery tag (numeric), cast to string for
 * JSON log compatibility.  It uniquely identifies the delivery within the
 * channel's lifetime but is reset on reconnect — it is NOT the same as
 * `eventId`.
 */
@Injectable()
export class LoggingInterceptor implements NestInterceptor {
  constructor(
    @Optional()
    @Inject(PinoLoggerService)
    private readonly pinoLogger: PinoLoggerService,
  ) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    if (!this.pinoLogger) {
      return next.handle();
    }

    const startNs = process.hrtime.bigint();

    // ── Extract contextual identifiers ────────────────────────────────
    const { correlationId, eventId, messageId, pattern } =
      this.extractRmqContext(context);

    const handlerClass = context.getClass()?.name ?? 'UnknownClass';
    const handlerMethod = context.getHandler()?.name ?? 'unknownMethod';
    const operation = `${handlerClass}.${handlerMethod}`;

    const baseFields = {
      correlationId,
      eventId,
      messageId,
      service: 'messaging',
      operation,
      pattern,
    };

    this.pinoLogger.debug('RMQ handler invoked', baseFields);

    return next.handle().pipe(
      tap(() => {
        const durationMs = this.elapsedMs(startNs);
        this.pinoLogger.log('RMQ handler completed', {
          ...baseFields,
          durationMs,
          success: true,
        });
      }),
      catchError((err: unknown) => {
        const durationMs = this.elapsedMs(startNs);
        const errorMessage = err instanceof Error ? err.message : String(err);
        const errorStack = err instanceof Error ? err.stack : undefined;

        this.pinoLogger.error('RMQ handler threw an error', {
          ...baseFields,
          durationMs,
          success: false,
          errorMessage,
          errorStack,
        });

        return throwError(() => err);
      }),
    );
  }

  // ── Private helpers ──────────────────────────────────────────────────

  private elapsedMs(startNs: bigint): number {
    return Number((process.hrtime.bigint() - startNs) / BigInt(1_000_000));
  }

  /**
   * Best-effort extraction of structured fields from the NestJS
   * ExecutionContext for a microservice (RMQ) handler.
   *
   * In RMQ transport the ExecutionContext wraps `{ pattern, data }` in the
   * `switchToRpc()` interface; the raw AMQP message is accessible via the
   * context argument injected by @Ctx() in the controller, but at the
   * interceptor level we only have the RPC context object.
   *
   * We extract what we can without depending on controller-specific
   * knowledge of the payload structure.
   */
  private extractRmqContext(context: ExecutionContext): {
    correlationId: string;
    eventId: string;
    messageId: string;
    pattern: string;
  } {
    let correlationId = 'unknown';
    let eventId = 'unknown';
    let messageId = 'unknown';
    let pattern = 'unknown';

    try {
      if (context.getType() === 'rpc') {
        const rpcCtx = context.switchToRpc();
        const data = rpcCtx.getData<Record<string, unknown>>();
        const ctx = rpcCtx.getContext<{
          getMessage?: () => amqplib.ConsumeMessage;
          getPattern?: () => string;
        }>();

        // Extract pattern (RMQ routing key / pattern name)
        if (typeof ctx?.getPattern === 'function') {
          pattern = String(ctx.getPattern());
        }

        // Extract AMQP message metadata
        if (typeof ctx?.getMessage === 'function') {
          const msg = ctx.getMessage();
          if (msg) {
            messageId = String(msg.fields?.deliveryTag ?? 'unknown');

            const headers: Record<string, unknown> =
              msg.properties?.headers ?? {};
            const headerCid = headers[CORRELATION_ID_HEADER];
            if (typeof headerCid === 'string') {
              correlationId = headerCid;
            }
            const headerXCid = headers['x-correlation-id'];
            if (typeof headerXCid === 'string') {
              correlationId = headerXCid;
            }
          }
        }

        // Best-effort payload extraction
        if (data && typeof data === 'object') {
          if (typeof data['correlationId'] === 'string') {
            correlationId = data['correlationId'];
          }
          if (typeof data['eventId'] === 'string') {
            eventId = data['eventId'];
          }
        }
      } else if (context.getType() === 'http') {
        // Health endpoint or HTTP request
        const req = context
          .switchToHttp()
          .getRequest<{ headers: Record<string, string | undefined> }>();
        correlationId =
          req.headers[CORRELATION_ID_HEADER] ??
          req.headers['x-correlation-id'] ??
          'unknown';
        pattern = `${context.getClass()?.name}.${context.getHandler()?.name}`;
      }
    } catch {
      // Never let interceptor failures propagate into the handler
    }

    return { correlationId, eventId, messageId, pattern };
  }
}
