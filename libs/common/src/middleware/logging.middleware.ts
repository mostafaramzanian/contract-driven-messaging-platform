import {
  Injectable,
  NestMiddleware,
  Inject,
  Optional,
} from '@nestjs/common';
import type { Request, Response, NextFunction } from 'express';
import { PinoLoggerService } from '../logger/pino-logger.service';
import { CORRELATION_ID_HEADER } from '../logger/correlation-id.middleware';

/**
 * LoggingMiddleware
 *
 * Logs every inbound HTTP request and its corresponding response using
 * Pino structured fields.  Registered in the gateway's `AppModule`
 * (and optionally the messaging service's HTTP server when it exposes
 * health endpoints).
 *
 * ## Fields logged on REQUEST (at debug level):
 *   - correlationId
 *   - traceId       ('noop' placeholder)
 *   - service
 *   - operation     ('http_request')
 *   - method        HTTP verb
 *   - url           full path + query string
 *   - userAgent     request User-Agent header
 *   - remoteIp      client IP
 *
 * ## Fields logged on RESPONSE (at info level):
 *   - All request fields above, PLUS:
 *   - statusCode    HTTP response status
 *   - durationMs    wall-clock time from request start to response finish
 *
 * ## Why a custom middleware instead of pino-http?
 *
 * `pino-http` is excellent but requires direct access to the Node HTTP
 * server at bootstrap, which conflicts with NestJS's `app.useLogger()`
 * pattern.  This middleware sits within the NestJS DI container, receives
 * `PinoLoggerService` by injection, and therefore works correctly with
 * hybrid apps (HTTP + microservice transports) and is testable with
 * NestJS's `TestingModule`.
 */
@Injectable()
export class LoggingMiddleware implements NestMiddleware {
  constructor(
    @Optional()
    @Inject(PinoLoggerService)
    private readonly pinoLogger: PinoLoggerService,
  ) {}

  use(req: Request, res: Response, next: NextFunction): void {
    // If PinoLoggerService is not available (test context without DI),
    // fall through silently
    if (!this.pinoLogger) {
      next();
      return;
    }

    const startNs = process.hrtime.bigint();

    const correlationId =
      (req.headers[CORRELATION_ID_HEADER] as string) ?? 'unknown';

    const requestFields = {
      correlationId,
      operation: 'http_request',
      method: req.method,
      url: req.originalUrl ?? req.url,
      userAgent: req.headers['user-agent'] ?? 'unknown',
      remoteIp:
        (req.headers['x-forwarded-for'] as string) ??
        req.socket?.remoteAddress ??
        'unknown',
    };

    // Log at debug so request-start noise is suppressed at info level
    this.pinoLogger.debug('Incoming HTTP request', requestFields);

    // Attach a finish listener so we log after the response is sent
    res.on('finish', () => {
      const durationNs = process.hrtime.bigint() - startNs;
      const durationMs = Number(durationNs / BigInt(1_000_000));

      const responseFields = {
        ...requestFields,
        statusCode: res.statusCode,
        durationMs,
      };

      // Use warn for 4xx/5xx so they surface at normal log levels
      if (res.statusCode >= 500) {
        this.pinoLogger.error('HTTP response 5xx', responseFields);
      } else if (res.statusCode >= 400) {
        this.pinoLogger.warn('HTTP response 4xx', responseFields);
      } else {
        this.pinoLogger.log('HTTP response', responseFields);
      }
    });

    next();
  }
}
