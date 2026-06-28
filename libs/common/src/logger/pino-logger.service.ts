import { Injectable, LoggerService as NestLoggerService } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { Logger } from 'pino';
import {
  createPinoLogger,
  type PinoBaseFields,
} from './pino.factory';

/**
 * PinoLoggerService
 *
 * Injectable NestJS logger that wraps a Pino `Logger` instance.
 * Implements `NestLoggerService` so it can be passed to
 * `app.useLogger(pinoLoggerService)` and replaces the built-in NestJS
 * logger transparently.
 *
 * ## Structured fields
 *
 * Every log record contains at minimum:
 *   - `service`     : injected at construction time
 *   - `traceId`     : 'noop' placeholder (ready for OpenTelemetry)
 *   - `level`       : pino level string
 *   - `time`        : ISO-8601 timestamp
 *   - `msg`         : the human-readable message
 *
 * Per-event fields (`correlationId`, `eventId`, `messageId`, `operation`)
 * are added by call sites that pass them as extra arguments.
 *
 * ## Usage
 *
 * ### Application-wide (main.ts)
 * ```ts
 * const pinoService = app.get(PinoLoggerService);
 * app.useLogger(pinoService);
 * ```
 *
 * ### Injected into a class
 * ```ts
 * constructor(private readonly logger: PinoLoggerService) {}
 *
 * this.logger.log('Event received', {
 *   correlationId: event.correlationId,
 *   eventId:       event.eventId,
 *   messageId:     String(msg.fields.deliveryTag),
 *   operation:     'handleMessage',
 * });
 * ```
 *
 * ### Compatibility with NestJS Logger.log(message, context?)
 * The second argument can also be a plain string (context name) to
 * maintain compatibility with existing code that calls `this.logger.log(msg, 'ClassName')`.
 */
@Injectable()
export class PinoLoggerService implements NestLoggerService {
  private readonly logger: Logger;

  constructor(private readonly configService: ConfigService) {
    const logLevel = this.configService.get<string>('LOG_LEVEL', 'info');
    const isPretty =
      this.configService.get<string>('NODE_ENV') !== 'production';
    const serviceName = this.configService.get<string>(
      'SERVICE_NAME',
      'messaging-showcase',
    );

    this.logger = createPinoLogger(logLevel, isPretty, {
      service: serviceName,
      traceId: 'noop',
    });
  }

  /**
   * Returns a child logger bound with the given per-request fields.
   * Use this in handlers that carry a correlationId / eventId so every
   * log line within that handler is tagged without repeating the fields.
   *
   * ```ts
   * const log = this.logger.child({ correlationId, eventId, operation: 'handleMessage' });
   * log.info('Processing');
   * ```
   */
  child(fields: PinoBaseFields & Record<string, unknown>): Logger {
    return this.logger.child(fields);
  }

  // ── NestLoggerService interface ──────────────────────────────────────

  /**
   * `context` may be:
   *   - a plain string (class name, for NestJS compatibility)
   *   - a PinoBaseFields object (structured extra fields)
   */
  log(
    message: string,
    contextOrFields?: string | (PinoBaseFields & Record<string, unknown>),
  ): void {
    this.logger.info(this.mergeFields(contextOrFields), message);
  }

  error(
    message: string,
    traceOrFields?: string | (PinoBaseFields & Record<string, unknown>),
    context?: string,
    correlationId?: string,
  ): void {
    let extra: Record<string, unknown> = {};

    if (typeof traceOrFields === 'string') {
      // Called as logger.error(msg, stack, context, correlationId)
      extra = {
        stack: traceOrFields,
        context,
        correlationId,
      };
    } else if (traceOrFields && typeof traceOrFields === 'object') {
      extra = traceOrFields;
    }

    this.logger.error(extra, message);
  }

  warn(
    message: string,
    contextOrFields?: string | (PinoBaseFields & Record<string, unknown>),
  ): void {
    this.logger.warn(this.mergeFields(contextOrFields), message);
  }

  debug(
    message: string,
    contextOrFields?: string | (PinoBaseFields & Record<string, unknown>),
  ): void {
    this.logger.debug(this.mergeFields(contextOrFields), message);
  }

  verbose(
    message: string,
    contextOrFields?: string | (PinoBaseFields & Record<string, unknown>),
  ): void {
    this.logger.trace(this.mergeFields(contextOrFields), message);
  }

  fatal(
    message: string,
    contextOrFields?: string | (PinoBaseFields & Record<string, unknown>),
  ): void {
    this.logger.fatal(this.mergeFields(contextOrFields), message);
  }

  // ── Private helpers ──────────────────────────────────────────────────

  private mergeFields(
    contextOrFields?: string | (PinoBaseFields & Record<string, unknown>),
  ): Record<string, unknown> {
    if (!contextOrFields) return {};
    if (typeof contextOrFields === 'string') {
      return { context: contextOrFields };
    }
    return contextOrFields;
  }
}
