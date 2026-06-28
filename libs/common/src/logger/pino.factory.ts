import pino, { type Logger, type LoggerOptions } from 'pino';
import { trace, context } from '@opentelemetry/api';

/**
 * Standard structured-log context fields injected into every log record.
 *
 * All fields are optional at the factory level; individual call sites
 * fill them in via child loggers or explicit bindings.
 *
 * Field semantics:
 *  - service     : identifies the NestJS application (gateway | messaging)
 *  - correlationId: propagated from the inbound HTTP x-correlation-id header
 *  - eventId     : unique event identifier from the event envelope
 *  - messageId   : AMQP deliveryTag (numeric) cast to string for logging
 *  - traceId     : placeholder for future distributed tracing integration
 *                  (e.g. OpenTelemetry trace ID).  Always emitted so
 *                  dashboards can be built without schema changes later.
 *  - operation   : name of the handler / method currently executing
 */
export interface PinoBaseFields {
  service?: string;
  correlationId?: string;
  eventId?: string;
  messageId?: string;
  /** Placeholder — emit 'noop' until a real tracer is wired. */
  traceId?: string;
  operation?: string;
}

/**
 * createPinoLogger
 *
 * Factory for a root Pino `Logger` instance.  Call once at bootstrap
 * and share the instance across the application; child loggers (bound
 * with per-request fields) are created from it via `logger.child(fields)`.
 *
 * @param logLevel  Pino log level string (trace|debug|info|warn|error|fatal).
 *                  Defaults to 'info'.
 * @param pretty    Enables pino-pretty console formatting for development.
 *                  In production pass `false` to get raw NDJSON.
 * @param baseFields Default fields merged into every log record produced
 *                   by the root logger (e.g. `{ service: 'messaging' }`).
 */
export function createPinoLogger(
  logLevel: string = 'info',
  pretty: boolean = false,
  baseFields: PinoBaseFields = {},
): Logger {
  // Pino serialisers: rename common fields for operator clarity.
  const serializers: LoggerOptions['serializers'] = {
    // Prevent raw Error objects from being serialised as `{}`.
    err: pino.stdSerializers.err,
    error: pino.stdSerializers.err,
    // Request/response serialisers for HTTP gateway logging
    req: pino.stdSerializers.req,
    res: pino.stdSerializers.res,
  };

  // The base object is merged into EVERY log record emitted by this
  // logger or any child created from it.
  const base: Record<string, unknown> = {
    // `pid` and `hostname` are included by Pino by default.
    // We add our own invariant fields on top:
    traceId: baseFields.traceId ?? 'noop', // placeholder for OTel later
    ...baseFields,
  };

  const transport: LoggerOptions['transport'] = pretty
    ? {
        target: 'pino-pretty',
        options: {
          colorize: true,
          translateTime: 'SYS:yyyy-mm-dd HH:MM:ss',
          ignore: 'pid,hostname',
          // Emit our structured fields on the same line as the message
          messageFormat:
            '{levelLabel} [{service}] [{correlationId}] {msg}',
          singleLine: false,
        },
      }
    : undefined; // no transport in production → raw NDJSON to stdout

  return pino({
    level: logLevel,
    base,
    serializers,
    timestamp: pino.stdTimeFunctions.isoTime,
    transport,
    // Dynamically attach the active OpenTelemetry trace/span id to every
    // log record. Falls back to 'noop' when no span is active (e.g. during
    // bootstrap, before any request/message is being processed).
    mixin() {
      const span = trace.getSpan(context.active());
      const spanContext = span?.spanContext();
      if (spanContext && trace.isSpanContextValid(spanContext)) {
        return { traceId: spanContext.traceId, spanId: spanContext.spanId };
      }
      return { traceId: 'noop' };
    },
    // Prevent Pino from calling process.exit on fatal
    onFatalError: (err) => {
      // eslint-disable-next-line no-console
      console.error('[pino] fatal error', err);
    },
  });
}

/**
 * createChildLogger
 *
 * Convenience wrapper that creates a child logger from an existing
 * root logger, binding the given fields so they appear in every
 * record emitted through the child.
 *
 * Usage:
 * ```ts
 * const log = createChildLogger(rootLogger, {
 *   correlationId: event.correlationId,
 *   eventId:       event.eventId,
 *   messageId:     String(msg.fields.deliveryTag),
 *   operation:     'handleMessage',
 *   service:       'messaging',
 * });
 * log.info('Event received');
 * ```
 */
export function createChildLogger(
  parent: Logger,
  fields: PinoBaseFields & Record<string, unknown>,
): Logger {
  return parent.child(fields);
}
