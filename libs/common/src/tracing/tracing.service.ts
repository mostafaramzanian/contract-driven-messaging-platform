import { Injectable } from '@nestjs/common';
import {
  trace,
  context,
  SpanStatusCode,
  SpanKind,
  type Span,
  type Attributes,
} from '@opentelemetry/api';

export const TRACER_NAME = 'messaging-showcase';

/**
 * TracingService
 *
 * Thin wrapper around the OpenTelemetry API for use inside NestJS
 * providers/controllers without depending on the SDK package directly.
 * The actual SDK (exporters, instrumentations) is initialised once in
 * `otel-bootstrap.ts`, required at process start. This service only reads
 * the globally-registered tracer/context.
 */
@Injectable()
export class TracingService {
  private readonly tracer = trace.getTracer(TRACER_NAME);

  /** Current trace id (hex), or 'noop' if no span is active. */
  getTraceId(): string {
    const span = trace.getSpan(context.active());
    const spanContext = span?.spanContext();
    return spanContext && trace.isSpanContextValid(spanContext)
      ? spanContext.traceId
      : 'noop';
  }

  /** Current span id (hex), or 'noop' if no span is active. */
  getSpanId(): string {
    const span = trace.getSpan(context.active());
    const spanContext = span?.spanContext();
    return spanContext && trace.isSpanContextValid(spanContext)
      ? spanContext.spanId
      : 'noop';
  }

  /**
   * Run `fn` inside a new active span. Automatically records exceptions
   * and sets the span status; ends the span when `fn` settles.
   */
  async withSpan<T>(
    name: string,
    fn: (span: Span) => Promise<T>,
    options?: { kind?: SpanKind; attributes?: Attributes },
  ): Promise<T> {
    return this.tracer.startActiveSpan(
      name,
      { kind: options?.kind ?? SpanKind.INTERNAL, attributes: options?.attributes },
      async (span) => {
        try {
          const result = await fn(span);
          span.setStatus({ code: SpanStatusCode.OK });
          return result;
        } catch (err) {
          span.recordException(err as Error);
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: err instanceof Error ? err.message : String(err),
          });
          throw err;
        } finally {
          span.end();
        }
      },
    );
  }

  /** Add attributes to the currently active span, if any. */
  setAttributes(attributes: Attributes): void {
    trace.getSpan(context.active())?.setAttributes(attributes);
  }

  /** Add an event (point-in-time annotation) to the currently active span. */
  addEvent(name: string, attributes?: Attributes): void {
    trace.getSpan(context.active())?.addEvent(name, attributes);
  }
}
