import { context, propagation, type Context } from '@opentelemetry/api';
import type { MessagePropertyHeaders } from 'amqplib';

/**
 * AMQP trace-context propagation helpers.
 *
 * `@opentelemetry/instrumentation-amqplib` already injects/extracts the
 * W3C `traceparent` header automatically on `channel.publish` / `consume`.
 * These helpers exist for the code paths that build AMQP headers
 * *manually* (retry republish, DLQ headers) where we want explicit,
 * auditable control over which trace context is carried forward —
 * e.g. preserving the ORIGINAL producer trace across multiple retry
 * hops rather than letting it silently re-root on each republish.
 */

/** Inject the current active trace context into a plain headers object. */
export function injectTraceContext(
  headers: MessagePropertyHeaders = {},
): MessagePropertyHeaders {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return { ...headers, ...carrier };
}

/**
 * Capture the current active trace context as a bare W3C propagation
 * carrier (e.g. `{ traceparent: "00-..." }`), with no headers merged in.
 *
 * Used where the carrier needs to be stored independently of any AMQP
 * headers — e.g. `OutboxTransactionService` persists this alongside an
 * outbox row at write time, long before that row is published (and
 * before any AMQP headers object exists at all), so the relay can later
 * propagate the *original* producer's trace rather than its own
 * ambient, parentless context at publish time.
 */
export function captureTraceContextCarrier(): Record<string, string> {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return carrier;
}

/** Extract a trace context from AMQP message headers (for manual spans). */
export function extractTraceContext(
  headers: MessagePropertyHeaders | undefined,
): Context {
  const carrier: Record<string, string> = {};
  if (headers) {
    for (const [k, v] of Object.entries(headers)) {
      if (typeof v === 'string') carrier[k] = v;
    }
  }
  return propagation.extract(context.active(), carrier);
}
