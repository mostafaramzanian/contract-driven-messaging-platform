import { z } from 'zod';

/**
 * Identifiers for services that can appear in an event's `source` field or
 * `trace` array. Kept as a plain string union (not a DB enum) since this is
 * a wire-contract concept, not a persistence concept.
 */
export const SERVICE_IDS = ['gateway', 'messaging'] as const;
export type ServiceId = (typeof SERVICE_IDS)[number];

export const serviceIdSchema = z.enum(SERVICE_IDS);

/**
 * Fields every versioned event contract must include, regardless of
 * payload shape. This is intentionally small: it is the lightweight trace
 * model described in docs/architecture.md, not a general-purpose tracing
 * system (no spans, no parent/child relationships, no external tracing
 * vendor).
 *
 * - eventId: unique per logical event (set once by the producer, never
 *   regenerated downstream). Used to detect duplicate delivery.
 * - correlationId: propagated from the inbound HTTP request (see
 *   CorrelationIdMiddleware in libs/common). Same value across every hop
 *   of a single request's event chain.
 * - timestamp: ISO-8601, set by the producer at emit time.
 * - source: the service that originally produced the event.
 * - trace: ordered list of service IDs the event has passed through.
 *   The producer sets this to `[source]`; each consumer that validates
 *   and accepts the event appends its own ID before acting on it.
 */
export const eventEnvelopeSchema = z.object({
  eventId: z.uuid(),
  correlationId: z.uuid(),
  timestamp: z.iso.datetime(),
  source: serviceIdSchema,
  trace: z.array(serviceIdSchema).min(1),
});

export type EventEnvelope = z.infer<typeof eventEnvelopeSchema>;
