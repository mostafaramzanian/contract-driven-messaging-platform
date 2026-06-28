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
 * Schema versions this platform has ever defined for an event family.
 * Kept as a flat string union (not derived from EventRegistry) so the
 * envelope module has no dependency on per-event-type schema files —
 * envelope.schema.ts must stay a leaf module that every v1/v2/v3 schema
 * imports *from*, never the reverse.
 *
 * '1' is included even though v1 predates this field's existence (see
 * `schemaVersion` below) so that code which *does* have a schemaVersion
 * value in hand (e.g. an upcaster, or the AMQP header mirror) can express
 * "this is v1" without a magic string.
 */
export const SCHEMA_VERSIONS = ['1', '2'] as const;
export type SchemaVersion = (typeof SCHEMA_VERSIONS)[number];

export const schemaVersionSchema = z.enum(SCHEMA_VERSIONS);

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
 * - schemaVersion: OPTIONAL at the shared-envelope level, and that is
 *   deliberate. `CreateMessageEvent.v1` was defined and frozen (see
 *   `v1/create-message.compat.spec.ts`) before this field existed, so a
 *   real, already-in-flight v1 message has no `schemaVersion` key at all.
 *   Making it required here would break that frozen fixture, which is by
 *   definition a breaking change to a contract that has already shipped.
 *   Instead:
 *     - `createMessageEventV1Schema` leaves it optional (untouched).
 *     - `createMessageEventV2Schema` (and every version after it)
 *       `.extend()`s this envelope with `schemaVersion: schemaVersionSchema`
 *       made *required*, pinned to its own literal version via
 *       `z.literal(...)`. See `v2/create-message.event.ts`.
 *   This means "does this envelope carry an explicit schemaVersion" is
 *   itself meaningful: absent → assume v1 (the only version that ever
 *   shipped without it); present → trust it over the `type` discriminator
 *   for dispatch. See `events/dispatch-schema-version.ts`.
 */
export const eventEnvelopeSchema = z.object({
  eventId: z.uuid(),
  correlationId: z.uuid(),
  timestamp: z.iso.datetime(),
  source: serviceIdSchema,
  trace: z.array(serviceIdSchema).min(1),
  schemaVersion: schemaVersionSchema.optional(),
});

export type EventEnvelope = z.infer<typeof eventEnvelopeSchema>;
