import { SCHEMA_VERSIONS, type SchemaVersion } from './envelope.schema';

/**
 * AMQP message-header key used to mirror an event's `schemaVersion` at the
 * transport level, alongside the existing `x-retry-count` /
 * `x-correlation-id` / `x-error-class` headers already set by
 * `MessagingController` (see apps/messaging/src/messaging.controller.ts).
 *
 * This is a *mirror*, not a second source of truth: `resolveSchemaVersion`
 * below always prefers the envelope's own `schemaVersion` field over this
 * header when both are present and they disagree (see precedence rules on
 * `resolveSchemaVersion`). The header exists so that infrastructure-level
 * tooling (broker-side routing rules, log shippers, dashboards) can filter
 * or branch on schema version without deserializing and validating the
 * full message body first.
 */
export const SCHEMA_VERSION_HEADER = 'x-schema-version' as const;

/**
 * The schema version every event is treated as if no explicit
 * `schemaVersion` can be determined from any source. This is '1' and only
 * '1', for one specific historical reason: `CreateMessageEvent.v1` shipped
 * and was frozen (see `v1/create-message.compat.spec.ts`) before this
 * field existed, so real v1 messages — already produced, possibly still
 * sitting in a queue or a retry/DLQ cycle — have no `schemaVersion`
 * anywhere on them. Defaulting "unknown" to v1 is what makes those
 * messages keep working through a dispatcher that is otherwise
 * version-aware. This default must never be changed to anything other
 * than '1' without a migration plan for already-published v1 messages.
 */
export const DEFAULT_SCHEMA_VERSION: SchemaVersion = '1';

/**
 * Inputs `resolveSchemaVersion` can draw on, all optional, all `unknown`
 * or loosely-typed because this function runs *before* full contract
 * validation — its entire job is to decide which schema
 * `validateEvent`/`EventRegistry` should validate against next. It must
 * not throw and must not assume any input is well-formed.
 */
export interface SchemaVersionSources {
  /**
   * The raw, not-yet-validated message body (e.g. the AMQP payload after
   * JSON parsing, or the object built by a producer before
   * `validateEvent` is called). If this has a `schemaVersion` property
   * whose value is one of `SCHEMA_VERSIONS`, it is the most trusted
   * source — it's the field the receiving contract schema will itself
   * require for any version 2 and above.
   */
  envelope?: unknown;
  /**
   * Raw AMQP header value, e.g.
   * `msg.properties.headers?.[SCHEMA_VERSION_HEADER]`. Headers arrive as
   * `unknown` from amqplib's typing (`MessagePropertyHeaders`), and may be
   * a string, a Buffer (some AMQP clients/brokers wrap header values),
   * a number, or absent entirely.
   */
  header?: unknown;
  /**
   * The event's `type` discriminator string, e.g.
   * `'CreateMessageEvent.v1'` or `'CreateMessageEvent.v2'`, if already
   * known to the caller. Used only as a last-resort fallback — see
   * precedence rules below.
   */
  type?: unknown;
}

/**
 * Narrows an arbitrary value to a known `SchemaVersion`, or returns
 * `undefined`. Accepts plain strings and, defensively, Buffer-wrapped
 * header values (some AMQP header codecs deliver byte-array headers
 * rather than strings) by coercing to a UTF-8 string first.
 */
function coerceToSchemaVersion(value: unknown): SchemaVersion | undefined {
  let candidate: unknown = value;

  if (
    candidate &&
    typeof candidate === 'object' &&
    Buffer.isBuffer(candidate)
  ) {
    candidate = candidate.toString('utf8');
  }

  if (typeof candidate === 'number') {
    candidate = String(candidate);
  }

  if (typeof candidate !== 'string') {
    return undefined;
  }

  const trimmed = candidate.trim();
  return (SCHEMA_VERSIONS as readonly string[]).includes(trimmed)
    ? (trimmed as SchemaVersion)
    : undefined;
}

/**
 * Extracts a `SchemaVersion` from a `type` discriminator string by
 * reading its `.vN` suffix, e.g. `'CreateMessageEvent.v2'` -> `'2'`.
 * Returns `undefined` if `value` is not a string, has no recognizable
 * `.vN` suffix, or the extracted `N` is not a known schema version.
 */
function extractFromTypeSuffix(value: unknown): SchemaVersion | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }

  const match = /\.v(\d+)$/.exec(value);
  if (!match) {
    return undefined;
  }

  return coerceToSchemaVersion(match[1]);
}

/**
 * Reads a `schemaVersion`-shaped property off an unknown value without
 * assuming it is an object at all (raw AMQP payloads are `unknown` until
 * validated).
 */
function readEnvelopeSchemaVersion(value: unknown): SchemaVersion | undefined {
  if (typeof value !== 'object' || value === null) {
    return undefined;
  }
  if (!('schemaVersion' in value)) {
    return undefined;
  }
  return coerceToSchemaVersion(
    (value as { schemaVersion?: unknown }).schemaVersion,
  );
}

/**
 * Reads a `type`-shaped property off an unknown value, for use as a
 * fallback when the caller did not separately pass `sources.type`.
 */
function readEnvelopeType(value: unknown): unknown {
  if (typeof value !== 'object' || value === null) {
    return undefined;
  }
  if (!('type' in value)) {
    return undefined;
  }
  return (value as { type?: unknown }).type;
}

/**
 * Resolves the effective `SchemaVersion` for an inbound or outbound event
 * from whatever sources are available, in strict precedence order:
 *
 *   1. `sources.envelope.schemaVersion`     (the contract's own field)
 *   2. `sources.header`                     (the AMQP transport mirror)
 *   3. `sources.type` / `sources.envelope.type`'s `.vN` suffix
 *   4. `DEFAULT_SCHEMA_VERSION` ('1')
 *
 * Rationale for this order: the envelope field is what the receiving
 * schema will itself validate (for v2+), so it is the most authoritative
 * *contract-level* signal — trusting it first means a message that
 * disagrees with its own header (e.g. a relay or proxy mutated/derived
 * the header incorrectly) still gets routed to the schema it actually
 * claims to satisfy, which `validateEvent` will then confirm or reject.
 * The header is consulted next specifically so that infrastructure can
 * route v2 traffic *before* JSON-parsing the body, for cases where the
 * caller has a header but hasn't parsed the body yet (e.g. a future
 * broker-side routing rule, or a metrics tap). `type`'s suffix is a
 * last-resort fallback for any caller that has neither field handy but
 * does have the discriminator string. Absent all three, every event is
 * v1 — see `DEFAULT_SCHEMA_VERSION` for why that default is permanent.
 *
 * This function never throws and never returns anything outside
 * `SCHEMA_VERSIONS`.
 */
export function resolveSchemaVersion(
  sources: SchemaVersionSources,
): SchemaVersion {
  const fromEnvelopeField = readEnvelopeSchemaVersion(sources.envelope);
  if (fromEnvelopeField) {
    return fromEnvelopeField;
  }

  const fromHeader = coerceToSchemaVersion(sources.header);
  if (fromHeader) {
    return fromHeader;
  }

  const typeSource =
    sources.type !== undefined
      ? sources.type
      : readEnvelopeType(sources.envelope);
  const fromType = extractFromTypeSuffix(typeSource);
  if (fromType) {
    return fromType;
  }

  return DEFAULT_SCHEMA_VERSION;
}
