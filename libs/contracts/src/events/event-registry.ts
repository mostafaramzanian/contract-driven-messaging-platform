import { createMessageEventV1Schema } from './v1/create-message.event';
import { createMessageEventV2Schema } from './v2/create-message.event';

/**
 * Central registry of every event type this system produces or consumes,
 * keyed by its versioned type string. This replaces bare string literals
 * like `client.emit('createMessage', ...)` with a single source of truth:
 * adding a new event version means adding one entry here, not hunting
 * down every `emit`/`@MessagePattern` call site.
 *
 * Versioning strategy: each breaking change to a payload shape gets a new
 * key (`CreateMessageEvent.v2`, `CreateMessageEvent.v3`, etc.) rather than
 * mutating an existing version's schema in place. Old producers/consumers
 * keep working against their existing entry until they are migrated; this
 * is the "v1, v2 strategy" referenced in docs/architecture.md.
 *
 * As of `CreateMessageEvent.v2`, this is no longer hypothetical: both
 * `'CreateMessageEvent.v1'` and `'CreateMessageEvent.v2'` are registered
 * and validated independently below, and both are actively dispatched to
 * by the messaging consumer (see
 * `apps/messaging/src/messaging.controller.ts`'s version-aware
 * dispatcher, which uses `resolveSchemaVersion` from
 * `./dispatch-schema-version` to decide which of the two entries below to
 * validate an inbound message against). A v1 producer that has not yet
 * migrated continues to validate against the v1 entry exactly as it
 * always has — adding the v2 entry below changes nothing about how v1
 * messages are validated, dispatched, or processed.
 *
 * Each version's schema also fixes `schemaVersion` to its own literal
 * (`z.literal('2')` for v2, and so on) wherever the version was *defined*
 * with that field present — see `envelope.schema.ts`'s `schemaVersion`
 * doc comment for why v1 specifically cannot do the same retroactively.
 */
export const EventRegistry = {
  'CreateMessageEvent.v1': createMessageEventV1Schema,
  'CreateMessageEvent.v2': createMessageEventV2Schema,
} as const;

export type EventType = keyof typeof EventRegistry;

/**
 * The literal v1 event-type string, as a stable named constant rather
 * than a string repeated at every call site. **Immutable**: this constant
 * has shipped, is asserted directly in
 * `v1/create-message.compat.spec.ts` and `index.spec.ts`, and is used as
 * the literal RabbitMQ message pattern
 * (`@MessagePattern(CreateMessageEvent.name)`) in
 * `apps/messaging/src/messaging.controller.ts`. Changing what this
 * resolves to would silently break every already-deployed v1
 * producer/consumer and every message already in flight or sitting in a
 * retry/DLQ cycle.
 */
export const CreateMessageEvent = {
  name: 'CreateMessageEvent.v1' as const,
};

/**
 * The literal v2 event-type string, following the exact same constant
 * pattern as `CreateMessageEvent` above. Producers should prefer
 * `CreateMessageEventNameV2.name` over the bare string literal for the
 * same reason `CreateMessageEvent.name` exists: one renamed/typo'd string
 * literal at a single call site is a silent runtime bug, while a renamed
 * constant is a compile error everywhere it's used.
 *
 * Named `CreateMessageEventNameV2`, not `CreateMessageEventV2` — that
 * shorter name is already taken by the *type*
 * `CreateMessageEventV2 = z.infer<typeof createMessageEventV2Schema>`
 * exported from `./v2/create-message.event`, which this module
 * re-exports via the barrel (`index.ts`). The two are different things
 * (a runtime name-holder object vs. a compile-time inferred type) and
 * both need to be importable without one shadowing the other.
 */
export const CreateMessageEventNameV2 = {
  name: 'CreateMessageEvent.v2' as const,
};
