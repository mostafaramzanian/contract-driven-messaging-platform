import { createMessageEventV1Schema } from './v1/create-message.event';

/**
 * Central registry of every event type this system produces or consumes,
 * keyed by its versioned type string. This replaces bare string literals
 * like `client.emit('createMessage', ...)` with a single source of truth:
 * adding a new event version means adding one entry here, not hunting
 * down every `emit`/`@MessagePattern` call site.
 *
 * Versioning strategy: each breaking change to a payload shape gets a new
 * key (`CreateMessageEvent.v2`, etc.) rather than mutating the v1 schema.
 * Old producers/consumers keep working against the v1 entry until they are
 * migrated; this is the "v1, v2 strategy" referenced in
 * docs/architecture.md, kept intentionally simple (no automatic migration
 * or dual-write logic, since nothing in this codebase needs that yet).
 */
export const EventRegistry = {
  'CreateMessageEvent.v1': createMessageEventV1Schema,
} as const;

export type EventType = keyof typeof EventRegistry;

export const CreateMessageEvent = {
  name: 'CreateMessageEvent.v1' as const,
};
