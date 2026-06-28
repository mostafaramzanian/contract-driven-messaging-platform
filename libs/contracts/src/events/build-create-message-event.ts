import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import {
  type CreateMessageEventV1,
  type CreateMessageEventV1Payload,
} from './v1/create-message.event';
import {
  createMessageEventV2PayloadSchema,
  type CreateMessageEventV2,
} from './v2/create-message.event';
import { CreateMessageEvent, CreateMessageEventNameV2 } from './event-registry';

/**
 * Builds a complete, contract-shaped CreateMessageEvent.v1 from a payload
 * and an inbound correlationId. Used by the producer (gateway) so eventId
 * generation, timestamping, and trace initialization happen in exactly one
 * place rather than being duplicated at every emit call site.
 */
export function buildCreateMessageEventV1(
  payload: CreateMessageEventV1Payload,
  correlationId: string,
): CreateMessageEventV1 {
  return {
    type: CreateMessageEvent.name,
    eventId: randomUUID(),
    correlationId,
    timestamp: new Date().toISOString(),
    source: 'gateway',
    trace: ['gateway'],
    payload,
  };
}

/**
 * Builds a complete, contract-shaped CreateMessageEvent.v2 from a payload
 * and an inbound correlationId. Mirrors `buildCreateMessageEventV1`
 * exactly for the envelope fields it owns (`eventId` via `randomUUID()`,
 * `timestamp` via `new Date().toISOString()`, `source: 'gateway'`,
 * `trace: ['gateway']`) plus the two fields v2 additionally requires on
 * its envelope: `type: 'CreateMessageEvent.v2'` and `schemaVersion: '2'`.
 *
 * ## Why the payload parameter is typed with `z.input<...>`, not
 * `CreateMessageEventV2Payload`
 *
 * `CreateMessageEventV2Payload` (exported from `./v2/create-message.event`)
 * is `z.infer<typeof createMessageEventV2PayloadSchema>` — the *output*
 * type, in which `priority` and `metadata` are both present and required,
 * because Zod's `.default()` always fills them in by the time parsing
 * completes. If this function's parameter were typed that way, every
 * caller would be forced to spell out `priority: 'normal', metadata: {}`
 * even when they don't care about either field — exactly the ergonomic
 * problem `.default()` exists to solve, just pushed onto every call site
 * instead.
 *
 * Using `z.input<typeof createMessageEventV2PayloadSchema>` instead (the
 * *input* type Zod infers, in which defaulted fields are optional) lets a
 * caller write `buildCreateMessageEventV2({ subject, content }, correlationId)`
 * exactly as they would for v1, and get `priority: 'normal'`,
 * `metadata: {}` for free — by running the payload through
 * `createMessageEventV2PayloadSchema.parse()` below, the same
 * derive-don't-duplicate approach `upcastCreateMessageEventV1ToV2` uses
 * (see `upcast/upcast-create-message-event.ts`) for the identical reason:
 * a single source of truth for what "no stated priority/metadata" means,
 * so this builder and the schema can never silently disagree about it.
 *
 * A caller that *does* care can still pass `priority`/`metadata`
 * explicitly; `.parse()` validates and preserves whatever was supplied,
 * exactly as it does for `subject`/`content`/`recipient`.
 */
export function buildCreateMessageEventV2(
  payload: z.input<typeof createMessageEventV2PayloadSchema>,
  correlationId: string,
): CreateMessageEventV2 {
  return {
    type: CreateMessageEventNameV2.name,
    schemaVersion: '2',
    eventId: randomUUID(),
    correlationId,
    timestamp: new Date().toISOString(),
    source: 'gateway',
    trace: ['gateway'],
    payload: createMessageEventV2PayloadSchema.parse(payload),
  };
}
