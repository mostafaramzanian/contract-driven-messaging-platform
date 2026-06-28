import { z } from 'zod';
import { eventEnvelopeSchema } from '../envelope.schema';

/**
 * Priority levels a v2 message can be tagged with. This is the first new
 * business field v2 introduces over v1: v1 carried no notion of urgency at
 * all, so every v1 message is implicitly "whatever priority the consumer
 * happens to process it at" (i.e. plain FIFO/queue-order). v2 lets a
 * producer state intent explicitly; what the consumer *does* with that
 * intent (e.g. priority queues, SLA-based alerting) is out of scope for
 * the contract itself — the contract's job is only to make the signal
 * representable and validated, not to implement scheduling.
 *
 * 'normal' is the default (see `createMessageEventV2PayloadSchema` below)
 * so that a v2 producer which does not care about priority does not have
 * to think about this field at all, mirroring how v1 callers never had to
 * think about it either.
 */
export const MESSAGE_PRIORITIES = ['low', 'normal', 'high'] as const;
export type MessagePriority = (typeof MESSAGE_PRIORITIES)[number];

export const messagePrioritySchema = z.enum(MESSAGE_PRIORITIES);

/**
 * Bounded key/value metadata a v2 producer can attach to a message, e.g.
 * `{ "campaignId": "spring-2026", "channel": "email" }`. This is the
 * second new field v2 introduces.
 *
 * Deliberately constrained rather than `z.record(z.string(), z.unknown())`:
 *
 * - Keys and values are both plain strings. Arbitrary nested JSON would
 *   make this payload an unversioned escape hatch in its own right —
 *   exactly the bare-string-keyed-payload problem `libs/contracts` exists
 *   to prevent (see docs/architecture.md's framing of the pre-contracts
 *   `client.emit('createMessage', { ...anything })` pattern this whole
 *   library replaces). If a future need arises for structured metadata,
 *   that is itself a v3 schema change, not a reason to loosen this type.
 * - The map itself is capped at `MAX_METADATA_ENTRIES` entries, and each
 *   key/value is capped at `MAX_METADATA_STRING_LENGTH` characters, so a
 *   producer cannot use "metadata" as an unbounded payload smuggling
 *   channel. These limits are intentionally generous for legitimate
 *   tagging use (campaign IDs, channel names, feature flags) while still
 *   being finite.
 */
export const MAX_METADATA_ENTRIES = 20;
export const MAX_METADATA_STRING_LENGTH = 256;

export const messageMetadataSchema = z
  .record(
    z.string().min(1).max(MAX_METADATA_STRING_LENGTH),
    z.string().max(MAX_METADATA_STRING_LENGTH),
  )
  .refine((entries) => Object.keys(entries).length <= MAX_METADATA_ENTRIES, {
    message: `metadata may not have more than ${MAX_METADATA_ENTRIES} entries`,
  });

export type MessageMetadata = z.infer<typeof messageMetadataSchema>;

/**
 * Payload for the v2 CreateMessageEvent.
 *
 * `subject`, `content`, and `recipient` are carried over from
 * `createMessageEventV1PayloadSchema` with identical constraints — v2 is
 * an additive evolution of v1's payload, not a redesign of it. Nothing a
 * v1 producer already validates against becomes invalid under v2; v2 only
 * adds fields that v1 producers never had to supply.
 *
 * `priority` and `metadata` are both optional with defaults
 * (`'normal'` and `{}` respectively) rather than required, so that the
 * upcaster (see `../upcast/upcast-create-message-event.ts`) can produce a
 * fully valid v2 event from a v1 event that never had these fields, with
 * no information to invent beyond "this v1 message had no stated
 * priority or metadata" — which `'normal'` and `{}` represent honestly,
 * rather than the upcaster having to guess a more specific value.
 */
export const createMessageEventV2PayloadSchema = z.object({
  subject: z.string().min(1).max(255),
  content: z.string().min(1),
  recipient: z.string().max(255).optional(),
  priority: messagePrioritySchema.default('normal'),
  metadata: messageMetadataSchema.default({}),
});

export type CreateMessageEventV2Payload = z.infer<
  typeof createMessageEventV2PayloadSchema
>;

/**
 * The v2 envelope extension. Unlike `createMessageEventV1Schema`, this
 * schema re-declares `schemaVersion` as *required* and pins it to the
 * literal `'2'` via `.extend()` — confirmed (see
 * `dispatch-schema-version.spec.ts` and the inline verification this
 * schema's own tests perform) to correctly override the shared envelope's
 * `schemaVersion: schemaVersionSchema.optional()` rather than merge with
 * it. This is what makes "schemaVersion is present and equals '2'" a
 * structural, validated guarantee for every v2 event, not just a
 * convention — see `envelope.schema.ts`'s `schemaVersion` doc comment for
 * the full rationale on why v1 cannot have the same guarantee
 * retroactively.
 */
export const createMessageEventV2Schema = eventEnvelopeSchema.extend({
  type: z.literal('CreateMessageEvent.v2'),
  schemaVersion: z.literal('2'),
  payload: createMessageEventV2PayloadSchema,
});

export type CreateMessageEventV2 = z.infer<typeof createMessageEventV2Schema>;
