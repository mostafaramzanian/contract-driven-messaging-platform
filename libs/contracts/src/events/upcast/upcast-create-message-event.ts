import type { CreateMessageEventV1 } from '../v1/create-message.event';
import {
  createMessageEventV2PayloadSchema,
  type CreateMessageEventV2,
} from '../v2/create-message.event';

/**
 * Transforms an already-validated `CreateMessageEvent.v1` into a fully
 * valid `CreateMessageEvent.v2`, with no information lost or invented
 * beyond what v2's own schema defaults already represent honestly (see
 * `createMessageEventV2PayloadSchema`'s doc comment: "this v1 message had
 * no stated priority or metadata" — which `'normal'` and `{}` express,
 * not a guess at what the producer "really meant").
 *
 * ## Why this exists
 *
 * The messaging consumer (`apps/messaging/src/messaging.controller.ts`)
 * accepts both v1 and v2 events on the wire — that is the entire point of
 * versioning the contract instead of breaking v1 producers. But the
 * service's *business logic* (`MessagingService.handleMessageCreation`
 * and downstream) should not have to branch on "is this a v1 or v2 event"
 * at every call site just because the wire contract supports both. This
 * upcaster lets the consumer's dispatcher validate against whichever
 * schema the inbound event actually claims (v1 or v2, via
 * `resolveSchemaVersion` + `EventRegistry`), then immediately normalize a
 * v1 result into the v2 shape before handing it to business logic — so
 * everything past the dispatch boundary speaks exactly one shape, v2,
 * regardless of which version arrived on the wire. v2 was deliberately
 * designed as a strict additive superset of v1 (see
 * `createMessageEventV2PayloadSchema`'s doc comment) specifically to make
 * this upcast lossless and total: there is no v1 event this function
 * cannot represent in v2.
 *
 * ## Why payload defaults are derived, not hardcoded
 *
 * `priority` and `metadata` are obtained by running
 * `createMessageEventV2PayloadSchema.parse()` over the carried-over v1
 * fields, rather than literally writing `priority: 'normal', metadata: {}`
 * here a second time. This means the upcaster's defaults can never drift
 * from the v2 schema's own defaults — if a future change to
 * `createMessageEventV2PayloadSchema` ever changes what "no stated
 * priority" defaults to, this function picks that up automatically
 * instead of silently disagreeing with the schema it's supposed to
 * produce valid output for.
 *
 * ## Determinism
 *
 * Deliberately **does not** call `randomUUID()` or `new Date()`, unlike
 * `buildCreateMessageEventV1` (which *creates* a brand-new event and
 * therefore must generate a fresh `eventId`/`timestamp`). This function
 * *transforms* an event that already has its own `eventId`, `timestamp`,
 * `correlationId`, `source`, and `trace` — upcasting must never change a
 * message's identity or origin, only restate its shape. Calling this
 * function twice on the same input always produces the same output
 * (verified in `upcast-create-message-event.spec.ts`), which matters
 * because the consumer may need to upcast the same already-validated
 * event more than once across a retry (e.g. if upcasting happened after,
 * rather than instead of, the idempotency check) without that producing
 * divergent `eventId`s for what is logically still the same event.
 *
 * ## Input contract
 *
 * `v1` is assumed to already be a `CreateMessageEventV1` that has passed
 * `validateEvent('CreateMessageEvent.v1', raw)` — this function does not
 * re-validate the envelope fields (`eventId`, `correlationId`, etc.) and
 * will propagate them as-is. It only runs the *payload* back through
 * `createMessageEventV2PayloadSchema` to apply v2's field defaults; that
 * payload re-validation cannot fail for any value that was already a
 * valid `CreateMessageEventV1Payload`, because v2's payload schema is a
 * strict superset of v1's (identical `subject`/`content`/`recipient`
 * constraints, plus two new fields that are both optional-with-default).
 */
export function upcastCreateMessageEventV1ToV2(
  v1: CreateMessageEventV1,
): CreateMessageEventV2 {
  const payload = createMessageEventV2PayloadSchema.parse({
    subject: v1.payload.subject,
    content: v1.payload.content,
    recipient: v1.payload.recipient,
  });

  return {
    type: 'CreateMessageEvent.v2',
    schemaVersion: '2',
    eventId: v1.eventId,
    correlationId: v1.correlationId,
    timestamp: v1.timestamp,
    source: v1.source,
    trace: v1.trace,
    payload,
  };
}
