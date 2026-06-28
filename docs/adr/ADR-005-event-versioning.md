# ADR-005: Immutable Event Versioning with Upcasting

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2024-01-15 |
| **Author** | Platform Engineering |
| **Reviewers** | Backend Engineering |
| **Supersedes** | — |
| **Superseded by** | — |

---

## Context

The `CreateMessageEvent` schema in `libs/contracts` was initially designed with a flat `subject` field. A product requirement introduced a `metadata` field (structured object with `tags` and `priority`) that is required for all new events but was not present in events created under the original schema.

Changing the existing `CreateMessageEvent` schema in place — adding `metadata` as a required field — would immediately break all producers that have not yet been updated to include `metadata`. Because producers and consumers are deployed independently, there is always a window where one service is ahead of the other. A breaking in-place schema change with no migration strategy collapses that window to zero, requiring a synchronized deployment of all services simultaneously.

In a two-service system this is manageable. In a system with more producers or consumers, synchronized deployments are operationally expensive and are a common source of production incidents.

---

## Problem Statement

**How do we evolve event schemas over time without requiring synchronized deployments of all producers and consumers?**

The constraints are:
- A v1 producer must be able to emit v1 events that a v2 consumer can process correctly
- A v2 producer must be able to emit v2 events that are processed correctly
- A consumer that has not yet been updated to handle v2 must continue to process v1 events without breaking
- The consumer's business logic must not branch on version — it should operate on a single canonical shape

---

## Decision

**Breaking schema changes produce a new, immutable schema version. The consumer handles both versions simultaneously during the migration window using a version dispatch mechanism and a deterministic upcaster.**

### Schema versioning

New versions are defined as separate Zod schemas in `libs/contracts`:

```typescript
// libs/contracts/events/create-message-event.v1.ts
export const CreateMessageEventV1Schema = z.object({ ... });

// libs/contracts/events/create-message-event.v2.ts
export const CreateMessageEventV2Schema = CreateMessageEventV1Schema.extend({
  payload: CreateMessageEventV1Schema.shape.payload.extend({
    metadata: z.object({ tags: z.array(z.string()), priority: z.enum(['low','normal','high']) })
  })
});
```

Both are registered in `EventRegistry`. The v1 schema is never mutated.

### Version dispatch

The consumer determines which schema to validate against using a three-step resolution chain:

1. `envelope.schemaVersion` field — highest precedence
2. AMQP message header `x-schema-version` — fallback if envelope field is absent
3. Routing key suffix (`.v1`, `.v2`) — fallback if header is absent
4. Default to `v1` — for pre-versioning messages in the DLQ or retry queue

### Upcasting

Before business logic runs, v1 events are normalized to v2 shape by `upcastCreateMessageEventV1ToV2`:

```typescript
function upcastCreateMessageEventV1ToV2(v1: CreateMessageEventV1): CreateMessageEventV2 {
  return {
    ...v1,
    payload: {
      ...v1.payload,
      metadata: { tags: [], priority: 'normal' }   // deterministic defaults
    },
    schemaVersion: 'v2'
  };
}
```

**The upcaster must be deterministic**: no `randomUUID()`, no `new Date()`, no external calls. The same v1 input must always produce the same v2 output. This is required for idempotency correctness — if the upcaster is non-deterministic and a v1 event is delivered twice (once on first delivery, once on retry), the two upcasted shapes must be identical for the idempotency check to work correctly.

Business logic operates exclusively on v2 shape. The version dispatch and upcasting happen in the consumer infrastructure layer, invisible to the domain code.

### Backward compatibility fixtures

A fixture test in `libs/contracts` asserts that the current upcaster can correctly process a stored v1 payload:

```typescript
it('can upcast stored v1 fixture to v2', () => {
  const fixture = JSON.parse(readFileSync('fixtures/create-message-event.v1.json', 'utf-8'));
  const upcasted = upcastCreateMessageEventV1ToV2(fixture);
  expect(CreateMessageEventV2Schema.safeParse(upcasted).success).toBe(true);
});
```

This test will fail if a future change to the upcaster or the v2 schema breaks the ability to process a stored v1 message — catching a regression that would otherwise be discovered in production.

---

## Alternatives Considered

### In-place schema mutation with optional fields

**Why considered:** The simplest approach. Add `metadata` as an optional field to the existing `CreateMessageEvent` schema. Existing producers don't need to change. The consumer handles absent `metadata` with a default.

**Why not chosen:** Optional fields accumulate. After several feature additions, the schema has many optional fields, and the consumer's business logic must handle the combinatorial space of present/absent combinations. More critically, this approach cannot handle structural changes (field renames, type changes, removed fields) — only additive changes. When a genuinely breaking change is eventually required, there is no migration infrastructure in place. Treating the first breaking change as an opportunity to build versioning infrastructure is less disruptive than retrofitting it later.

### Topic/routing-key-per-version (separate queues per version)

**Why considered:** Route `CreateMessageEvent.v1` events to a `messaging.work.v1` queue and `CreateMessageEvent.v2` events to a `messaging.work.v2` queue. Deploy separate consumer instances per version.

**Why not chosen:** This multiplies the number of queues, consumers, and deployment targets by the number of active versions. For a system with 10 event types and 3 active versions each, this is 30 queues and 30 consumer configurations. The maintenance burden is proportional to `event_types × active_versions`. The upcaster approach keeps the consumer count fixed at 1 per event type regardless of how many versions are active.

### Schema registry with compatibility enforcement

**Why considered:** A schema registry (Confluent Schema Registry, AWS Glue) enforces compatibility rules at publish time. A producer that attempts to publish a schema that is not backward-compatible with the registered schema is rejected before the message enters the broker.

**Why not chosen:** Schema registries are the correct solution at scale (many producers, many consumers, many event types). For a two-service system, the operational overhead of a schema registry (additional service, HTTP calls on every publish, schema evolution workflow) exceeds the benefit. The `libs/contracts` shared library provides equivalent enforcement at compile time, with the fixture test providing regression detection for backward compatibility. A schema registry should be evaluated when the number of event types or services grows beyond what the shared library can cleanly support.

### Event sourcing with aggregate replay

**Why considered:** Store all events immutably and reconstruct state by replaying from the beginning. Schema evolution is handled at read time by an event migrator that applies the current version's schema to all historical events.

**Why not chosen:** Event sourcing changes the fundamental data model of the system. The current system is a message delivery pipeline, not an event-sourced aggregate. Adopting event sourcing to solve the schema evolution problem is using a sledgehammer for a nail.

---

## Tradeoffs

| Gains | Costs |
|---|---|
| Producers and consumers can be deployed independently | Old version handlers accumulate — must be actively deprecated and removed |
| Business logic operates on exactly one canonical shape (v2) | Each new version requires a new schema file, a new registry entry, and a new upcaster stage |
| Upcaster is independently testable with fixture-based tests | The version resolution chain adds latency and code complexity to every consume cycle |
| Backward compatibility regressions are caught by fixture tests before deployment | If `schemaVersion` is absent from the envelope and the headers, the consumer falls back to v1 — a silent default that may be wrong for future versions |
| Migration window is controlled — v1 support can be removed when all producers are confirmed on v2 | The upcaster chain grows linearly with the number of version transitions (v1→v2, v2→v3, ...) |

---

## Consequences

1. **Version deprecation is an explicit operational step.** When all producers are confirmed to be emitting v2, the v1 handler and upcaster can be removed. This requires: (a) confirming that no v1 messages remain in the work queue, retry queue, DLQ, or outbox tables, and (b) removing the v1 schema from `EventRegistry` and the v1 handler from the consumer. Removing too early causes the consumer to fail on any remaining v1 messages with a "no handler for version" error.

2. **Upcaster determinism is a correctness requirement, not a style preference.** Any non-deterministic value in the upcaster (generated IDs, current timestamps, random values) will cause idempotency failures under retry: the first and second deliveries of the same v1 event will produce different v2 shapes, and the idempotency check (which compares by `eventId`, not by payload) will correctly identify them as the same event — but the business write on the first delivery and the rejected duplicate on the second delivery will have been computed from different inputs. For events where the upcasted payload is stored (not just used for processing), this can cause data inconsistency.

3. **The fixture test in `libs/contracts` must be updated when the upcaster is updated.** The fixture file (`fixtures/create-message-event.v1.json`) is a pinned sample of a real v1 wire-format payload. If the upcaster is changed in a way that can no longer correctly process the fixture, the test will fail before deployment.

4. **The version resolution chain has a specific precedence order that must be documented.** The order is: envelope field > AMQP header > routing key suffix > default v1. Consumers that are added to the system must use the same resolution chain, or they will dispatch on a different version than the infrastructure expects.

---

## Operational Impact

### Alert rules

```yaml
- alert: UnknownSchemaVersion
  expr: increase(schema_dispatch_unknown_version_total[5m]) > 0
  for: 0m
  severity: warning
  annotations:
    summary: "Consumer received an event with an unrecognized schema version — possible new version not yet deployed"
```

### Runbook — unknown schema version received

1. Inspect the `x-schema-version` header and `envelope.schemaVersion` field on the failing message via DLQ inspection
2. Check whether a new producer version has been deployed before the consumer was updated
3. If the version is a legitimate new version: deploy the updated consumer with the new schema and upcaster registered
4. If the version is invalid (typo, test payload): ack the message via the DLQ admin endpoint; do not requeue

### Migration checklist — deprecating v1

- [ ] Confirm no v1 producers are deployed (check deployment registry)
- [ ] Confirm `messaging.work` queue contains no messages with `schemaVersion: 'v1'`
- [ ] Confirm `messaging.retry.q` contains no v1 messages
- [ ] Confirm `messaging.dlq` contains no v1 messages requiring requeue
- [ ] Confirm `gateway_outbox_events` contains no pending v1 rows
- [ ] Remove `CreateMessageEventV1Schema` from `EventRegistry`
- [ ] Remove `upcastCreateMessageEventV1ToV2` from consumer infrastructure
- [ ] Remove v1 fixture test
- [ ] Deploy consumer
- [ ] Monitor `schema_dispatch_unknown_version_total` for 24h post-deployment

---

## Future Considerations

- **Upcaster chain management**: As versions accumulate (v1→v2→v3→v4), the upcaster chain grows. A message arriving as v1 must be upcasted through v1→v2→v3→v4. This is correct but requires that every upcaster in the chain is maintained and tested. Consider a "fold" approach where old versions are periodically consolidated: after v4 is stable and v1/v2 are deprecated, the v1 fixture can be updated to a v3 fixture and the v1→v2→v3 chain replaced with a single v1→v4 upcaster.
- **Schema registry integration**: When the number of event types or services grows, consider migrating schema definitions from `libs/contracts` to a dedicated schema registry with compatibility enforcement. The upcasting architecture remains unchanged; only the source of schema definitions changes.
