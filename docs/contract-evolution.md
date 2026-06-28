# Contract Evolution & Deprecation Strategy

This document describes how this system evolves an event contract without
breaking existing producers or consumers, using `CreateMessageEvent`'s v1 →
v2 transition as the concrete (and, as of this writing, only) example. See
[`docs/architecture.md`](./architecture.md) for the contract system's
internal structure (`EventRegistry`, `validateEvent`, the envelope schema);
this document is about the *process* around a version's lifecycle, not the
schema mechanics themselves.

> **A note on scope.** Parts of `docs/architecture.md` describe the system
> as it existed before `CreateMessageEvent.v2`, the schema-version header,
> and the reliability hardening (manual ack/nack, retry, DLQ) that has
> since landed — for example it still states the RMQ transport runs with
> `noAck: true`, which is no longer accurate (`apps/messaging/src/main.ts`
> and `dlq-consumer.service.ts` both now set `noAck: false`). Reconciling
> that document fully is a separate piece of work from this one; this
> document is deliberately new and self-contained rather than building on
> sections of `architecture.md` that may not reflect current behavior.

## The model: versions are additive, never mutated

Every breaking change to an event's payload shape gets a **new registry
entry** (`'CreateMessageEvent.v2'`, eventually `.v3`, …) rather than an edit
to an existing version's schema. `createMessageEventV1Schema` is frozen —
literally enforced by a golden-fixture test,
`libs/contracts/src/events/v1/create-message.compat.spec.ts`, which
re-validates a hand-written, never-edited example event against the
*current* schema on every test run. If a future change to that schema ever
makes the frozen fixture fail, that is by definition a breaking change to a
contract that has already shipped, and the correct fix is a new version,
not an edit to v1.

This means a version, once registered, is a permanent commitment: any
producer that has ever spoken v1 can keep speaking v1 indefinitely, and the
messaging consumer will keep accepting it, for as long as v1 remains in
`EventRegistry`. Deprecating a version is therefore a deliberate, separate
decision from introducing the version that replaces it — they are not the
same event.

## What "deprecating v1" does and does not mean here

**Does mean:**

- Declaring an intended sunset window during which v1 producers are
  expected to migrate to v2.
- Making it possible to observe, with existing tooling, how much real
  traffic is still v1 versus v2 at any point during that window.
- Eventually — once migration is confirmed complete — removing the v1
  schema, its registry entry, and the `CreateMessageEvent.v1` pattern from
  `MessagingController.handleMessage`'s `@MessagePattern` array, in its own
  dedicated change.

**Does not mean, at any point before that final removal:**

- Rejecting, warning on, rate-limiting, or otherwise degrading v1 traffic
  at the producer or consumer. A v1 event that passes
  `createMessageEventV1Schema` validation is processed exactly as
  successfully as a v2 event that passes `createMessageEventV2Schema`
  validation — same idempotency guarantee, same retry/DLQ behavior, same
  business outcome (see `upcastCreateMessageEventV1ToV2`'s role in
  `MessagingController.handleMessage`, which normalizes a validated v1
  event to v2 shape before business processing, but never rejects or
  alters a v1 event for being v1).
- Any synchronous friction for the caller — no deprecation HTTP header, no
  slowed response, no forced opt-in prompt. The two gateway routes,
  `GET /api/test-rabbit` (v1) and `GET /api/test-rabbit-v2` (v2), both stay
  fully functional and equally fast for the entire deprecation window.

Deprecation, in this codebase's model, is a **signaling and tracking**
concern, not a gating one. The mechanism for "stop supporting v1" is
removing it from the registry on a known date *after* usage has actually
dropped to zero — not making v1 gradually worse beforehand to coerce that
drop.

## Tracking v1 usage: no new instrumentation needed

`MetricsService` (`libs/common/src/metrics/metrics.service.ts`) already
exposes `messages_processed_total` and `messages_failed_total` as
Prometheus counters labelled `{service, event_type, outcome}` /
`{service, event_type, error_class}`. Since the consumer-side dispatcher
change in `MessagingController.handleMessage`, `event_type` is always the
**wire-resolved** version string — `'CreateMessageEvent.v1'` or
`'CreateMessageEvent.v2'` — determined by `resolveSchemaVersion` before any
upcasting happens, specifically so that normalizing a v1 event to v2 shape
internally (see above) never erases which version actually arrived on the
wire. The gateway's two routes set the same label on the producer side via
`event_type: CreateMessageEvent.name` /
`event_type: CreateMessageEventNameV2.name` in their respective
`processingDurationSeconds.startTimer(...)` calls.

This means the question "what fraction of traffic is still v1?" is already
answerable today, with the Prometheus instance defined in
`observability/prometheus/prometheus.yml`, with no new metrics, no new
labels, and no code change:

```promql
sum(rate(messages_processed_total{event_type="CreateMessageEvent.v1"}[1h]))
/
sum(rate(messages_processed_total{event_type=~"CreateMessageEvent\\.v.*"}[1h]))
```

A value sustained near `0` over a representative window is the signal that
v1 is safe to remove. A Grafana panel built on this query (or the
equivalent per-service breakdown, by also grouping on the existing
`service` label) is the recommended way to watch this over the deprecation
window — no separate dashboard infrastructure needs to be built, since
Grafana is already provisioned (`observability/grafana/provisioning/`)
against this same Prometheus instance.

### Optional: an informational alert

`observability/prometheus/alert_rules.yml` already defines threshold-based
alerts in this exact style (`HighMessageFailureRate`, `DlqMessagesGrowing`,
`ExcessiveRetries`). A v1-usage alert can follow the same pattern once a
concrete sunset date is set, for example:

```yaml
- alert: LegacyV1ContractStillActive
  expr: sum(rate(messages_processed_total{event_type="CreateMessageEvent.v1"}[1h])) > 0
  for: 1h
  labels:
    severity: info
  annotations:
    summary: 'CreateMessageEvent.v1 traffic still present'
    description: >
      v1 traffic was observed in the last hour. v1 is scheduled for
      removal on <date> — confirm whether known producers have migrated.
```

This is deliberately left as a documented pattern rather than added to
`alert_rules.yml` directly in this change: `severity: info` alerts that
fire continuously until a human-chosen sunset date are a judgment call
about team alerting conventions (some teams want this as a standing
reminder; others find a continuously-firing informational alert noisy) that
belongs to whoever owns the on-call rotation, not to the contract-evolution
work itself.

## Migration path for a v1 producer

There is currently exactly one v1 producer in this codebase:
`AppController.sendTestMessage` (`GET /api/test-rabbit`). Migrating it
means switching the call site from `buildCreateMessageEventV1` /
`CreateMessageEvent.name` to `buildCreateMessageEventV2` /
`CreateMessageEventNameV2.name`, following the same shape already
implemented for `AppController.sendTestMessageV2`
(`GET /api/test-rabbit-v2`) — no consumer-side change is required to
support an existing v1 producer migrating to v2, since
`MessagingController.handleMessage` already accepts both versions
simultaneously via its multi-pattern `@MessagePattern`.

For a hypothetical *external* v1 producer outside this codebase, the same
principle applies: it can adopt v2 (new optional `priority` and `metadata`
fields, same `subject`/`content`/`recipient` fields it already sends)
whenever convenient within the deprecation window, with no coordinated
flag-day cutover required, because both versions are validated and
processed by the consumer independently and simultaneously throughout that
window.

## Removal checklist (for whenever v1 usage has reached zero)

This is **not** part of the current change — it is recorded here so the
eventual removal is a small, well-defined, low-risk diff rather than a
rediscovery effort:

1. Confirm `messages_processed_total{event_type="CreateMessageEvent.v1"}`
   has been at zero for a full representative traffic window (see PromQL
   above).
2. Remove `'CreateMessageEvent.v1'` from the `@MessagePattern([...])` array
   in `MessagingController.handleMessage`.
3. Remove the `schemaVersion === '2' ? ... : upcastCreateMessageEventV1ToV2(...)`
   branch in the same method — once v1 can no longer arrive, `event` is
   always already v2-shaped from `validationResult.event` directly.
4. Remove `'CreateMessageEvent.v1': createMessageEventV1Schema` from
   `EventRegistry`, and the now-unreferenced `CreateMessageEvent` constant,
   `createMessageEventV1Schema`, `buildCreateMessageEventV1`, and
   `upcastCreateMessageEventV1ToV2` exports — at which point `tsc` will
   surface every remaining call site that still needs updating, including
   `AppController.sendTestMessage` (which would need to be removed or
   migrated to the v2 builder) and every `*.spec.ts` file that constructs a
   v1 fixture.
5. Move `v1/create-message.event.ts` and its golden-fixture spec out of the
   active source tree (e.g. to a `docs/`-referenced archive or simply
   delete them with this document updated to record the removal date) —
   the frozen-fixture test's entire purpose was guarding against breaking
   *active* v1 producers, which no longer exist at this point.
6. Update this document's "currently exactly one v1 producer" statement
   above (or remove the document if a v3/v4 deprecation cycle has its own
   updated version by then).

Steps 2–4 are intentionally ordered so that `tsc`'s own type errors guide
the removal — deleting the registry entry before deleting its consumers
turns "did I forget a call site" from a manual audit into a compiler error.
