# ADR-002: Contract-Driven Event Design

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2024-01-10 |
| **Author** | Platform Engineering |
| **Reviewers** | Backend Engineering |
| **Supersedes** | — |
| **Superseded by** | — |

---

## Context

The gateway service and the messaging service communicate via AMQP messages. An AMQP message body is an untyped byte array. Without an enforced schema, either service can change its payload structure without the other's knowledge. This produces a class of failure that is invisible at build time and silent at runtime: the producer emits a payload that the consumer deserializes into a structurally valid but semantically wrong object, and the failure manifests as incorrect business logic execution rather than an explicit error.

In the initial prototype, event shapes were defined as local TypeScript interfaces in each service independently. A field rename in the producer (`subject` → `title`) was not reflected in the consumer, resulting in consumer processing with `event.payload.subject === undefined` — a value that passed TypeScript's type checks because the consumer's local interface still declared `subject` as optional.

---

## Problem Statement

TypeScript interfaces provide compile-time type checking within a single compilation unit. They do not enforce runtime validation, they do not create a shared canonical definition across service boundaries, and they do not detect wire-format drift between independently deployed services. A distributed system that relies on TypeScript types alone for contract enforcement has no protection against schema drift between separately compiled and deployed binaries.

---

## Decision

**Define all inter-service events as versioned Zod schemas in `libs/contracts`, a shared library that both services import as a compile-time and runtime dependency.**

The `EventRegistry` in `libs/contracts` maps event type identifiers to their Zod schemas. Both the gateway and the messaging service import from `libs/contracts`. Neither service defines its own local event shape. `validateEvent()` is called at both the emit boundary (gateway, before outbox INSERT) and the consume boundary (messaging service, before business logic).

Schema validation at both boundaries provides defense in depth: a producer bug that generates an invalid payload is caught before the event enters the broker; a relay or broker that corrupts the payload is caught before it reaches business logic. In both cases the failure is explicit, logged with a structured error record, and does not cause silent incorrect processing.

---

## Alternatives Considered

### Local TypeScript interfaces per service

**Why considered:** Lowest friction. Each service owns its types. No shared library. No coordination for changes.

**Why not chosen:** This is the architecture that produced the prototype failure described above. TypeScript interfaces are erased at runtime; they provide no protection against wire-format drift between independently compiled binaries. A field that is `string` in the producer's interface and `string | undefined` in the consumer's interface will cause the consumer to silently process `undefined` without a compilation error in either service.

### JSON Schema with AJV

**Why considered:** JSON Schema is a language-agnostic standard. AJV is a fast, well-tested validator. JSON Schema definitions can be used across non-TypeScript consumers (Python workers, Go services) without recompilation.

**Why not chosen:** JSON Schema and TypeScript types are separate artifacts that must be kept in sync manually. The schema file defines the validation rules; the TypeScript interface defines the type. Drift between the two is exactly the class of bug this decision is intended to prevent. Zod derives the TypeScript type from the same schema object that is used for runtime validation, eliminating the possibility of type-validator drift.

### Protobuf / gRPC

**Why considered:** Binary serialization with schema-enforced encoding. Breaking changes are structurally impossible to deploy silently — a producer that encodes a field as `string` and a consumer that expects `int32` will fail at the deserialization step. The proto file is the single source of truth.

**Why not chosen:** Protobuf requires a code generation step. The generated TypeScript types are not ergonomic for application code. AMQP is a text/binary envelope protocol; switching to Protobuf for the payload would require custom framing logic in both producer and consumer. The operational complexity of maintaining a proto compiler in the build pipeline is not justified for a two-service system with a small number of event types.

### AsyncAPI specification

**Why considered:** AsyncAPI is a specification standard for event-driven APIs. An AsyncAPI document can serve as the canonical contract definition, with generated validators and types for multiple languages.

**Why not chosen:** AsyncAPI code generation is not mature for TypeScript + Zod. The specification adds an indirection layer between the schema definition and the runtime validator, reintroducing the type-validator drift problem. For a TypeScript-only system, Zod provides better ergonomics with less tooling.

---

## Tradeoffs

| Gains | Costs |
|---|---|
| Schema drift is a build error, not a runtime failure | Both services must be redeployed when a shared contract changes |
| Runtime validation catches payload corruption at both boundaries | `libs/contracts` is a shared mutable dependency — changes require coordination |
| TypeScript type and runtime validator are derived from the same Zod schema — no drift possible | Zod parse errors produce verbose output that must be mapped to structured log fields |
| `EventRegistry` is the single source of truth for all event types in the system | New contributors must understand Zod's `.extend()`, `.discriminatedUnion()`, and `.infer<>` APIs |
| Contract validation is independently testable as a pure function | The contracts library must be compiled and linked before either service can build |

---

## Consequences

1. **Schema changes require a contracts library release.** Changing `CreateMessageEvent.v1` requires incrementing the contracts package version, rebuilding both services, and deploying them. This is a deliberate forcing function that prevents unilateral schema changes. For additive changes, backward compatibility can be maintained by making new fields optional; for breaking changes, a new version must be created (see ADR-005).

2. **`validateEvent()` is called at both boundaries.** This means every event is parsed twice per delivery cycle — once at the producer and once at the consumer. For simple schemas, Zod parse time is sub-millisecond. For schemas with deeply nested objects or large arrays, this overhead is measurable. If Zod parse time becomes a bottleneck, the consumer-side validation can be made conditional on a `SKIP_VALIDATION` flag for known-trusted internal events, at the cost of the consumer-side defense.

3. **The `EventRegistry` must be kept current.** An event type that is not registered in `EventRegistry` cannot be emitted or consumed. This is the desired behavior, but it means that adding a new event type requires a corresponding registry entry before any service can use it.

4. **Consumer schema validation failures are acked, not nacked.** A message that fails Zod validation at the consumer will never become valid through retry — the payload is what it is. Nacking it would cause an infinite retry loop until the DLQ absorbs it. The correct behavior is to ack the message (removing it from the queue), log the validation error with the full payload for forensic inspection, and emit a metric. This is a departure from the intuition that "error = nack."

---

## Operational Impact

### Alert rules

```yaml
- alert: ContractValidationFailureAtProducer
  expr: increase(contract_validation_failures_total{boundary="producer"}[5m]) > 0
  for: 0m
  severity: warning
  annotations:
    summary: "Producer emitted an event that failed schema validation — check gateway logs"

- alert: ContractValidationFailureAtConsumer
  expr: increase(contract_validation_failures_total{boundary="consumer"}[5m]) > 0
  for: 0m
  severity: warning
  annotations:
    summary: "Consumer received an event that failed schema validation — possible schema drift or relay corruption"
```

### Runbook — consumer validation failure

1. Inspect `contract_validation_failures_total{boundary="consumer"}` counter and correlate with recent gateway or relay deployments
2. Retrieve the failing event via `correlationId` in structured logs — the full raw payload is logged at `warn` level on validation failure
3. Determine whether the failure is: (a) schema drift between a newly deployed producer and an older consumer, (b) relay corruption, or (c) a test event with an incorrect payload
4. If (a): identify which field changed and whether the change is backward-compatible. If compatible, add the field as optional to the current schema version. If not, create a new schema version (see ADR-005).
5. If (b): inspect relay logs for buffer or encoding errors
6. If (c): identify the source and fix it; no schema change required

---

## Future Considerations

- **Schema registry**: As the number of event types grows, a centralized schema registry (Confluent Schema Registry, AWS Glue Schema Registry) provides version history, compatibility enforcement, and schema governance independent of the package release cycle. Migration from `libs/contracts` to a schema registry is non-trivial but does not require changing the runtime validation logic — only the source from which schemas are loaded.
- **Cross-language consumers**: If a non-TypeScript consumer (Python, Go) is added, JSON Schema derived from Zod (via `zod-to-json-schema`) can serve as the canonical contract for other language ecosystems. The TypeScript services continue to use Zod natively; other consumers use the derived JSON Schema.
