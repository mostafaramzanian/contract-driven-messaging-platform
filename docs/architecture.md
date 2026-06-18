# Architecture

## Event Contracts

`libs/contracts` is the source of truth for what an event between the
gateway and the messaging service is allowed to look like. It replaces bare
string-keyed payloads (`client.emit('createMessage', { ...anything })`)
with a small, versioned schema system built on [Zod](https://zod.dev):

- **`eventEnvelopeSchema`** (`libs/contracts/src/events/envelope.schema.ts`)
  defines the fields every event must carry regardless of its payload:
  `eventId` (UUID, unique per logical event), `correlationId` (UUID,
  propagated from the originating HTTP request), `timestamp` (ISO-8601,
  set by the producer), `source` (the service that produced the event:
  `'gateway'` or `'messaging'`), and `trace` (an ordered array of service
  IDs the event has passed through — the lightweight distributed-trace
  model described below).
- **`createMessageEventV1Schema`**
  (`libs/contracts/src/events/v1/create-message.event.ts`) extends the
  envelope with a `type: 'CreateMessageEvent.v1'` discriminator and a
  `payload` of `{ subject, content, recipient? }` — the same fields the
  gateway already sent before contracts existed, not new business fields.
- **`EventRegistry`** (`libs/contracts/src/events/event-registry.ts`) maps
  versioned event-type strings to their schema. `CreateMessageEvent.name`
  (`'CreateMessageEvent.v1'`) is the single source of truth for that
  string — both `AppController.sendTestMessage` (producer) and
  `MessagingController.handleMessage` (consumer, via
  `@MessagePattern(CreateMessageEvent.name)`) reference the constant
  rather than repeating a literal.
- **`validateEvent(eventType, raw)`** validates `raw` against the schema
  registered for `eventType` and returns a discriminated
  `{ valid: true, event } | { valid: false, errors }` result — it never
  throws, so callers decide what "fail fast" means in their own context.

### Versioning

A breaking change to a payload shape gets a new registry entry
(`'CreateMessageEvent.v2'`) rather than mutating the v1 schema in place,
so existing producers/consumers that still speak v1 keep working unchanged
until they are migrated. There is intentionally no automatic
migration/dual-write machinery for this yet — nothing in this codebase
needs it — but the registry's keying scheme is what makes adding a v2
straightforward later: a new schema file under `events/v2/`, a new
registry entry, and a second `@MessagePattern` on the consumer for as
long as both versions need to be supported simultaneously.

### Fail-fast enforcement

- **Gateway**: `AppController.sendTestMessage` builds the event via
  `buildCreateMessageEventV1` (which fills in `eventId`, `timestamp`,
  `source: 'gateway'`, and `trace: ['gateway']` consistently) and calls
  `validateEvent` before calling `client.emit`. An invalid event — for
  example, a caller-supplied `x-correlation-id` header that is not a UUID —
  is never emitted to RabbitMQ; the gateway logs a structured validation
  failure and responds `400 Bad Request` with the validation errors.
- **Messaging**: `MessagingController.handleMessage` calls `validateEvent`
  again, independently, on whatever arrives over RabbitMQ. The RMQ
  transport here runs with `noAck: true` (the `@nestjs/microservices`
  default for this app), meaning the broker already removes a message from
  the queue at delivery time regardless of whether the handler succeeds or
  throws. So "reject, log, drop, do not retry" for an invalid event does
  not require any special nack/requeue logic — it only requires the
  handler to log the structured validation failure and return without
  acting on the payload, which is what it does.

This means two independent validations exist for the same contract — once
at the producer, once at the consumer — rather than trusting that whatever
the gateway sent is automatically what the messaging service will see.

## Distributed Trace Model (Lightweight)

Every event's `trace` field is an ordered list of the services it has
passed through. The gateway initializes it to `['gateway']` when building
the event; the messaging service's handler computes
`[...event.trace, 'messaging']` once it has validated the event, purely to
reflect the hop in its own logging (the contract schema itself does not
require appending to `trace` before re-validating — there is only one
consumer today, so there is nothing to forward the appended trace *to*).

This is deliberately not a general-purpose distributed tracing system: there
are no spans, no parent/child span relationships, and no integration with
an external tracing vendor or OpenTelemetry. It exists to answer one
question cheaply for a two-hop system — "which services has this event
been through?" — not to support arbitrary fan-out topologies.

## Event Lifecycle Tracing (Test-Only Observability)

Separately from the contract system above, `libs/contracts/src/lifecycle`
implements an opt-in mechanism for observing an event's lifecycle
(`emitted`, `received`, `validated`, `rejected`, `persisted`) as it
actually happens, without querying application state:

- `EventLifecyclePublisher` publishes a small JSON record to a dedicated
  RabbitMQ **fanout exchange** (`event-lifecycle.test`) — separate from the
  `messaging_queue` used for real business events — whenever the gateway or
  messaging service reaches one of those stages for a given `eventId`.
  Publishing is gated behind the `EVENT_LIFECYCLE_TRACING` environment
  variable (set to `'true'` only in `.env.test`, never in `.env.example` or
  the dev `docker-compose.yml`) and every failure is caught and logged
  rather than thrown, so this mechanism can never affect or break the real
  event flow it is observing, even if RabbitMQ is briefly unavailable for
  the lifecycle channel specifically.
- `EventLifecycleSubscriber` (wrapped by `test/utils/event-tracker.ts`'s
  `EventTracker`) connects to the same broker, binds its own
  exclusive/auto-delete queue to the fanout exchange, and lets a caller
  `await` the exact moment a specific `eventId` reaches a specific stage —
  resolving immediately on arrival rather than on a polling interval, and
  rejecting with a clear timeout error if the stage never occurs.

This exists specifically so the integration test (see below) can assert
"did this event reach the `persisted` stage" as a real, event-driven signal
instead of polling the `messages` table and inferring the answer from
whether a matching row eventually showed up.

## Service Boundaries

The system is split into independently deployable applications under
`apps/`:

- **`apps/gateway`** — the API gateway. It is the only service that accepts
  HTTP traffic from outside the cluster. It has no database connection of
  its own.
- **`apps/messaging`** — a NestJS microservice. It does not expose an HTTP
  server; it listens for messages on a RabbitMQ queue (`messaging_queue`)
  and is the only service connected to PostgreSQL.

Each service has its own `tsconfig.app.json` and is built and run
independently (`nest start gateway`, `nest start messaging`), but both share
the `@app/common` library for logging and correlation ID handling, and the
`@app/contracts` library for event schemas, the event registry, and event
lifecycle tracing.

The boundary is drawn around a business domain (messages) rather than
around a technical layer (e.g. "all controllers" vs "all services"). The
intent is that additional domains, if added, would follow the same shape:
their own app under `apps/`, their own database ownership, their own message
queue, communicating with the gateway and other services asynchronously via
RabbitMQ — and their own versioned entries in `EventRegistry` for whatever
events they produce or consume.

## Message Flow

The implemented flow, end to end:

1. A client sends an HTTP request to the gateway (`GET /api/test-rabbit` in
   the current implementation).
2. `CorrelationIdMiddleware` (applied to all routes in `AppModule`) ensures
   the request has an `x-correlation-id` header, generating one with `uuid`
   if absent, and sets it on the response. The value is not yet validated
   as a UUID at this point — see step 4.
3. `AppController.sendTestMessage` reads the correlation ID from the
   request headers and calls `buildCreateMessageEventV1` to construct a
   complete `CreateMessageEvent.v1` (with a fresh `eventId`, current
   `timestamp`, `source: 'gateway'`, `trace: ['gateway']`, and the
   `{ subject, content }` payload).
4. `validateEvent('CreateMessageEvent.v1', event)` checks the constructed
   event against its schema. This is what actually enforces "correlationId
   must be a UUID" end to end — a non-UUID `x-correlation-id` header fails
   here, before anything is emitted. If validation fails, the gateway logs
   a structured rejection, publishes a `rejected` lifecycle record (see
   above), and responds `400 Bad Request`; nothing reaches RabbitMQ.
5. If validation passes, `AppController` calls
   `client.emit(CreateMessageEvent.name, result.event)` on the
   `MESSAGING_SERVICE` RabbitMQ client and publishes an `emitted` lifecycle
   record. `emit` is fire-and-forget — the gateway does not wait for the
   messaging service to process the event; it returns `{ status: 'success',
   correlationId, eventId, eventType }` to the HTTP caller immediately.
6. RabbitMQ delivers the message to the `messaging_queue`, which is declared
   `durable: true` by the messaging service.
7. `MessagingController.handleMessage` (a
   `@MessagePattern(CreateMessageEvent.name)` handler) receives the raw
   payload and calls `validateEvent` again, independently of the gateway's
   validation. A failure here is logged and published as a `rejected`
   lifecycle record (tagged `service: 'messaging'` this time) and the
   handler returns without processing the message further — see "Fail-fast
   enforcement" above for why no nack/requeue is needed.
8. On success, the handler publishes a `received` lifecycle record, then a
   `validated` lifecycle record, then calls
   `MessagingService.handleMessageCreation` with the validated
   `event.payload` and `event.correlationId`.
9. `MessagingService.handleMessageCreation` constructs a `Message` entity
   and persists it via the TypeORM repository. On success, the handler
   publishes a `persisted` lifecycle record.
10. Both the gateway and the messaging service log each step through
    `Logger`, including the correlation ID, so the full request can be
    traced across both services' logs (see `docs/observability.md`).

A second handler, `@MessagePattern('test-rabbit')`, exists on the messaging
controller but nothing in this codebase emits to that pattern — it predates
the `CreateMessageEvent.v1` contract and is unused dead code, kept as-is
since removing it is unrelated to the contract system above.

The `apps/messaging/src/messages` module implements a full CRUD message API
(`createMessage`, `findAllMessages`, `findOneMessage`, `updateMessage`,
`removeMessage`) against the same `Message` entity, but the gateway does not
currently call these patterns, and they are not yet part of `EventRegistry`
— see Project Status below.

## Integration Testing

`test/integration/messaging-flow.integration-spec.ts` verifies the flow
described above end-to-end, against real infrastructure, using the event
lifecycle tracing mechanism instead of database polling. See
[`docs/testing.md`](./testing.md) for the full description of how the test
is structured and what it asserts.

### Limitations

- The `/api/test-rabbit` endpoint sends a fixed payload, so the test cannot
  yet verify arbitrary input. Extending this to the `messages` CRUD API
  (once wired to the gateway and registered in `EventRegistry`) would allow
  testing with caller-supplied data.
- Event lifecycle tracing (`EVENT_LIFECYCLE_TRACING`) is test-only by
  design — it is not a general-purpose production observability or tracing
  system, and it is not enabled in `.env.example` or the dev
  `docker-compose.yml`. Lifecycle records are also not currently persisted
  anywhere; they exist only as long as a subscriber is connected to observe
  them in real time.
- Running `npm run test:integration` requires Docker and Docker Compose on
  the host, and will fail in environments without a working Docker daemon
  or without network access to pull the `postgres:15` and
  `rabbitmq:3-management` images.

## Why RabbitMQ

RabbitMQ is used for all communication between the gateway and the messaging
service, instead of direct HTTP calls between services, for two main
reasons:

- **Decoupled availability**: the gateway can accept and queue a request
  even if the messaging service is temporarily down or restarting, because
  RabbitMQ holds durable messages for a durable queue until a consumer is
  available.
- **Decoupled deployment**: the gateway and the messaging service can be
  deployed, scaled, and restarted independently without one needing to know
  the network location or availability of the other at the moment of the
  call — they only need to agree on the message contract.

The tradeoff is that the gateway cannot return a result of the messaging
service's processing in the same request/response cycle, since `emit` does
not wait for a reply. For the current `create_message` flow this is
acceptable, since the gateway's response to the client confirms that the
message was published, not that the record was persisted.

## Why a Modular Monorepo (and Not a Tightly-Coupled Monolith)

A common failure mode in systems that grow organically is for the frontend,
backend, and a single shared database schema to be deployed and changed
together, even when the underlying business domains are largely
independent. This tends to create two recurring problems:

- A change to one domain's code or schema risks breaking unrelated domains,
  because everything shares the same deploy and the same schema.
- There is no clear ownership boundary for data — any part of the code can
  read or write any table.

The structure in this repository — one app per business domain, each owning
its own database connection and schema, communicating via well-defined
messages over RabbitMQ — is intended to make those boundaries explicit. A
change to the messaging service's internal schema or logic does not require
redeploying the gateway, and the gateway cannot accidentally read or write
the messaging service's tables directly.

This is not a full microservices architecture with independent scaling or
separate databases-per-instance in production; it is a modular structure
within a single monorepo, chosen so that splitting services apart further
(or consolidating them) remains possible without a rewrite, while avoiding
the operational overhead of a large number of separately-deployed services
at this stage.
