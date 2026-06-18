# Testing

This repository has four layers of tests, mirroring the levels described
below: contract tests, unit tests, a module bootstrap (e2e) test, and a
full-stack integration test.

## Contract Tests

```bash
npm run test:contracts
```

Runs every `*.spec.ts` file under `libs/contracts/` via Jest
(`jest --testPathPatterns=libs/contracts`). These tests have no
infrastructure dependency at all — no Docker, no Postgres, no RabbitMQ —
and run in under a second. Two files:

- `libs/contracts/src/index.spec.ts` — verifies `buildCreateMessageEventV1`
  and `validateEvent` against the `CreateMessageEvent.v1` schema: a
  correctly-built event validates, each required field's absence is
  individually rejected, an unknown `source`/`trace` service ID is
  rejected, an unexpected top-level field is tolerated (Zod strips unknown
  keys rather than rejecting the whole object), and so on.
- `libs/contracts/src/events/v1/create-message.compat.spec.ts` — backward
  compatibility / breaking-change detection. It validates a frozen,
  hand-written example event (`FROZEN_V1_EVENT`) against the *current*
  schema on every run. If a future edit to `createMessageEventV1Schema`
  makes that unchanged fixture fail validation, that is by definition a
  breaking change to the v1 contract — the fix is a new `v2` schema and
  registry entry (see `docs/architecture.md` → "Versioning"), not editing
  the fixture to match. The same file also asserts that
  `CreateMessageEvent.name` is still the literal string
  `'CreateMessageEvent.v1'` and that both `'gateway'` and `'messaging'`
  remain valid service identifiers.

This runs as the first step of CI, before lint, build, or any other test,
specifically because it is the fastest possible signal and does not
require a build step.

## Unit Tests

```bash
npm run test
```

Runs all `*.spec.ts` files under `apps/` via Jest, configured in
`package.json`. These tests use NestJS's `TestingModule` with mocked
TypeORM repositories (`jest.Mocked<Repository<...>>`) and mocked
`Logger` calls, so they run without any external infrastructure.

Covered:

- `apps/gateway/src/app.controller.spec.ts` — verifies `AppController`'s
  root endpoint response, and `sendTestMessage`'s contract behavior: a
  valid correlation ID results in exactly one `client.emit` call with a
  `CreateMessageEvent.v1`-shaped payload and a published `emitted`
  lifecycle record; a non-UUID correlation ID results in no `emit` call,
  a thrown `BadRequestException`, and a published `rejected` lifecycle
  record instead. The `MESSAGING_SERVICE` RabbitMQ client and the
  `EVENT_LIFECYCLE_PUBLISHER` are both mocked.
- `apps/messaging/src/messaging.service.spec.ts` — verifies
  `MessagingService.handleMessageCreation` against a mocked `Message`
  repository, including default-value handling and error propagation on
  save failure.
- `apps/messaging/src/messages/messages.service.spec.ts` — verifies the
  `messages` CRUD service (`create`, `findAll`, `findOne`, `update`,
  `remove`), including the `NotFoundException` path.
- `apps/messaging/src/messages/messages.controller.spec.ts` — verifies
  that the `messages` CRUD `@MessagePattern` handlers delegate to the
  corresponding service methods.
- `apps/messaging/src/messaging.controller.spec.ts` — verifies
  `handleTestRabbit`'s delegation to the service layer, and
  `handleMessage`'s contract behavior: a valid `CreateMessageEvent.v1`
  payload is validated, persisted via the (mocked) service, and produces
  `received` → `validated` → `persisted` lifecycle records in order; an
  invalid payload is dropped (the service is never called) and produces a
  single `rejected` lifecycle record instead.

## Module Bootstrap Test (e2e)

```bash
npm run test:e2e
```

Runs `apps/gateway/test/app.e2e-spec.ts`, which boots the full `AppModule`
via `Test.createTestingModule` and `app.init()`, then issues a real HTTP
request to the in-memory Nest application. It asserts that `GET /` returns
`200` with the JSON body produced by `AppController.getRoot()`:

```json
{
  "message": "Welcome to the Messaging Showcase platform",
  "status": "active",
  "endpoints": {
    "api": "/api",
    "testRabbit": "/api/test-rabbit"
  }
}
```

This confirms the module wires together correctly (providers, controllers,
middleware) without requiring RabbitMQ or PostgreSQL. `npm run test:e2e`
does not set `RABBITMQ_URL` at all, so `AppModule`'s RabbitMQ client falls
through to the application's own built-in default
(`amqp://guest:guest@showcase-rabbitmq:5672` — see
`apps/gateway/src/app.module.ts`). On a host where that hostname does not
resolve (true for an unmodified developer machine and for the GitHub
Actions runner, since `showcase-rabbitmq` is only resolvable inside
`docker-compose.yml`'s own bridge network), the connection attempt in
`AppController.onModuleInit` fails immediately via DNS lookup failure,
which is caught and logged rather than re-thrown, so the test still passes
quickly and deterministically. There is no test-specific port override or
script-level `RABBITMQ_URL` value to keep in sync with anything else — the
only RabbitMQ address that exists anywhere in this codebase is the one
already used by the real application (`docker-compose.yml`,
`docker-compose.test.yml`, and this fallback all agree on `5672` as the
broker's port).

There is no separate e2e bootstrap test for the messaging microservice: as a
`@nestjs/microservices` RMQ transport, it requires a real RabbitMQ
connection to start, so its bootstrap is instead exercised by the full-stack
integration test below (the `messaging` service in
`docker-compose.test.yml` boots against a real RabbitMQ/PostgreSQL stack).

## Full-Stack Integration Test

```bash
npm run test:integration
```

`npm run test:integration` runs the following steps in sequence:

1. `npm run test:integration:up` brings up `docker-compose.test.yml`:
   isolated PostgreSQL (`localhost:5432`) and RabbitMQ (`localhost:5672`/
   `15672`) containers, a one-shot `migrate` container that runs TypeORM
   migrations against the test database, and the real `gateway`
   (`localhost:3005`) and `messaging` applications built from the project's
   `Dockerfile`. These host-mapped ports are intentionally the same port
   numbers the dev stack (`docker-compose.yml`) uses — see the header
   comment in `docker-compose.test.yml` for why, and the tradeoff this
   implies (the two stacks cannot run at the same time). `.env.test` sets
   `EVENT_LIFECYCLE_TRACING=true` for these containers, which is what makes
   the event-driven assertions below possible — see "Event Lifecycle
   Tracing" in `docs/architecture.md`.
2. A fixed 10-second wait (`node -e "setTimeout(() => process.exit(0), 10000)"`).
   This is a coarse safety margin inherited from before the test had its
   own readiness checks; it is not what the test actually relies on for
   correctness — see step 3.
3. `npm run test:integration:run` runs the Jest spec
   (`test/integration/messaging-flow.integration-spec.ts`) against that
   stack, with `NO_PROXY=127.0.0.1`, `HTTP_PROXY=`, and `HTTPS_PROXY=` set
   via `cross-env` so the test runner's requests to `localhost`/`127.0.0.1`
   are not routed through any proxy configured in the host environment.
4. `npm run test:integration:down` tears down the stack
   (`docker-compose -f docker-compose.test.yml down -v`).

### What the spec actually does

`test/integration/messaging-flow.integration-spec.ts` contains two tests,
both against the real, unmocked stack:

**Happy path** — `beforeAll` first calls `waitForHttpReady` and
`waitForRabbitMqReady` (`test/utils/wait-for-health.ts`) to confirm the
gateway's HTTP server and the RabbitMQ broker are actually accepting
connections; this is a bounded infrastructure-readiness check, not a
substitute for the assertions that follow. It then connects an
`EventTracker` (`test/utils/event-tracker.ts`) to the same RabbitMQ broker
the real services use, and a plain `pg.Client` to the test database. The
test itself:

1. Sends a real `GET /api/test-rabbit` request to the gateway container and
   asserts the response is `200` with a `CreateMessageEvent.v1` `eventId`
   and `correlationId`, both UUID-shaped.
2. Calls `tracker.waitForFullChain(eventId)`, which awaits — in order, with
   no fixed-interval polling — the `emitted`, `received`, `validated`, and
   `persisted` lifecycle records for that exact `eventId`, each resolving
   the instant the corresponding RabbitMQ message arrives on the
   lifecycle-tracking exchange (or rejecting with a clear timeout error if
   one never does).
3. Asserts that `correlationId` is identical across all four records (the
   end-to-end correlation check the previous DB-polling version of this
   test could not perform, since the `Message` entity has no
   `correlationId` column — see "Limitations" in `docs/architecture.md`),
   that each record is tagged with the correct producing service
   (`gateway` for `emitted`, `messaging` for the other three), and that
   their timestamps are non-decreasing.
4. Only after `persisted` has been confirmed does it run a single,
   non-polling `SELECT` against the real database to confirm the row's
   `title`, `content`, `sender`, `id`, and `createdAt` fields — this is a
   read to confirm what was already known to be true, not a wait.

**Rejection path** — a second test sends `GET /api/test-rabbit` with an
`x-correlation-id` header that is not a valid UUID, asserts a `400`
response with `status: 'rejected'` and a `correlationId`-path validation
error, then awaits a `rejected` lifecycle record for that event's `eventId`
(tagged `service: 'gateway'`) to confirm the rejection path actually ran.
Because the event is rejected before ever being emitted, there is no
`received`/`validated`/`persisted` record to wait for and nothing written
to the database — the test does not need to (and does not) assert an
absence by polling, since there is nothing to poll for in the first place.

### Why this replaces DB polling

A polling loop against the `messages` table can only ever answer "did a
row with a matching value show up by the time I gave up looking?" — it
cannot say the row came from *this* request, that contract validation
happened on both ends, or in what order things occurred. The lifecycle
records are published by the request-handling code at the moment each
step actually happens, so waiting on them is bounded by the real event
(resolving immediately on arrival, not on the next poll tick) and answers
the more specific question this test suite is built around: did a
validated, schema-compliant event propagate correctly through the
distributed system, for this exact event, in the right order, with the
correlation ID intact at every hop.

### Requirements

- Docker and Docker Compose must be available on the host.
- The host must have network access to pull the `postgres:15` and
  `rabbitmq:3-management` images (and any base images referenced by the
  project `Dockerfile`) on first run.

### Known Limitations

See "Limitations" in [`docs/architecture.md`](./architecture.md#limitations)
for what this test does and does not cover (e.g. it exercises a fixed
payload via `/api/test-rabbit` rather than caller-supplied input, and
lifecycle records are not persisted anywhere outside the test run itself).

## Continuous Integration

`.github/workflows/ci.yml` runs on every push and pull request, in two
sequential jobs:

1. **Build & unit test job**: `npm ci`, contract tests (`npm run
   test:contracts`, first and fastest), lint, build both apps (`gateway`
   and `messaging`), unit tests, and the module bootstrap (e2e) test — all
   without external infrastructure.
2. **Integration test job** (runs after the first job passes): brings up
   the real stack via `npm run test:integration:up` (the same
   `docker-compose.test.yml` used locally), waits for every container to
   report healthy (`docker-compose ... ps`, polling on the existing
   Postgres/RabbitMQ healthchecks instead of relying solely on the fixed
   sleep), runs `npm run test:integration:run`, uploads container logs as
   a build artifact if anything fails, and always tears the stack down
   afterward.

Both jobs have timeouts so a hung container or test does not block the
workflow indefinitely.
