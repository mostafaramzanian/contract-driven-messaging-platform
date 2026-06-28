# Evolution Stage Changelog

## Stage 4 — Enterprise Observability (OpenTelemetry + Prometheus + Grafana)

Adds distributed tracing, metrics, and dashboards across the full
gateway → RabbitMQ → messaging → PostgreSQL pipeline.

- `libs/common/src/tracing/otel-bootstrap.ts` — Node SDK bootstrap (OTLP
  trace + metric exporters, auto-instrumentation for http/express/pg/amqplib).
  Imported as the first statement in both `main.ts` entrypoints.
- `libs/common/src/tracing/{tracing.service,amqp-propagation}.ts` — span
  helpers and manual W3C trace-context injection/extraction for AMQP headers.
- `libs/common/src/metrics/*` — `prom-client` registry + `/metrics`
  controller exposing `messages_processed_total`, `messages_failed_total`,
  `dlq_messages_total`, `retry_count_total`, `processing_duration_seconds`,
  `outbox_pending_events`, plus default Node.js process metrics.
- `apps/messaging/src/outbox/*` + migration `004_CreateOutboxEventsTable.ts`
  — transactional outbox pattern backing `outbox_pending_events`.
- `observability/` — otel-collector config, Prometheus scrape + alert
  rules, Grafana datasource/dashboard provisioning and dashboard JSON.
- `docker-compose.yml` — adds `otel-collector`, `jaeger`, `prometheus`,
  `grafana` services; gateway/messaging get `OTEL_*` env vars.
- `test/integration/observability.integration-spec.ts` — asserts both
  `/metrics` endpoints expose the required metric catalogue and that
  counters move when real traffic flows through the pipeline.

---

## Stage 3 — Exactly-Once, Structured Logging, Production Health

This stage evolves the messaging platform from "reliable delivery with
DLQ" (Stage 2) to "production-grade observability and idempotent
processing" (Stage 3).

---

### 1. Idempotency — Exactly-Once Processing

**New migration:** `003_CreateProcessedEventsTable.ts`

Creates `processed_events` table:
- `event_id VARCHAR(36) UNIQUE` — the correctness guarantee
- `event_type`, `correlation_id`, `result JSONB`, `processed_at`
- Indexes on `correlation_id` and `processed_at`

**New entity:** `ProcessedEvent` (TypeORM)

**New service:** `IdempotencyService`
- `checkAndMark({ eventId, eventType, correlationId, result })` — atomic
  INSERT attempt; catches PostgreSQL code 23505 (unique violation) and
  returns `{ isDuplicate: true, processedAt, result }` to callers without
  a SELECT-then-INSERT race condition.
- `findByEventId(eventId)` — read-only lookup for tests and dashboards.

**New module:** `IdempotencyModule`

**Updated controller:** `MessagingController.handleMessage`
- Idempotency gate inserted between validation and business processing
  (Step 2 of the handler pipeline).
- On duplicate: acks the AMQP message, returns cached result, emits no
  `persisted` lifecycle stage (intentional — the event was already counted).
- On idempotency DB failure: nacks to retry/DLQ rather than re-processing.

**Updated `MessagingModule`:** `ProcessedEvent` added to TypeORM entity
list; `IdempotencyModule` imported.

**Updated `typeorm.config.ts`:** `ProcessedEvent` included so the CLI
can generate/diff migrations.

---

### 2. Structured Logging — Pino

**Removed:** `nest-winston`, `winston`, `winston-daily-rotate-file`
**Added:** `pino`, `pino-pretty`

**New factory:** `libs/common/src/logger/pino.factory.ts`
- `createPinoLogger(logLevel, pretty, baseFields)` — root Pino instance.
- `createChildLogger(parent, fields)` — typed child-logger helper.
- Standard structured fields: `correlationId`, `eventId`, `messageId`,
  `traceId` (placeholder `'noop'`), `service`, `operation`.
- Production: raw NDJSON to stdout. Development: pino-pretty with colour.

**New service:** `libs/common/src/logger/pino-logger.service.ts`
- `PinoLoggerService` implements `NestLoggerService`.
- All existing `logger.log(msg, context?)` / `logger.error(msg, stack?,
  context?)` call-sites continue to work without modification.
- New call-sites pass a `PinoBaseFields` object for structured output.
- `child(fields)` method returns a bound child logger for handler scope.

**Updated `LoggerModule`:** provides and exports `PinoLoggerService`
(was: `LoggerService` / Winston).

**Updated gateway `main.ts`:** `app.get(PinoLoggerService)` + `app.useLogger(...)`.
**Updated messaging `main.ts`:** same pattern.

**Updated `MessagingController`:** replaces `new Logger(...)` with
`PinoLoggerService.child({ correlationId, eventId, messageId, ... })` so
every log line within a handler carries all structured fields automatically.

---

### 3. HTTP Logging Middleware

**New middleware:** `libs/common/src/middleware/logging.middleware.ts`
- `LoggingMiddleware` — NestJS `NestMiddleware`.
- Logs incoming requests at DEBUG; responses at INFO / WARN / ERROR based
  on status code.
- Fields: `correlationId`, `traceId`, `operation`, `method`, `url`,
  `userAgent`, `remoteIp`, `statusCode`, `durationMs`.

**Updated gateway `AppModule`:**
- `CorrelationIdMiddleware` applied first, then `LoggingMiddleware` (order
  matters: CID must be set before the logger reads the header).

---

### 4. RMQ Message Logging Interceptor

**New interceptor:** `apps/messaging/src/interceptors/logging.interceptor.ts`
- `LoggingInterceptor` — global `NestInterceptor` registered in
  `main.ts` via `app.useGlobalInterceptors(...)`.
- Logs before and after every RMQ handler with: `correlationId`,
  `eventId`, `messageId`, `traceId`, `service`, `operation`, `durationMs`.
- On error: logs `errorMessage` and `errorStack` then re-throws.
- Extracts AMQP delivery tag as `messageId` from the RmqContext.

---

### 5. Health Checks — @nestjs/terminus

**Added:** `@nestjs/terminus`, `@nestjs/axios`

#### Messaging service — internal HTTP server (port 3006)

**Architecture change: microservice → hybrid app**

`NestFactory.create` (HTTP) + `app.connectMicroservice` (RMQ):
- HTTP server on `HEALTH_PORT` (default 3006) for health probes.
- RMQ consumer unchanged (manual ack, prefetch 10, DLX topology).
- Port 3006 is NOT Nginx-proxied — internal use only.

**New indicator:** `RabbitMQHealthIndicator`
- Extends `HealthIndicator` from `@nestjs/terminus`.
- Dials and immediately closes an AMQP connection with configurable
  timeout (`HEALTH_RABBIT_TIMEOUT_MS`, default 5000 ms).
- Masks credentials in the returned URL field.

**New controller:** `MessagingHealthController`
- `GET /internal/health/ready` — checks PostgreSQL (`TypeOrmHealthIndicator.pingCheck`)
  and RabbitMQ (`RabbitMQHealthIndicator.isHealthy`). Returns 503 on failure.
- `GET /internal/health/live` — empty check array, always 200 while the
  process is alive. Does NOT check infrastructure (liveness ≠ readiness).

**New module:** `MessagingHealthModule`

#### Gateway — HTTP health endpoints

**New controller:** `GatewayHealthController`
- `GET /health/ready` — checks the messaging service via `HttpHealthIndicator.pingCheck`
  against `MESSAGING_HEALTH_URL` (configurable env var).
- `GET /health/live` — empty check, always 200.
- Health routes are NOT under the `/api` global prefix.

**New module:** `GatewayHealthModule`

**Updated `AppModule` (gateway):** imports `GatewayHealthModule`.

---

### 6. Environment & Infrastructure

**Updated `.env.test`:**
- `HEALTH_PORT=3006`
- `MESSAGING_HEALTH_URL=http://messaging:3006/internal/health/ready`
- `SERVICE_NAME=messaging`
- `HEALTH_RABBIT_TIMEOUT_MS=5000`

**Updated `docker-compose.yml`:**
- `messaging` service exposes port 3006.
- `gateway` service receives `MESSAGING_HEALTH_URL`.
- `SERVICE_NAME` injected into both services.

**Updated `docker-compose.test.yml`:**
- Same additions as docker-compose.yml.
- `messaging` gains a Docker healthcheck on `/internal/health/live` so
  the `gateway` container waits until the health HTTP server is ready.

---

### 7. Tests

#### Unit tests (new)
- `IdempotencyService` — 23505 handling, driverError shape, error propagation.
- `PinoLoggerService` — NestLoggerService compat, structured fields, child.
- `LoggingInterceptor` — happy path, error path, field extraction.
- `LoggingMiddleware` — request/response logging, status-code routing.
- `MessagingHealthController` — readiness/liveness delegation.
- `RabbitMQHealthIndicator` — success, timeout, HealthCheckError shape.

#### Integration tests (new)
- `idempotency.integration-spec.ts` — first delivery processed, second
  delivery silently acked, no duplicate DB row, exactly one `processed_events`
  row, schema assertions.
- `health.integration-spec.ts` — gateway and messaging health endpoints,
  response shape, indicator keys, liveness vs readiness separation, SLA.

---

### Breaking changes

| What | Before | After |
|------|--------|-------|
| `LoggerService` type | Winston-backed | Pino-backed (`PinoLoggerService`) |
| `createWinstonLogger` | exported from `@app/common` | removed |
| Messaging `main.ts` | `createMicroservice` | hybrid `create` + `connectMicroservice` |
| Messaging listening port | none (microservice only) | 3006 (HTTP health) |
| `MESSAGE_PORT` env var | not used | `HEALTH_PORT` (default 3006) |

### Non-breaking changes

All existing `Logger.log(msg, context?)` / `.error(msg, stack?, context?)`
call patterns continue to work with `PinoLoggerService`.

The AMQP queue name (`messaging.work`), exchange topology, retry logic,
DLQ consumer, and contract schemas are unchanged.
