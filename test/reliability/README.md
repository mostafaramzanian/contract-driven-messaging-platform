# Reliability Test Suite

Production-grade failure-injection tests for the Contract-Driven Messaging Platform. Every test uses **real RabbitMQ**, **real PostgreSQL**, and **real Docker containers**. No mocks. No stubs.

---

## Architecture Under Test

```
HTTP Client
    │
    ▼
Gateway (NestJS)
    │  captureTraceContextCarrier()
    │  OutboxTransactionService.insertOutboxEvents()
    ▼
PostgreSQL                         ◄──── SOURCE OF TRUTH
    │   outbox_events (status: pending → sent | failed)
    │   processed_events (idempotency guard)
    │   event_attempts (durable retry counter)
    │   messages (business rows)
    │
    │  OutboxRelayService (poll every 2s in test config)
    │    ├─ claimBatch()       ← SKIP LOCKED + lock_version bump
    │    ├─ publish()          ← amqplib ConfirmChannel
    │    ├─ waitForConfirms()  ← broker must ACK before markSent()
    │    ├─ markSent()         ← CAS on lock_version (fencing token)
    │    └─ reapStaleLocks()   ← clears dead relay instance locks
    │
    ▼
RabbitMQ (messaging.direct exchange)
    │
    ├──► messaging.work  ──► MessagingController (NestJS consumer)
    │                              ├─ validate schema
    │                              ├─ check processed_events (idempotency)
    │                              ├─ write messages row
    │                              ├─ write processed_events row
    │                              └─ manual ACK
    │
    ├──► messaging.retry.q (TTL → re-enters work queue)
    │
    └──► messaging.dlq (dead letters)
```

---

## Scenarios

| # | Scenario | Failure Injected | Key Invariant |
|---|----------|-----------------|---------------|
| 01 | **Outbox Crash Recovery** | Relay locked row, then `SIGKILL` before `markSent()` | Stale-lock reaper recovers the row; event eventually delivered |
| 02 | **Publisher Confirm Failure** | RabbitMQ paused/stopped mid-confirm | Event stays `pending`; relay retries after broker recovers |
| 03 | **Relay Race Condition** | Two relay instances race for same outbox row | SKIP LOCKED + fencing token = exactly one publish |
| 04 | **Retry Persistence** | Consumer restarted; x-retry-count header reset to 0 | Durable `event_attempts` counter survives; max_attempts enforced |
| 05 | **DLQ Recovery** | Outbox row in `failed` state; operator triggers replay | Replay works; idempotency prevents double-processing |
| 06 | **Trace Continuity** | Outbox relay runs outside HTTP context (no parent span) | `trace_context` column preserves W3C traceparent end-to-end |
| 07 | **RabbitMQ Outage** | Broker container stopped during active publishing | Events accumulate in outbox; drained after broker returns |
| 08 | **PostgreSQL Outage** | Database container stopped during message processing | Consumer NACKs; messages re-delivered and processed after recovery |
| 09 | **Graceful Shutdown** | SIGTERM sent during active processing | In-flight work completes; no message loss; no duplicate side effects |
| 10 | **Chaos Suite** | Random kills of relay + RabbitMQ + consumers; 100× redelivery storm | Eventual consistency; no lost events; no duplicate side effects |

---

## Running the Suite

### Quick start

```bash
# Prerequisites: Docker running, dev stack down, npm install done
./run-reliability-tests.sh
```

### Step by step

```bash
# 1. Build images and start the stack
docker compose -f docker-compose.reliability.yml up -d --build

# 2. Wait for services to be healthy (~30-60s)
docker compose -f docker-compose.reliability.yml ps

# 3. Run all scenarios
npm run test:reliability:run

# 4. Tear down
docker compose -f docker-compose.reliability.yml down -v
```

### Run a single scenario

```bash
# Run only Scenario 01 (crash recovery)
./run-reliability-tests.sh --scenario 01

# Run only the chaos suite
./run-reliability-tests.sh --scenario 10
```

### Skip image rebuild (faster iteration)

```bash
./run-reliability-tests.sh --skip-build
```

### Leave stack running for inspection

```bash
./run-reliability-tests.sh --no-cleanup
# Inspect container state, query Postgres, inspect RabbitMQ UI at http://localhost:15672
# Then tear down manually:
docker compose -f docker-compose.reliability.yml down -v
```

---

## File Structure

```
test/
  reliability/
    jest-reliability.json              # Jest config (maxWorkers=1, no parallelism)
    01-outbox-crash-recovery.reliability-spec.ts
    02-publisher-confirm-failure.reliability-spec.ts
    03-relay-race-condition.reliability-spec.ts
    04-retry-persistence.reliability-spec.ts
    05-dlq-recovery.reliability-spec.ts
    06-trace-continuity.reliability-spec.ts
    07-08-infrastructure-outages.reliability-spec.ts
    09-graceful-shutdown.reliability-spec.ts
    10-chaos-suite.reliability-spec.ts
  utils/
    pg-client.ts          # Direct Postgres client for DB state manipulation
    rabbitmq-client.ts    # Direct amqplib client for queue inspection/injection
    docker-control.ts     # Docker CLI wrappers for container start/stop/kill/pause
    event-tracker.ts      # EventLifecycleSubscriber wrapper (existing)
    wait-for-health.ts    # HTTP + AMQP readiness polling (existing)

docker-compose.reliability.yml  # Test stack with tuned outbox timing knobs
.env.reliability                # Host-side Jest environment variables
run-reliability-tests.sh        # One-shot runner with pre-flight checks
```

---

## Design Principles

### No mocks, no stubs
Every test exercises the real application code against real infrastructure. Failures are injected at the infrastructure level (Docker container stop/pause/kill), not at the code level.

### Honest timing
`pollUntil()` is used only for **eventual-consistency assertions** (did the relay eventually publish?). Infrastructure readiness uses `waitForHttpReady` / `waitForRabbitMqAmqpReady`. This mirrors how the system behaves in production.

### Surgical state injection
Tests inject failures by directly manipulating Postgres rows (setting `locked_at`, `status`, `attempts`) rather than by calling application APIs — this ensures the exact failure state is reproduced deterministically, not just approximated.

### Self-healing assertions
Tests assert that the system self-heals: after a crash, the reaper fires; after an outage, the relay reconnects; after a restart, the consumer resumes. No operator intervention is simulated unless explicitly testing the DLQ replay path.

### Isolation
Each test uses unique `eventId` and `correlationId` values. The idempotency layer and outbox status checks are scoped to these IDs, so tests do not interfere with each other.

---

## Timing Knobs (docker-compose.reliability.yml)

| Variable | Reliability Stack | Production Default | Purpose |
|---|---|---|---|
| `OUTBOX_LOCK_TTL_MS` | 5,000 ms | 60,000 ms | Stale-lock reaper TTL |
| `OUTBOX_POLL_INTERVAL_MS` | 2,000 ms | 5,000 ms | Relay poll frequency |
| `OUTBOX_REAPER_INTERVAL_MS` | 5,000 ms | 30,000 ms | Reaper run frequency |
| `OUTBOX_CLAIM_BATCH_SIZE` | 5 | 20 | Rows claimed per poll |
| `SHUTDOWN_TIMEOUT_MS` | 10,000 ms | 10,000 ms | NestJS graceful shutdown |

The test stack uses shorter intervals so tests complete in seconds rather than minutes, while exercising the **exact same code paths** as production.

---

## CI Integration

```yaml
# .github/workflows/reliability.yml (example)
- name: Run reliability suite
  run: ./run-reliability-tests.sh
  timeout-minutes: 30
  env:
    DOCKER_BUILDKIT: "1"
```

The suite is designed to run in CI with a single command. Total runtime is approximately **15–25 minutes** depending on hardware, dominated by the Chaos Suite (scenario 10).
