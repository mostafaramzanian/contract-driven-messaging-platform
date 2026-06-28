# Runbook: Trace Propagation Failure

| Field | Value |
|---|---|
| **ID** | RB-005 |
| **Alert** | `TraceOrphanSpansElevated` |
| **Severity** | Warning |
| **SLO Impact** | Indirect. No data is lost. However, distributed debugging becomes significantly harder — a trace without context propagation appears as an unrelated root span, making it impossible to correlate a consumer failure with its originating HTTP request in Jaeger without manual log correlation by `correlationId`. |
| **On-call Action** | Investigate during business hours unless combined with active consumer failures. At 3 AM, confirm this alert is not a leading indicator of a consumer processing failure before deprioritizing. |
| **Last Updated** | 2024-01-18 |

---

## Symptoms

The W3C `traceparent` / `tracestate` context is not propagating correctly from the HTTP request through the outbox relay to the consumer. Evidence:

- `trace_orphaned_root_spans_total` > 0 and increasing
- In Jaeger: relay publish spans and consumer handler spans appear as root spans with no parent trace, disconnected from the originating HTTP request
- The span waterfall for a given `correlationId` is broken into two or more disconnected traces
- `e2e_request_duration_ms` histogram is missing data points (traces without context cannot be attributed to the originating request)
- On-call is receiving a separate alert and attempting to trace the request flow — finds broken traces in Jaeger

**What is NOT happening:**
- Events are still being published and consumed correctly
- Business data is not lost
- The outbox and relay mechanisms are functioning

**Why this matters at 3 AM:** If you are debugging an active consumer failure and you cannot trace the request in Jaeger, this alert is making your job significantly harder. Fixing trace propagation during an active incident is not feasible, but knowing the limitation is important for your investigation strategy.

---

## Detection Signals

### Primary alert

```yaml
alert: TraceOrphanSpansElevated
expr: increase(trace_orphaned_root_spans_total[5m]) > 5
for: 2m
severity: warning
```

A small number of orphan spans (1–2 per 5 minutes) may be acceptable due to service restarts or test traffic. More than 5 per 5-minute window indicates a systematic regression.

### Supporting queries

```promql
# Orphan span rate over time
rate(trace_orphaned_root_spans_total[1m])

# Span volume by service (a drop in relay or consumer spans relative to gateway indicates context loss)
rate(spans_total{service="gateway"}[1m])
rate(spans_total{service="relay"}[1m])
rate(spans_total{service="consumer"}[1m])

# Are errors correlated with trace loss? (context loss may hide errors)
rate(spans_errors_total[1m]) / rate(spans_total[1m])

# OTel collector health
up{job="otel-collector"}
```

---

## Metrics

| Metric | Normal | Incident |
|---|---|---|
| `trace_orphaned_root_spans_total` rate | 0 | > 0 and sustained |
| `spans_total{service="relay"}` rate | proportional to `spans_total{service="gateway"}` | lower (context lost before relay) or 0 |
| `spans_total{service="consumer"}` rate | proportional to relay spans | lower or 0 |
| `up{job="otel-collector"}` | 1 | may be 0 if collector is down |
| `trace_context_restore_failures_total` | 0 | > 0 |

---

## Grafana Panels

**Dashboard:** `cdmp-distributed-tracing` (UID: `cdmp-distributed-tracing`)

1. **Stat: Root span orphans** — non-zero value is the trigger. The number tells you the scope.
2. **Time series: Trace volume by service** — a service whose span volume drops relative to others is the loss point
3. **Panel: Span waterfall** — broken waterfall (relay or consumer spans not connected to gateway span) confirms propagation failure
4. **Time series: Trace error rate** — errors that appear only in consumer spans (no parent) are invisible in the gateway trace tree

---

## Root Cause Analysis

Trace context propagation across the outbox boundary works as follows:

1. **Capture:** At outbox INSERT time, the active W3C `traceparent` is serialized to the `trace_context` column of `gateway_outbox_events`
2. **Persist:** The `trace_context` value is stored durably alongside the outbox row
3. **Restore:** The relay reads `trace_context` from the outbox row and calls `context.with(propagator.extract(ROOT_CONTEXT, carrier), ...)` before creating the relay publish span
4. **Forward:** The relay injects the restored context into the AMQP message headers as `traceparent`
5. **Consumer restore:** The consumer extracts `traceparent` from the AMQP message headers and creates its spans as children of the relay span

A break at any step produces orphan spans. Identify the step.

### Cause A: `trace_context` column is NULL at INSERT

The `trace_context` is not being captured at outbox INSERT time. This happens when:
- The HTTP handler is not executing within an active span (the OTel middleware is disabled or misconfigured)
- The `captureTraceContext()` call is missing from the outbox service
- A code change removed the trace capture step

**Signals:** `trace_context` column is `NULL` for recent outbox rows. Relay logs show `trace_context: null` or `trace_context: undefined`. The relay cannot restore a context that was never captured.

### Cause B: Relay is not restoring context before creating spans

The relay reads `trace_context` from the outbox row but either fails to parse it or does not call `context.with()` before the publish span is created. The publish span becomes a new root span rather than a child.

**Signals:** `trace_context` column has a value, but relay publish spans appear as root spans in Jaeger. The `trace_context_restore_failures_total` counter is > 0. Relay logs may show `invalid traceparent` or `context restore failed`.

### Cause C: OTel Collector is dropping or not receiving spans

The spans are created with correct parent-child relationships, but the OTel Collector is not exporting them to Jaeger. Spans that are not exported appear as orphans from Jaeger's perspective.

**Signals:** `up{job="otel-collector"}` = 0. Collector logs show export errors. Spans are created in the application but do not appear in Jaeger. The issue is in the export pipeline, not in the propagation logic.

### Cause D: AMQP `traceparent` header not forwarded to consumer

The relay creates the relay publish span with correct context but fails to inject `traceparent` into the AMQP message headers. The consumer has no context to restore.

**Signals:** The relay publish span is a child of the gateway span (correct), but the consumer handler span is a new root span (context lost at the AMQP boundary). Inspect the AMQP message headers: `traceparent` header is absent.

### Cause E: Consumer not extracting context from AMQP headers

The `traceparent` header is present in the AMQP message, but the consumer does not call `propagator.extract()` on the message headers before creating its spans.

**Signals:** `traceparent` header is present in the AMQP message (verify via message inspection), but consumer handler spans appear as root spans. This is a code regression in the consumer's AMQP handler setup.

### Cause F: OTel SDK version mismatch between services

A version change in the OTel SDK may produce `traceparent` headers in a format that the other service's SDK version cannot parse. The extract call fails silently and returns an empty context.

**Signals:** The issue appeared immediately after an OTel SDK dependency update. Both services' package.json show different OTel SDK versions. The `traceparent` format is correct (W3C) but the versions disagree on encoding details.

---

## Investigation Steps

### Step 1 — Confirm orphan spans in Jaeger (2 minutes)

```bash
# Open Jaeger UI and search for traces without a parent
# Jaeger query: service=relay, operation=relay.publish_with_confirm, limit=20
# Look for traces where the relay span is a root span (no parent in the trace timeline)

# Or via Jaeger API:
curl -s "http://localhost:16686/api/traces?service=relay&limit=10" \
  | jq '.data[] | {traceID: .traceID, rootSpan: .spans[0].references}'
```

If `spans[0].references` is empty for relay spans, the relay spans have no parent — confirmed orphan.

### Step 2 — Check the `trace_context` column (2 minutes)

```sql
-- Sample recent outbox rows — is trace_context being captured?
SELECT id, event_id, status, created_at,
       trace_context IS NOT NULL AS has_trace_context,
       LEFT(trace_context::text, 60) AS trace_context_preview
FROM gateway_outbox_events
ORDER BY created_at DESC
LIMIT 20;

-- What percentage of recent rows have trace context?
SELECT
  COUNT(*) AS total,
  COUNT(trace_context) AS with_context,
  ROUND(COUNT(trace_context) * 100.0 / COUNT(*), 1) AS pct_with_context
FROM gateway_outbox_events
WHERE created_at > NOW() - INTERVAL '10 minutes';
```

**If `pct_with_context` < 100%:** Cause A — context not captured at INSERT.
**If `pct_with_context` = 100%:** Context is captured. The loss is downstream (Causes B–F).

### Step 3 — Inspect the relay's context restore log (2 minutes)

```bash
# Check if the relay is finding and restoring trace context
grep '"operation":"relay.poll"\|"operation":"relay.publish_with_confirm"' \
  /var/log/gateway-service/*.log \
  | tail -20 \
  | jq '{
      time: .time,
      operation: .operation,
      eventId: .eventId,
      traceContextFound: .traceContextFound,
      traceId: .traceId,
      parentSpanId: .parentSpanId
    }'
```

**If `traceContextFound: false`:** Cause A (not captured) or Cause B (restore failing).
**If `traceContextFound: true` but `parentSpanId` is null/empty:** Cause B (restore code path not working).
**If `traceContextFound: true` and `parentSpanId` is populated:** Context is restored in the relay. Loss is at the AMQP header or consumer side (Causes D or E).

### Step 4 — Inspect AMQP message headers (3 minutes)

```bash
# Get a sample message from the work queue WITHOUT consuming it
curl -s -u guest:guest \
  http://localhost:15672/api/queues/%2F/messaging.work/get \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"count":1,"requeue":true,"encoding":"auto","ackmode":"ack_requeue_true"}' \
  | jq '.[].properties.headers | {traceparent, tracestate, correlationId}'
```

**If `traceparent` is absent:** Cause D — relay not injecting into AMQP headers.
**If `traceparent` is present:** Cause E or F — consumer not extracting, or version mismatch.

### Step 5 — Check OTel Collector (1 minute)

```bash
# Is the collector up?
curl -s http://localhost:13133/ | jq .  # health check endpoint

# Collector pipeline errors
docker logs otel-collector 2>&1 | grep -i "error\|failed\|dropped" | tail -20

# Prometheus metric
curl -s http://localhost:9090/api/v1/query \
  -d 'query=up{job="otel-collector"}' \
  | jq '.data.result[0].value[1]'
```

---

## Recovery Procedure

### Recovery A: Context not captured at INSERT — fix code and deploy

```bash
# Identify the commit that removed trace capture
git log --oneline apps/gateway/src/outbox/ | head -10

# Confirm the issue: captureTraceContext() must be called within an active span
# In the gateway handler, the OTel middleware must run before the route handler

# Fix: ensure trace capture is called with an active context
# apps/gateway/src/outbox/outbox.service.ts
# const traceContext = captureCurrentTraceContext();  // must be inside active span
# await this.outboxRepo.save({ ..., trace_context: traceContext });

# Deploy the fix
docker compose up -d --build gateway-service

# Verify: new rows have trace_context populated
psql $DATABASE_URL -c "
SELECT COUNT(trace_context) * 100.0 / COUNT(*) AS pct_with_context
FROM gateway_outbox_events
WHERE created_at > NOW() - INTERVAL '2 minutes';"
# Expected: 100.0
```

### Recovery B: Context restore failing — fix relay code

```bash
# Check relay context restore logic
# The correct pattern:
# const carrier = JSON.parse(row.trace_context);
# const ctx = propagator.extract(ROOT_CONTEXT, carrier);
# return context.with(ctx, async () => {
#   // create spans here — they will be children of the restored context
# });

# Common mistakes:
# 1. Parsing trace_context outside context.with()
# 2. Not passing the extracted context to context.with()
# 3. Missing await on the context.with() callback

# After fix, verify relay publish spans have parentSpanId
grep '"operation":"relay.publish_with_confirm"' /var/log/gateway-service/*.log \
  | tail -5 \
  | jq '{traceId: .traceId, parentSpanId: .parentSpanId}'
# Expected: parentSpanId is populated (16 hex chars)
```

### Recovery C: OTel Collector down — restart and verify

```bash
# Restart the collector
docker compose restart otel-collector

# Verify it comes up
sleep 5
curl -s http://localhost:13133/ | jq .

# Verify spans are flowing to Jaeger
# Wait 30 seconds for spans to appear in Jaeger
# Search for traces from the last 5 minutes
```

**Note:** Spans emitted while the collector was down are lost from the trace backend. The underlying events were processed correctly; only the observability record is missing. This is not a data integrity event.

### Recovery D/E: AMQP header injection or consumer extraction — fix code

```bash
# Verify the relay injects traceparent
# apps/gateway/src/outbox/gateway-outbox-relay.service.ts
# channel.publish(exchange, routingKey, content, {
#   headers: {
#     ...existingHeaders,
#     traceparent: carrier.traceparent,   // must be injected
#     tracestate: carrier.tracestate,
#   }
# });

# Verify the consumer extracts traceparent
# apps/messaging/src/amqp/messaging.controller.ts
# const carrier = msg.properties.headers;
# const ctx = propagator.extract(ROOT_CONTEXT, carrier);
# return context.with(ctx, async () => {
#   // handle message here
# });
```

### Recovery F: OTel SDK version mismatch — align versions

```bash
# Check current versions
cat apps/gateway/package.json | jq '."dependencies" | to_entries[] | select(.key | startswith("@opentelemetry"))'
cat apps/messaging/package.json | jq '."dependencies" | to_entries[] | select(.key | startswith("@opentelemetry"))'

# Pin both to the same version in the monorepo root package.json
# "@opentelemetry/api": "1.x.x",
# "@opentelemetry/sdk-node": "0.x.x",

# Rebuild and redeploy both services
npm install
docker compose up -d --build
```

---

## Validation Checklist

- [ ] `trace_orphaned_root_spans_total` rate = 0 for at least 5 minutes
- [ ] `trace_context` column is non-null for 100% of recent `gateway_outbox_events` rows
- [ ] A sample AMQP message from `messaging.work` contains `traceparent` in its headers
- [ ] In Jaeger: a recent trace shows gateway span → relay span → consumer span as a connected waterfall
- [ ] `up{job="otel-collector"}` = 1
- [ ] `spans_total{service="relay"}` rate is proportional to `spans_total{service="gateway"}` rate
- [ ] `spans_total{service="consumer"}` rate is proportional to relay rate
- [ ] OTel Collector logs show no export errors
- [ ] `trace_context_restore_failures_total` = 0
- [ ] The `root span orphans` panel on the Distributed Tracing dashboard shows 0

---

## Postmortem Questions

1. What was the root cause? Which step in the five-step propagation chain broke?
2. Was trace propagation working before a recent deployment? If so, which commit introduced the regression?
3. Were there active consumer failures during the trace propagation outage? If so, how much did the broken traces impede the investigation of those failures?
4. Is there a test that would have caught this regression before deployment? A unit test that asserts `trace_context IS NOT NULL` for new outbox rows, or an integration test that verifies `traceparent` appears in AMQP headers, would catch Causes A and D at CI time.
5. The `trace_orphaned_root_spans_total` metric requires the OTel Collector to detect and count orphans. If the Collector is the cause of the outage, the metric itself may not fire. Is there a fallback detection mechanism?
6. Should the `trace_context` column have a NOT NULL constraint? This would cause outbox INSERTs to fail if trace capture is broken — a louder failure than silent orphan spans. Is failing fast on trace capture loss acceptable?
7. How many minutes of trace data were lost (not exported) due to the Collector outage? Is this acceptable per the observability SLA?
