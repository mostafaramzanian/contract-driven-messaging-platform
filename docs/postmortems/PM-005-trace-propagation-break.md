# PM-005 — Trace Propagation Break — OTel Collector OOM

| Field | Value |
|---|---|
| **Incident ID** | PM-005 |
| **Severity** | SEV-3 |
| **Status** | Resolved |
| **Incident start** | 2024-04-19 08:00 UTC (estimated — see Detection section) |
| **Incident end** | 2024-04-19 12:12 UTC |
| **Total duration** | 4 hours 12 minutes |
| **Incident commander** | On-call engineer (rotation: backend team) |
| **Postmortem author** | On-call engineer |
| **Review date** | 2024-04-21 |
| **Related runbook** | [RB-005 — Trace Propagation Failure](../runbooks/RB-005-trace-propagation-failure.md) |

---

## Summary

On 2024-04-19, the OpenTelemetry Collector was OOM-killed by the Linux kernel at approximately 08:00 UTC. The collector had been running without a memory limit in Docker Compose. Over several days, the collector's in-memory span buffer had grown as span export to Jaeger intermittently failed due to network flakiness between the collector and Jaeger's OTLP endpoint. Spans accumulated in the collector's retry buffer, eventually exhausting available memory.

The collector's crash was not alerted — `up{job="otel-collector"}` was not being scraped by Prometheus. The incident was discovered at 12:00 UTC by an engineer debugging an unrelated consumer issue, who noticed that Jaeger showed no traces for the previous 4 hours. Investigation confirmed the collector had been down since approximately 08:00 UTC.

Business functionality was unaffected throughout. No events were lost, no DLQ entries were created, and gateway latency was normal. The incident was SEV-3 because it degraded the engineering team's ability to debug issues, not because it affected end users.

---

## Impact

| Area | Impact |
|---|---|
| Event delivery | Unaffected |
| DLQ events | 0 |
| Data loss | 0 |
| Jaeger traces | 4 hours 12 minutes of traces not exported — permanently lost from the trace backend |
| Prometheus metrics | Unaffected — metrics scraping is independent of the OTel Collector |
| Structured logs | Unaffected — Pino logging is independent of the OTel Collector |
| On-call debugging capability | Significantly degraded for 4 hours — no trace data available for the outage window |
| `trace_orphaned_root_spans_total` | Not measurable — the metric requires the collector to be running |

---

## Timeline

All times UTC.

| Time | Event |
|---|---|
| **2024-04-15 (approx)** | Jaeger OTLP endpoint begins experiencing intermittent connection resets, likely due to a network configuration change in the Docker bridge network. OTel Collector begins accumulating spans in its retry buffer. |
| **2024-04-15–18** | Collector's retry buffer grows daily. No alert — collector memory usage is not monitored. |
| **2024-04-19 ~08:00** | Linux OOM killer terminates the `otel-collector` process. All spans in the retry buffer are lost. No alert fires — `up{job="otel-collector"}` is not scraped. |
| **2024-04-19 08:00–12:00** | Application services continue running normally. Spans are exported to the collector's endpoint, which is now refusing connections. The OTel SDK's export failure is handled gracefully — export errors are logged at DEBUG level and do not affect application code paths. |
| **2024-04-19 08:00–12:00** | No alert fires. No page. Incident is undetected for 4 hours. |
| **2024-04-19 12:00** | Engineer investigating a consumer behavior question opens Jaeger. Observes: last trace timestamp is 07:58 UTC. Queries for recent traces — none returned. |
| **2024-04-19 12:02** | Engineer checks OTel Collector: process not running. Docker logs show OOM kill at 07:58 UTC. |
| **2024-04-19 12:03** | Engineer restarts the collector: `docker compose restart otel-collector`. |
| **2024-04-19 12:04** | Collector starts. Prometheus begins scraping `up{job="otel-collector"}` — wait, this metric is not configured. Engineer adds the scrape target manually during the investigation. |
| **2024-04-19 12:04** | Jaeger begins receiving spans. New traces appear in Jaeger within 15 seconds of collector restart. |
| **2024-04-19 12:06** | Engineer checks the collector config. Identifies missing `memory_limiter` processor and no Docker memory limit set. |
| **2024-04-19 12:08** | Engineer adds `memory_limiter` to the collector pipeline config and sets Docker memory limit to 256MB. Restarts collector with new config. |
| **2024-04-19 12:10** | Collector running with memory constraints. Traces flowing to Jaeger. |
| **2024-04-19 12:12** | Engineer declares incident resolved. Raises postmortem. |

---

## Detection

**How the incident was detected:** Discovery by an engineer opening Jaeger for an unrelated reason. There was no alert. The incident had been ongoing for 4 hours before anyone noticed.

**Why no alert fired:**
1. `up{job="otel-collector"}` was not in the Prometheus scrape config. The collector was not a monitored target.
2. The `TraceOrphanSpansElevated` alert (`trace_orphaned_root_spans_total > 5`) requires the collector to be running to emit the metric. When the collector is down, the metric is absent — the alert cannot fire.
3. There is no Prometheus alert for "metric absent for N minutes" (`absent()` function). This class of alert was never configured.
4. Application services log OTel export errors at DEBUG level (not WARN or ERROR) — they did not produce visible signals in the structured log stream.

**Detection gap:** 4 hours 12 minutes from incident onset to discovery. This is the longest detection gap in any postmortem to date and the most significant finding from this incident.

---

## Metrics

### Metrics that changed during the incident

| Metric | Pre-incident | During (08:00–12:04) | Post-recovery |
|---|---|---|---|
| `up{job="otel-collector"}` | Not scraped | Not scraped | 1 (added during investigation) |
| Jaeger trace count | ~200/min | 0 | ~200/min |
| `trace_orphaned_root_spans_total` | 0 | Not emitted (collector down) | 0 |
| Application metrics (Prometheus) | Normal | Normal | Normal |
| Application logs (Pino) | Normal | Normal | Normal |

### Grafana dashboards that showed anomalies

1. **Distributed Tracing** (`cdmp-distributed-tracing`): all trace volume panels showed 0 or no data from 08:00 UTC. The panel silently showed "No data" without triggering an alert.

**What was unaffected:**
- System Overview dashboard — normal throughout
- Reliability dashboard — normal throughout
- Outbox Health dashboard — normal throughout

The incident was entirely invisible in Prometheus-based dashboards because metrics collection was independent of the OTel Collector.

---

## Root Cause Analysis

### Proximate cause

The OTel Collector process was OOM-killed after its in-memory retry buffer grew beyond the available memory. The Docker Compose service definition had no `mem_limit` set for the collector, allowing it to consume all available host memory. The Erlang-based collector's retry buffer accumulated spans over 4 days as export to Jaeger intermittently failed.

### Why Jaeger export was failing intermittently

Post-incident investigation found that the Docker bridge network MTU was set to 1500 bytes, which is standard for physical Ethernet but causes fragmentation issues in Docker-within-Docker configurations. The OTLP/gRPC export protocol uses large frames for batched span export. When frames exceeded the effective MTU of the Docker bridge, packets were fragmented and some were dropped by the container network stack. This caused intermittent gRPC stream resets, which the collector's retry logic handled by buffering spans and retrying — correctly from the collector's perspective, but this allowed the retry buffer to grow unboundedly.

### Why the collector had no memory limit

The Docker Compose service definition for `otel-collector` was copied from an example configuration that did not include resource limits. No resource limits review was done when the collector was added to the stack. The gap between "configured" and "configured with resource limits" was not caught in code review.

### Why the collector was not a Prometheus scrape target

The collector exposes its own Prometheus metrics on port 8888. This was not added to the Prometheus scrape config when the collector was deployed. There is no enforcement mechanism that requires new services to register scrape targets — it is a manual step that was missed.

### Why the `absent()` alert was not used

The `absent()` function in Prometheus returns a 1 when a metric is absent, allowing alerts like `absent(up{job="otel-collector"}) == 1`. This is the standard pattern for detecting missing scrape targets. It was not applied because the team was not aware the scrape target was missing — you cannot alert on the absence of a target you do not know should be present.

### Why spans were exported at DEBUG level

The OTel SDK's export error logging level is configurable. The default in the SDK version in use was DEBUG. This is arguably correct — a transient export failure should not fill production logs with error-level noise. However, sustained export failure (collector down for hours) should escalate to at least WARN. The SDK does not distinguish between "one failed export" and "no successful export in 4 hours."

---

## Immediate Mitigation

1. `docker compose restart otel-collector` — collector restarted at 12:04 UTC
2. Added `memory_limiter` processor to collector pipeline config
3. Added `mem_limit: 256m` to Docker Compose service definition
4. Added `otel-collector` to Prometheus scrape config
5. Restarted Prometheus to pick up the new scrape target

---

## Permanent Corrective Actions

### Action 1: Add OTel Collector to Prometheus scrape config and add alert

**Owner:** Platform team  
**Target:** 2024-04-23

```yaml
# prometheus/prometheus.yml
scrape_configs:
  - job_name: 'otel-collector'
    static_configs:
      - targets: ['otel-collector:8888']
```

```yaml
# prometheus/alerts.yml
- alert: OtelCollectorDown
  expr: absent(up{job="otel-collector"}) == 1
  for: 2m
  severity: critical
  annotations:
    summary: "OTel Collector not reachable — trace pipeline is down"

- alert: OtelCollectorUnhealthy
  expr: up{job="otel-collector"} == 0
  for: 1m
  severity: critical
  annotations:
    summary: "OTel Collector scrape failing — check collector process"
```

### Action 2: Add memory_limiter to collector pipeline and Docker memory limit

**Owner:** Infrastructure  
**Target:** 2024-04-23

```yaml
# otel-collector-config.yml
processors:
  memory_limiter:
    check_interval: 5s
    limit_mib: 200       # Drop spans if memory exceeds 200MB
    spike_limit_mib: 50  # Allow brief spikes to 250MB

service:
  pipelines:
    traces:
      processors: [memory_limiter, batch]

# docker-compose.yml
otel-collector:
  mem_limit: 256m
  mem_reservation: 128m
```

The `memory_limiter` will drop spans rather than buffer them indefinitely when memory is under pressure. Dropped spans are logged and counted in `otelcol_processor_dropped_spans`. Dropping spans is preferable to OOM-killing the process.

### Action 3: Fix Docker bridge network MTU

**Owner:** Infrastructure  
**Target:** 2024-04-26  
**Description:** Set the Docker bridge network MTU to 1450 bytes to avoid fragmentation in Docker-within-Docker environments. Alternatively, configure gRPC keepalive settings in the OTLP exporter to detect and reset stale streams more quickly.

```yaml
# docker-compose.yml
networks:
  default:
    driver: bridge
    driver_opts:
      com.docker.network.driver.mtu: 1450
```

### Action 4: Add OTel export failure escalation logging

**Owner:** Backend team  
**Target:** 2024-04-30  
**Description:** Add a custom OTel SDK diagnostic logger that tracks consecutive export failures. After 5 consecutive failures, log at WARN level. After 60 seconds of sustained export failure, log at ERROR level and emit a `otel_export_failure_sustained` metric.

### Action 5: Add `absent()` alerts for all critical services

**Owner:** Platform team  
**Target:** 2024-04-26  
**Description:** Audit all services that should appear in Prometheus. For each, add an `absent()` alert. Current gaps: `otel-collector`, `jaeger`, `grafana` scrape health.

### Action 6: Add resource limits to all Docker Compose services

**Owner:** Infrastructure  
**Target:** 2024-04-30  
**Description:** A service without a `mem_limit` can consume all host memory and affect all other services. Set explicit `mem_limit` and `mem_reservation` for every service in `docker-compose.yml`. Add a CI check that fails if any service definition is missing `mem_limit`.

---

## Lessons Learned

**A metric that doesn't exist cannot alert.** The OTel Collector was a production dependency — it was deployed, it was logging data, and it had a failure mode that caused a 4-hour detection gap. But it was not a Prometheus scrape target, so `up{job="otel-collector"}` never existed, and no alert could fire. Every infrastructure component that the system depends on for correctness or observability must be scraped and monitored. This is a process gap, not a technology gap.

**The `absent()` function is the correct tool for detecting missing targets.** If a target should be up and its `up` metric is absent (not 0, but absent — the series doesn't exist), `absent()` fires. This is distinct from `up == 0`, which fires when the target exists but is unhealthy. Both patterns are needed. Only `up == 0` was being used.

**SEV-3 incidents have real costs.** This incident caused no user-visible impact and no data loss. But for 4 hours and 12 minutes, every engineer who needed to debug anything in the system was working without trace data. If a SEV-1 had occurred during those 4 hours, the diagnosis time would have been significantly longer. Degraded observability is a risk multiplier for future incidents.

**Unbounded retry buffers are a memory leak by design.** The collector's retry logic is correct in intent — retry failed exports rather than drop them. But without a memory limit, "retry indefinitely" becomes "grow indefinitely." The `memory_limiter` processor exists precisely for this pattern and should be standard in every OTel Collector deployment.

**A 4-hour detection gap for a production observability system is not acceptable for a team that relies on tracing for debugging.** The detection mechanism for the OTel Collector was "an engineer happened to open Jaeger." This is not a detection mechanism. For any component whose absence degrades incident response capability, the alert must fire faster than the time it takes for the next incident to occur that would require that component.

---

## Follow-up Tasks

| # | Task | Owner | Priority | Target |
|---|---|---|---|---|
| 1 | Add `otel-collector` to Prometheus scrape config | Platform | P0 | 2024-04-23 |
| 2 | Add `OtelCollectorDown` alert using `absent()` | Platform | P0 | 2024-04-23 |
| 3 | Add `memory_limiter` processor to OTel Collector config | Infrastructure | P0 | 2024-04-23 |
| 4 | Set `mem_limit: 256m` in docker-compose.yml for otel-collector | Infrastructure | P0 | 2024-04-23 |
| 5 | Fix Docker bridge network MTU to 1450 | Infrastructure | P1 | 2024-04-26 |
| 6 | Audit all services for missing Prometheus scrape targets | Platform | P0 | 2024-04-26 |
| 7 | Add `absent()` alerts for all critical services | Platform | P1 | 2024-04-26 |
| 8 | Add resource limits (mem_limit) to all Docker Compose services | Infrastructure | P1 | 2024-04-30 |
| 9 | Add OTel export failure escalation logging (WARN after 5 failures) | Backend | P2 | 2024-04-30 |
| 10 | Add CI check: fail if any docker-compose service missing mem_limit | Infrastructure | P2 | 2024-04-30 |
| 11 | Document: "Every new service must be added to prometheus scrape config before merge" | Platform | P1 | 2024-04-26 |
