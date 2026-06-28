# Runbook: RabbitMQ Outage

| Field | Value |
|---|---|
| **ID** | RB-002 |
| **Alert** | `RabbitMQWorkQueueDepth` · `OutboxRelayLagging` · `MessagingServiceDown` |
| **Severity** | Critical |
| **SLO Impact** | Immediate — no events are being delivered. Gateway continues accepting HTTP requests (202 Accepted) but all downstream processing is halted. |
| **On-call Action** | Immediate. Every minute of broker downtime accumulates outbox rows that must be drained on recovery. |
| **Last Updated** | 2024-01-18 |

---

## Symptoms

The broker is unavailable. Depending on the failure mode, you may see one or more of:

- Gateway service logs: `Error: connect ECONNREFUSED 127.0.0.1:5672` or `heartbeat timeout`
- Relay logs: `getChannel() failed — broker unreachable` on every poll tick
- `outbox_pending_events{source="gateway"}` rising steadily (rows accumulating, none being drained)
- `outbox_published_total` rate drops to 0
- `messages_processed_total` rate drops to 0 (consumer receives nothing)
- HTTP responses from the gateway are still 202 (this is correct — the outbox is durable)
- Downstream systems report missing or delayed data

**What is NOT happening:**
- Data is not being lost. The outbox pattern ensures that events committed to `gateway_outbox_events` survive the broker outage intact.
- The gateway HTTP layer is not affected. Clients can still submit requests.

---

## Detection Signals

### Primary alerts

```yaml
# Outbox accumulating without draining
alert: OutboxRelayLagging
expr: outbox_pending_events{source="gateway"} > 200
for: 2m

# Queue depth elevated (if broker is partially available)
alert: RabbitMQWorkQueueDepth
expr: rabbitmq_queue_messages{queue="messaging.work"} > 100
for: 2m
```

### RabbitMQ-native signals

```bash
# Check broker health directly
curl -s -u guest:guest http://localhost:15672/api/healthchecks/node | jq .

# Check cluster status
curl -s -u guest:guest http://localhost:15672/api/nodes | jq '.[].running'

# Check if connections are being accepted
curl -s -u guest:guest http://localhost:15672/api/connections | jq 'length'
```

### Supporting Prometheus queries

```promql
# Outbox drain rate (should be > 0 during normal operation)
rate(outbox_published_total{source="gateway"}[1m])

# Time since last successful publish (approximate)
time() - max(outbox_last_published_timestamp)

# Is the broker scrape target still reachable?
up{job="rabbitmq"}
```

---

## Metrics

| Metric | Normal | Broker Down |
|---|---|---|
| `outbox_published_total` rate | matches msg/s | 0 |
| `outbox_pending_events{source="gateway"}` | < 20 | rising continuously |
| `rabbitmq_queue_messages{queue="messaging.work"}` | < 30 | 0 (broker unreachable) or frozen |
| `messages_processed_total` rate | matches msg/s | 0 |
| `up{job="rabbitmq"}` | 1 | 0 |
| Relay error log rate | 0 | 1/poll-interval (every 5s) |

---

## Grafana Panels

**Dashboard:** `cdmp-outbox-health` (UID: `cdmp-outbox-health`)

1. **Stat: Pending (gateway)** — will be climbing rapidly
2. **Time series: Published events** — will show a cliff edge at the time of outage
3. **Time series: Relay throughput** — drops to 0

**Dashboard:** `cdmp-system-overview` (UID: `cdmp-system-overview`)

4. **Time series: Throughput** — publish and consume rates both drop to 0
5. **Stat: Queue depth** — metric may become unavailable if broker scrape fails

**External:** RabbitMQ management UI at `http://localhost:15672`
- Node overview: check memory, disk, and `net_ticktime` alarms
- Connections: should show 0 if fully down, or may show stale connections being terminated

---

## Root Cause Analysis

### Cause A: Process crash

The `rabbitmq` process exited. No connections are being accepted.

**Signals:** `up{job="rabbitmq"} = 0`. No connections in management UI. RabbitMQ process not in process list.

### Cause B: Memory or disk alarm

RabbitMQ has hit a memory or disk watermark and has blocked all producers (flow control). Consumers may still be active but the broker is refusing new publishes.

**Signals:** Management UI shows a red alarm banner ("memory alarm" or "disk alarm"). `outbox_pending_events` rises. `messages_processed_total` may continue briefly as the consumer drains the existing work queue before it too empties.

### Cause C: Network partition (broker reachable from management but not from app)

The broker process is running but the application cannot reach it on port 5672. The management UI (port 15672) may still be reachable.

**Signals:** `curl http://localhost:15672/api/healthchecks/node` succeeds but `nc -zv rabbitmq-host 5672` fails or times out. Firewall rule change or network reconfiguration is the likely cause.

### Cause D: Authentication or TLS failure

The broker is running and accepting connections but rejecting the relay's credentials. Possible after a credential rotation or TLS certificate expiry.

**Signals:** Relay logs show `ACCESS-REFUSED` or `530 NOT_ALLOWED` in the AMQP connection error. Management UI is accessible with admin credentials.

### Cause E: Quorum queue unavailability

If `messaging.work` has been migrated to a quorum queue and the quorum is lost (fewer than `(n/2)+1` nodes available), the queue is unavailable even if the broker node is running.

**Signals:** Management UI shows queue in `down` state. Individual nodes appear healthy. Check quorum queue status: `rabbitmq-diagnostics check_if_node_is_quorum_critical`.

---

## Investigation Steps

### Step 1 — Establish broker status (2 minutes)

```bash
# Is the process running?
pgrep -a beam.smp | grep rabbitmq
# Or via Docker:
docker inspect rabbitmq --format='{{.State.Status}}'

# Is port 5672 accepting connections?
nc -zv localhost 5672 && echo "PORT OPEN" || echo "PORT CLOSED"

# Is the management UI responsive?
curl -s -o /dev/null -w "%{http_code}" http://localhost:15672/api/overview -u guest:guest

# Check RabbitMQ node health
curl -s -u guest:guest http://localhost:15672/api/healthchecks/node | jq .
```

### Step 2 — Check alarms (1 minute)

```bash
# Memory and disk alarms
curl -s -u guest:guest http://localhost:15672/api/nodes \
  | jq '.[] | {name, mem_alarm, disk_free_alarm, running}'

# Check actual memory and disk usage
curl -s -u guest:guest http://localhost:15672/api/nodes \
  | jq '.[] | {
      mem_used_mb: (.mem_used / 1048576 | floor),
      mem_limit_mb: (.mem_limit / 1048576 | floor),
      disk_free_gb: (.disk_free / 1073741824 | floor),
      disk_limit_gb: (.disk_free_limit / 1073741824 | floor)
    }'
```

### Step 3 — Check relay logs (1 minute)

```bash
# Find the first relay error and its specific error code
grep -E '"level":"error"|ECONNREFUSED|ACCESS-REFUSED|heartbeat|NOT_ALLOWED' \
  /var/log/gateway-service/*.log \
  | tail -20 \
  | jq '{time: .time, msg: .msg, err: .err}'

# Confirm outbox is accumulating
grep '"operation":"relay.poll"' /var/log/gateway-service/*.log \
  | tail -5 \
  | jq '{time: .time, pendingCount: .pendingCount, status: .status}'
```

### Step 4 — Estimate impact (1 minute)

```sql
-- How many rows are waiting to be published?
SELECT COUNT(*) AS pending_rows,
       MIN(created_at) AS oldest_pending,
       NOW() - MIN(created_at) AS max_lag
FROM gateway_outbox_events
WHERE status = 'pending';

-- At normal relay throughput (~250/s), how long will drain take?
-- pending_rows / 250 = seconds to drain
```

---

## Recovery Procedure

### Recovery A: Process crash — restart broker

```bash
# Docker Compose
docker compose restart rabbitmq

# Wait for broker to be ready (management API responsive)
until curl -s -u guest:guest http://localhost:15672/api/healthchecks/node \
  | jq -e '.status == "ok"' > /dev/null 2>&1; do
  echo "Waiting for RabbitMQ..."
  sleep 2
done
echo "RabbitMQ ready"

# Verify topology was re-asserted by the services on reconnect
# The relay services assert topology on every connection — check logs
grep '"operation":"topology.assert"' /var/log/gateway-service/*.log | tail -3
```

**After broker restart**, the relay's lazy-connect pattern will attempt reconnection on the next poll tick (within 5s). No manual relay restart required for the gateway relay.

**Known gap:** `DlqConsumerService` does not auto-reconnect after broker restart. It must be restarted:

```bash
docker compose restart messaging-service
# Or in Kubernetes:
kubectl rollout restart deployment/messaging-service
```

### Recovery B: Memory alarm — free memory

```bash
# Option 1: Increase memory watermark temporarily (buys time to investigate)
rabbitmqctl set_vm_memory_high_watermark 0.8

# Option 2: Force GC on the broker node
rabbitmqctl eval 'erlang:garbage_collect().'

# Option 3: If a queue has a large backlog causing memory pressure, inspect it
curl -s -u guest:guest http://localhost:15672/api/queues \
  | jq 'sort_by(-.messages) | .[:5] | .[] | {name, messages, memory}'
```

### Recovery C: Disk alarm — free disk space

```bash
# Check disk usage
df -h /var/lib/rabbitmq

# Option 1: Purge the DLQ if it has accumulated large payloads
rabbitmqctl purge_queue messaging.dlq
# WARNING: this permanently discards DLQ messages. Export them first if needed.
# curl -u guest:guest http://localhost:15672/api/queues/%2F/messaging.dlq/get ...

# Option 2: Increase disk watermark temporarily
rabbitmqctl set_disk_free_limit 500MB

# Option 3: Clean up old log files
find /var/log/rabbitmq -name "*.log" -mtime +7 -delete
```

### Recovery D: Authentication failure — rotate credentials

```bash
# Verify the credentials the relay is using
grep RABBITMQ_URL /etc/gateway-service/.env

# Test credentials directly
curl -s -u <username>:<password> http://localhost:15672/api/whoami | jq .

# If credentials are wrong, update and restart
# 1. Update the credential in the secret store
# 2. Restart the service to pick up the new credential
docker compose up -d --force-recreate gateway-service messaging-service
```

### Post-recovery: Monitor drain

After the broker recovers, the outbox relay will begin draining. Monitor the drain progress:

```bash
# Watch pending count decrease in real time
watch -n 5 'psql $DATABASE_URL -t -c "
SELECT COUNT(*) AS pending
FROM gateway_outbox_events
WHERE status = '"'"'pending'"'"'"'

# Or via Prometheus (query every 10s)
# outbox_pending_events{source="gateway"}
```

Expected drain rate: approximately equal to pre-outage throughput (relay is not throttled on recovery). At 250 msg/s, 10,000 pending rows drain in ~40 seconds.

---

## Validation Checklist

- [ ] `up{job="rabbitmq"}` = 1
- [ ] RabbitMQ management UI accessible and shows node as running
- [ ] No memory or disk alarms active
- [ ] `outbox_published_total` rate is positive and rising
- [ ] `outbox_pending_events{source="gateway"}` is decreasing toward normal baseline (< 20)
- [ ] `messages_processed_total` rate has recovered to pre-outage level
- [ ] `messaging.work` queue has consumers connected
- [ ] `messaging.dlq` queue depth — check if any messages arrived in the DLQ during the outage (retry budget exhaustion during extended outage)
- [ ] Consumer logs show successful processing (no sustained error patterns)
- [ ] `DlqConsumerService` is connected (check messaging-service logs for `dlq consumer started`)
- [ ] `outbox_pending_events` returns to baseline within 5 minutes of broker recovery

---

## Postmortem Questions

1. What caused the broker to become unavailable? Was it a process crash, resource exhaustion, or network partition?
2. How long was the broker down? How many messages accumulated in the outbox during the outage?
3. Did any messages exhaust their retry budget and land in the DLQ during the outage? If so, what was the retry window relative to the outage duration?
4. Was the `DlqConsumerService` auto-reconnection gap known before this incident? What is the remediation plan (Kubernetes liveness probe, supervisor process, explicit reconnect loop)?
5. Was the outage visible to end users? The gateway returned 202 throughout — did downstream systems surface the data delay?
6. Was the broker running as a single node? If so, is a quorum queue or mirrored queue configuration needed for the availability requirement?
7. Was the memory or disk watermark alarm the root cause? If so, what is the capacity plan to prevent recurrence?
8. How long did it take to identify the root cause from the alert to a specific cause (A through E)? Were the investigation steps in this runbook sufficient?
