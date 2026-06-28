# Performance Benchmark Results

This directory contains benchmark report templates for all load test scenarios defined in `perf/k6/scenarios/`. Each report is populated after a test execution run. Reports in this directory are never auto-generated — they require a human engineer to run the scenario, collect measurements, and fill in the placeholders.

## Reports

| Report | Scenario | Status | Last run |
|---|---|---|---|
| [baseline-throughput.md](baseline-throughput.md) | `01-baseline-throughput.js` | <!-- PENDING --> | <!-- date --> |
| [sustained-load.md](sustained-load.md) | `02-ramp-to-peak.js` | <!-- PENDING --> | <!-- date --> |
| [peak-load.md](peak-load.md) | `03-outbox-backlog.js` | <!-- PENDING --> | <!-- date --> |
| [retry-storm.md](retry-storm.md) | `04-retry-amplification.js` | <!-- PENDING --> | <!-- date --> |
| [relay-horizontal-scaling.md](relay-horizontal-scaling.md) | `05-relay-scalability.js` | <!-- PENDING --> | <!-- date --> |

## How to Complete a Report

### Step 1 — Prepare the environment

```bash
# Clean state before every run
docker compose down -v
docker compose up -d
./scripts/wait-for-ready.sh

# Confirm baseline health
curl -s http://localhost:3000/health | jq .
psql $DATABASE_URL -c "SELECT COUNT(*) FROM gateway_outbox_events WHERE status='pending';"
# Expected: 0
```

### Step 2 — Capture environment metadata

```bash
# Machine
uname -a
docker --version
docker compose version

# Service versions inside containers
docker compose exec gateway-service node --version
docker compose exec postgres psql --version
docker compose exec rabbitmq rabbitmq-diagnostics server_version

# k6
k6 version
```

### Step 3 — Run the scenario

```bash
k6 run perf/k6/scenarios/<scenario>.js \
  --env BASE_URL=http://localhost:3000 \
  --env PROMETHEUS_URL=http://localhost:9090 \
  --summary-export perf/results/exports/<scenario>-$(date +%Y%m%d-%H%M).json \
  2>&1 | tee perf/results/exports/<scenario>-$(date +%Y%m%d-%H%M).txt
```

### Step 4 — Capture Grafana screenshots

Open the dashboards listed in each report's **Grafana Screenshots** section.
Set the time range to the exact test window (from `setup()` to `teardown()`).
Save screenshots to `docs/screenshots/` with the filename specified in each placeholder.

### Step 5 — Query database state

Run the SQL queries listed in each report immediately after test completion, before the database is modified by subsequent activity.

### Step 6 — Fill in the report

Replace every `<!-- MEASURED -->`, `<!-- FILL AFTER EXECUTION -->`, and `<!-- PASTE k6 OUTPUT HERE -->` placeholder with actual values. Do not fabricate numbers. If a measurement was not taken, write `NOT COLLECTED` and add it to the Open Issues section.

### Step 7 — Commit

```bash
git add perf/results/ docs/screenshots/
git commit -m "perf: add <scenario> benchmark results <date>"
```

## Placeholder Convention

| Placeholder | Meaning |
|---|---|
| `<!-- MEASURED -->` | Requires a numeric measurement from the test run |
| `<!-- FILL AFTER EXECUTION -->` | Requires analysis text written after reviewing the data |
| `<!-- PASTE k6 OUTPUT HERE -->` | Paste the full k6 terminal summary verbatim |
| `<!-- SCREENSHOT PLACEHOLDER -->` | Replace the comment block with an actual image reference |
| `<!-- PENDING_EXECUTION -->` | Replace the status field with PASS, FAIL, or PARTIAL |

## Export Directory

Raw k6 JSON exports and terminal logs are stored in `perf/results/exports/` (git-ignored).

```
perf/results/exports/
├── 01-baseline-throughput-20240118-1430.json
├── 01-baseline-throughput-20240118-1430.txt
└── ...
```

Add `perf/results/exports/` to `.gitignore`. The markdown report files in `perf/results/` are committed; the raw JSON exports are not.
