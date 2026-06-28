# Load Testing & Capacity Planning

This directory contains the complete load-testing and capacity-planning strategy for `contract-driven-messaging-platform`. All k6 scripts, configuration files, and analysis tools are production-ready.

## Directory Structure

```
perf/
├── README.md                          ← this file
├── k6/
│   ├── scenarios/
│   │   ├── 01-baseline-throughput.js  ← steady-state throughput and latency
│   │   ├── 02-ramp-to-peak.js         ← traffic ramp + bottleneck identification
│   │   ├── 03-outbox-backlog.js        ← outbox accumulation under relay constraint
│   │   ├── 04-retry-amplification.js   ← consumer failure → retry storm
│   │   └── 05-relay-scalability.js     ← horizontal relay scaling
│   ├── lib/
│   │   ├── checks.js                   ← shared threshold checks
│   │   ├── metrics.js                  ← custom k6 metrics
│   │   └── payloads.js                 ← event payload generators
│   └── config/
│       ├── thresholds.json             ← success criteria per scenario
│       └── environments.json           ← target URLs per environment
├── analysis/
│   ├── bottleneck-methodology.md       ← systematic bottleneck identification
│   └── capacity-model.md              ← scaling assumptions and projections
└── dashboards/
    └── k6-performance-dashboard.json  ← Grafana dashboard for load test results
```

## Quick Start

```bash
# Install k6
brew install k6  # macOS
# or: https://k6.io/docs/get-started/installation/

# Start the system under test
docker compose up -d

# Wait for services to be ready
./scripts/wait-for-ready.sh

# Run baseline scenario (10 minutes)
k6 run k6/scenarios/01-baseline-throughput.js \
  --env BASE_URL=http://localhost:3000 \
  --env PROMETHEUS_URL=http://localhost:9090

# Run full suite
for scenario in k6/scenarios/*.js; do
  k6 run "$scenario" --env BASE_URL=http://localhost:3000
  sleep 60  # cool-down between scenarios
done
```
