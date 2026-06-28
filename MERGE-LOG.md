# Semantic Merge Log

This file documents how `contract-driven-messaging-platform-reliability-FIXED_1_.zip` (ZIP1) and `repo_final.zip` (ZIP2) were semantically merged.

---

## Merge Strategy

This was not a file-by-file overwrite. Each category of content was analysed and the best version selected or both retained if complementary.

---

## Source Code (ZIP1 wins — canonical)

ZIP1 contains the latest source code with all fixes applied (dated 2026-06-24). ZIP2 contains no source code. All `apps/`, `libs/`, `test/`, `observability/`, and config files come from ZIP1.

## README.md (ZIP2 wins — more complete)

ZIP1 README: 376 lines, basic architecture + quick start.  
ZIP2 README: 1319 lines, full narrative with ADR links, runbooks, postmortems, perf, interview topics.  
**Decision:** ZIP2 is a strict superset. Project Structure section updated to include new `docs/` files.

## docs/evolution.md (ZIP2 wins — new file)

ZIP1 had `docs/contract-evolution.md` (contract versioning lifecycle).  
ZIP2 has `docs/evolution.md` (10-phase system evolution narrative).  
**Decision:** Both retained — they are complementary, not overlapping.

## docs/adr/ (ZIP2 wins — new directory)

6 Architecture Decision Records not present in ZIP1. Kept as-is.

## docs/runbooks/ (ZIP2 wins — new directory)

5 operator runbooks not present in ZIP1. Kept as-is.

## docs/postmortems/ (ZIP2 wins — new directory)

5 blameless postmortems not present in ZIP1. Kept as-is.

## docs/architecture-internals.md (ZIP1 renamed)

ZIP1 `docs/architecture.md` is a code-level internal reference (EventRegistry, validateEvent internals, quote about noAck that was outdated).  
ZIP2 `docs/architecture-diagram.md` is the Mermaid source for the SVG hero diagram.  
**Decision:** ZIP1 file renamed to `architecture-internals.md` to avoid conflict. Both kept.

## docs/observability.md (ZIP1 wins — code-level)

ZIP1 has a detailed `observability.md` covering correlation ID generation, AMQP propagation, metric names, OpenTelemetry wiring — code-level detail not in ZIP2.  
ZIP2 covers observability in the README.  
**Decision:** ZIP1 file retained as `docs/observability.md`.

## docs/testing.md (ZIP1 wins — code-level)

ZIP1 has a detailed `docs/testing.md` (four layers, command reference, what each layer tests).  
ZIP2 covers testing in the README at a higher level.  
**Decision:** ZIP1 file retained as `docs/testing.md`.

## docs/production-readiness-fixes.md (ZIP1 wins — unique)

Not present in ZIP2. Retained.

## docs/architectural-gap-closure.md (ZIP1 wins — unique)

Not present in ZIP2. Retained.

## perf/ (ZIP2 wins — new directory)

5 k6 scenarios, config, lib helpers, 5 result reports, capacity model, bottleneck methodology.  
Not present in ZIP1. Kept as-is.

## observability/ infra configs (ZIP1 wins — canonical)

Prometheus rules, OTel collector, Grafana provisioning YAML all come from ZIP1 source.  
ZIP2 has `docs/grafana-dashboards.json` (importable JSON) — kept alongside.

---

*Generated during Semantic Merge — June 2026.*
