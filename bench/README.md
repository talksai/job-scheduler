# Load test

k6 (via Docker, no local install) creates one-shot jobs at a constant arrival rate,
each firing 5–30 s later — so creation AND firing both run at ~RATE/s sustained.
The stack must be up (`deploy/docker-compose.yml`, overlay optional).

```bash
RATE=100 DURATION=3m ./bench/run-load.sh
```

The script waits for every job to fire, then reconciles from Postgres: exact
accuracy percentiles over *every* fire (`claimed_at` vs the precise `fire_at`),
duplicate count, unpublished-outbox count, DLQ count.

## Last run (2026-07-03) — RATE=100, 3 min, two-instance stack

| Measure | Result |
|---|---|
| Jobs created / fired | **18 001 / 18 001** — zero duplicates, zero unpublished, zero DLQ |
| Sustained rate | **100 fires/s** (≈ **8.6 M jobs/day** equivalent) |
| Scheduling accuracy (all 18 k fires) | p50 = 50 ms · p95 = 95 ms · **p99 = 100 ms** (SLO ≤ 500 ms) |
| End-to-end execution lag (fired → effect applied) | p99 = 32 ms |
| Create API latency (k6) | p95 = 5.3 ms, 0 failed requests |
