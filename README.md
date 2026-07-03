# job-scheduler

A horizontally-scalable **distributed job scheduler built from scratch** in Java 21 — cron / fixed-rate / one-shot scheduling with **exactly-once execution effect**, a leader-elected coordinator with **fencing tokens**, chaos tests, and SLO observability. Single-region by design.

**Measured** (two-instance stack, 2026-07-03): sustained **100 fires/s** (≈8.6 M jobs/day equivalent) · scheduling accuracy **p99 = 100 ms** · **0 duplicates** across 18 k loaded fires and SIGKILL chaos · coordinator failover **4–6 s**, worker failover **4–6 s**, both split-brain-safe (fencing proven by a paused-leader chaos run).

**Project docs:** [README](projects/02-job-scheduler/README.md) → [ARCHITECTURE](projects/02-job-scheduler/ARCHITECTURE.md) → [BUILD-PLAN](projects/02-job-scheduler/BUILD-PLAN.md) → [STATUS](projects/02-job-scheduler/STATUS.md) (progress source of truth).
**Interview prep:** [interview-prep/](interview-prep/README.md) — system walkthrough, trade-offs, deep-dive Q&A, run-and-observe lab.

## Stack

Java 21 · Spring Boot 3 (WebFlux + Reactor) · R2DBC Postgres · Kafka (KRaft) via reactor-kafka · Flyway · Micrometer → Prometheus → Grafana · Testcontainers.

## Modules

```
scheduler-api/           WebFlux REST (create/cancel/query) + bootable app
scheduler-core/          hierarchical timing wheel, claim/fire path, missed-fire policies
scheduler-execution/     reactor-kafka producer/consumer, retry topics + DLQ
scheduler-coordinator/   CoordinationService (Postgres-lease leader election in M3)
scheduler-store/         Flyway schema + R2DBC repositories
scheduler-observability/ Micrometer meters + SLO definitions (see SLO.md)
deploy/                  docker-compose: Postgres, Kafka, app, Prometheus, Grafana
chaos/  bench/           chaos suite + load tests (M5)
```

## Quickstart

```bash
# full local stack: Postgres + Kafka (KRaft) + app + Prometheus + Grafana
docker compose -f deploy/docker-compose.yml up --build

# ...or the two-instance HA stack (adds a second worker on :8081)
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.chaos.yml up --build

# create a one-shot job that fires in 10 s
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' -d '{
  "type": "email",
  "payload": "{\"to\":\"user@example.com\"}",
  "scheduleType": "ONE_SHOT",
  "fireAt": "'"$(date -u -v+10S +%Y-%m-%dT%H:%M:%SZ)"'"
}'

# a cron job (6-field, UTC): every minute
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' -d '{
  "type": "report", "scheduleType": "CRON", "cronExpression": "0 * * * * *"
}'

# query / cancel
curl -s localhost:8080/api/jobs/<id>
curl -s -X DELETE localhost:8080/api/jobs/<id>
```

Grafana: http://localhost:3000 (admin/admin) → "Job Scheduler — Overview". Prometheus: http://localhost:9090.

## Tests, chaos & load

```bash
mvn test                                  # 24 tests: unit + Testcontainers integration (needs Docker)
./chaos/run-all.sh                        # kill-worker, kill-coordinator, paused-leader — invariant-asserted
RATE=100 DURATION=3m ./bench/run-load.sh  # k6 load run with exactly-once reconciliation from Postgres
```

See [chaos/README.md](chaos/README.md) and [bench/README.md](bench/README.md) for scenarios and the latest measured numbers.

> With OrbStack (or any daemon not on `/var/run/docker.sock`): `DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock mvn test`.
> The Docker API version for Testcontainers is pinned to 1.44 in `scheduler-api/pom.xml` (Docker Engine ≥ 29 rejects the docker-java default of 1.32).
