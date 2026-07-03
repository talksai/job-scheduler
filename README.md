# job-scheduler

A horizontally-scalable **distributed job scheduler built from scratch** in Java 21 — cron / fixed-rate / one-shot scheduling with **exactly-once execution effect**, a leader-elected coordinator with **fencing tokens**, chaos tests, and SLO observability. Single-region by design.

**Project docs:** [README](projects/02-job-scheduler/README.md) → [ARCHITECTURE](projects/02-job-scheduler/ARCHITECTURE.md) → [BUILD-PLAN](projects/02-job-scheduler/BUILD-PLAN.md) → [STATUS](projects/02-job-scheduler/STATUS.md) (progress source of truth).

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

## Tests

```bash
mvn test          # includes the M0 Testcontainers test (needs a running Docker daemon)
```

> With OrbStack (or any daemon not on `/var/run/docker.sock`): `DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock mvn test`.
> The Docker API version for Testcontainers is pinned to 1.44 in `scheduler-api/pom.xml` (Docker Engine ≥ 29 rejects the docker-java default of 1.32).
