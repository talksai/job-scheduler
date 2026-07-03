#!/usr/bin/env bash
# Load test against the compose stack. Requires the stack up (base or +chaos overlay).
# Usage: RATE=100 DURATION=3m ./bench/run-load.sh
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
COMPOSE="docker compose -f $REPO_ROOT/deploy/docker-compose.yml"
RATE="${RATE:-100}"
DURATION="${DURATION:-3m}"
NETWORK="job-scheduler_default"

psql_query() {
  $COMPOSE exec -T postgres psql -U scheduler -d scheduler -tAc "$1"
}

echo ">>> clearing previous bench data"
psql_query "DELETE FROM execution WHERE job_id IN (SELECT id FROM job WHERE type='bench');
            DELETE FROM outbox WHERE aggregate_id IN (SELECT id::text FROM job WHERE type='bench');
            DELETE FROM job WHERE type='bench';" > /dev/null

echo ">>> k6: creating jobs at ${RATE}/s for ${DURATION} (fires spread 5-30s out)"
START_TS=$(date +%s)
docker run --rm -i --network "$NETWORK" \
  -e RATE="$RATE" -e DURATION="$DURATION" -e BASE_URL=http://app:8080 \
  grafana/k6 run --quiet - < "$HERE/load.js"

echo ">>> waiting for every bench job to fire and complete"
DEADLINE=$(( $(date +%s) + 120 ))
while :; do
  PENDING=$(psql_query "SELECT count(*) FROM job WHERE type='bench' AND state='PENDING'")
  [[ "$PENDING" == "0" ]] && break
  if (( $(date +%s) > DEADLINE )); then echo "TIMEOUT: $PENDING bench jobs still pending" >&2; exit 1; fi
  sleep 3
done
END_TS=$(date +%s)

echo ""
echo ">>> RESULTS"
CREATED=$(psql_query "SELECT count(*) FROM job WHERE type='bench'")
EXECS=$(psql_query "SELECT count(*) FROM execution e JOIN job j ON j.id=e.job_id WHERE j.type='bench'")
APPLIED=$(psql_query "SELECT count(*) FROM execution e JOIN job j ON j.id=e.job_id WHERE j.type='bench' AND e.status='COMPLETED'")
DUPS=$(psql_query "SELECT count(*) FROM (SELECT job_id, fire_epoch FROM execution GROUP BY 1,2 HAVING count(*)>1) d")
UNPUB=$(psql_query "SELECT count(*) FROM outbox WHERE published_at IS NULL")
WALL=$(( END_TS - START_TS ))
echo "jobs created:            $CREATED"
echo "executions (exactly 1x): $EXECS (duplicates: $DUPS, effects applied: $APPLIED, unpublished outbox: $UNPUB)"
echo "wall time incl. drain:   ${WALL}s -> sustained $(( CREATED / WALL )) fires/s ($(( CREATED * 86400 / WALL )) jobs/day equivalent)"

# scheduling accuracy, exact, over every bench fire (claimed_at vs the precise fireAt)
psql_query "SELECT 'accuracy ms  p50=' || round(1000*percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (e.claimed_at - j.fire_at))))
                 || '  p95=' || round(1000*percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (e.claimed_at - j.fire_at))))
                 || '  p99=' || round(1000*percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (e.claimed_at - j.fire_at))))
                 || '  max=' || round(1000*max(EXTRACT(EPOCH FROM (e.claimed_at - j.fire_at))))
            FROM execution e JOIN job j ON j.id = e.job_id WHERE j.type='bench'"

echo "execution lag p99 (fired->applied): $(curl -sf localhost:8080/actuator/prometheus | grep 'scheduler_execution_lag_seconds{' | grep '0.99' | awk '{print $NF}')s"
echo "DLQ events: $(curl -sf localhost:8080/actuator/prometheus | grep '^scheduler_events_dlq_total' | awk '{print $NF}')"
