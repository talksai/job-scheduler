#!/usr/bin/env bash
# Shared helpers for the chaos suite. Scripts exit non-zero on any violated invariant.
# The fleet: worker-1..worker-5 (services app, app2..app5; ports 8080..8084).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="docker compose -f $REPO_ROOT/deploy/docker-compose.yml -f $REPO_ROOT/deploy/docker-compose.chaos.yml"
TOTAL_SHARDS=16
ALL_WORKERS=(worker-1 worker-2 worker-3 worker-4 worker-5)

psql_query() {
  $COMPOSE exec -T postgres psql -U scheduler -d scheduler -tAc "$1"
}

leader_worker() { psql_query "SELECT holder FROM coordinator_lease WHERE role='coordinator'"; }
lease_epoch()   { psql_query "SELECT epoch  FROM coordinator_lease WHERE role='coordinator'"; }
shards_of()     { psql_query "SELECT count(*) FROM shard_assignment WHERE worker_id='$1' AND shard < $TOTAL_SHARDS"; }
exec_count()    { psql_query "SELECT count(*) FROM execution WHERE job_id='$1'"; }

service_of() {
  case "$1" in
    worker-1) echo app  ;;
    worker-2) echo app2 ;;
    worker-3) echo app3 ;;
    worker-4) echo app4 ;;
    worker-5) echo app5 ;;
    *) echo "unknown worker '$1'" >&2; return 1 ;;
  esac
}

port_of() {
  case "$1" in
    worker-1) echo 8080 ;;
    worker-2) echo 8081 ;;
    worker-3) echo 8082 ;;
    worker-4) echo 8083 ;;
    worker-5) echo 8084 ;;
  esac
}

# any worker that is not $1 (deterministic: first in fleet order)
other_worker() {
  local w
  for w in "${ALL_WORKERS[@]}"; do
    if [[ "$w" != "$1" ]]; then echo "$w"; return; fi
  done
}

health_ok() { curl -sf "localhost:$1/actuator/health" 2>/dev/null | grep -q '"UP"'; }

all_healthy() {
  local w
  for w in "${ALL_WORKERS[@]}"; do
    health_ok "$(port_of "$w")" || return 1
  done
}

# every worker owns shards, spread is even (max-min <= 1), all 16 assigned to live fleet
fair_split() {
  local owners mn mx
  owners=$(psql_query "SELECT count(DISTINCT worker_id) FROM shard_assignment WHERE shard < $TOTAL_SHARDS")
  [[ "$owners" == "${#ALL_WORKERS[@]}" ]] || return 1
  mn=$(psql_query "SELECT min(c) FROM (SELECT count(*) AS c FROM shard_assignment WHERE shard < $TOTAL_SHARDS GROUP BY worker_id) t")
  mx=$(psql_query "SELECT max(c) FROM (SELECT count(*) AS c FROM shard_assignment WHERE shard < $TOTAL_SHARDS GROUP BY worker_id) t")
  (( mx - mn <= 1 ))
}

print_split() {
  psql_query "SELECT worker_id || '=' || count(*) FROM shard_assignment WHERE shard < $TOTAL_SHARDS GROUP BY worker_id ORDER BY worker_id" | paste -sd' ' -
}

check_count()     { [[ "$(psql_query "$1")" == "$2" ]]; }
check_leader_is() { [[ "$(leader_worker)" == "$1" ]]; }
leader_changed()  { local now; now=$(leader_worker); [[ -n "$now" && "$now" != "$1" ]]; }

metric_of() { # port, metric-name-prefix
  curl -sf "localhost:$1/actuator/prometheus" 2>/dev/null | grep "^$2" | head -1 | awk '{print $NF}'
}

wait_until() { # timeout_seconds, description, command...
  local timeout=$1 desc=$2; shift 2
  local start; start=$(date +%s)
  until "$@" >/dev/null 2>&1; do
    if (( $(date +%s) - start > timeout )); then
      echo "  ✗ TIMEOUT after ${timeout}s: $desc" >&2
      return 1
    fi
    sleep 1
  done
  echo "  ✓ $desc"
}

assert_eq() { # actual, expected, message
  if [[ "$1" != "$2" ]]; then
    echo "  ✗ ASSERT FAILED: $3 (actual='$1' expected='$2')" >&2
    exit 1
  fi
  echo "  ✓ $3 ($1)"
}

create_one_shot() { # port, type, fire_in_seconds
  local fire_at
  fire_at=$(python3 -c "from datetime import datetime,timedelta,timezone; print((datetime.now(timezone.utc)+timedelta(seconds=$3)).isoformat(timespec='milliseconds').replace('+00:00','Z'))")
  curl -sf -X POST "localhost:$1/api/jobs" -H 'Content-Type: application/json' \
    -d "{\"type\":\"$2\",\"payload\":\"{}\",\"scheduleType\":\"ONE_SHOT\",\"fireAt\":\"$fire_at\"}" > /dev/null
}

create_ticker() { # port, type, rate_seconds -> prints job id
  curl -sf -X POST "localhost:$1/api/jobs" -H 'Content-Type: application/json' \
    -d "{\"type\":\"$2\",\"payload\":\"{}\",\"scheduleType\":\"FIXED_RATE\",\"fixedRateSeconds\":$3}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])"
}

cancel_job() { # port, id
  curl -sf -X DELETE "localhost:$1/api/jobs/$2" > /dev/null || true
}

stack_stable() {
  wait_until 240 "all ${#ALL_WORKERS[@]} instances healthy" all_healthy
  wait_until 90 "shards spread fairly across the fleet" fair_split
  echo "  fleet: $(print_split) · leader=$(leader_worker) epoch=$(lease_epoch)"
}

# The core exactly-once reconciliation for a set of jobs identified by type.
assert_exactly_once() { # type, expected_count
  local execs completed unpublished dups
  execs=$(psql_query "SELECT count(*) FROM execution e JOIN job j ON j.id = e.job_id WHERE j.type='$1'")
  completed=$(psql_query "SELECT count(*) FROM execution e JOIN job j ON j.id = e.job_id WHERE j.type='$1' AND e.status='COMPLETED'")
  unpublished=$(psql_query "SELECT count(*) FROM outbox o JOIN job j ON j.id::text = o.aggregate_id WHERE j.type='$1' AND o.published_at IS NULL")
  dups=$(psql_query "SELECT count(*) FROM (SELECT job_id, fire_epoch FROM execution GROUP BY 1,2 HAVING count(*) > 1) d")
  assert_eq "$execs" "$2" "$1: execution rows == expected occurrences"
  assert_eq "$completed" "$2" "$1: every effect applied (all COMPLETED)"
  assert_eq "$unpublished" "0" "$1: no ghost events stuck in the outbox"
  assert_eq "$dups" "0" "zero duplicate (job_id, fire_epoch) rows anywhere"
}
