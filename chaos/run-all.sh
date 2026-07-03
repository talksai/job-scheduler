#!/usr/bin/env bash
# Runs the full chaos suite against the two-instance compose stack.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/lib.sh"

echo ">>> bringing up the two-instance stack"
$COMPOSE up -d --build
stack_stable

# hygiene: cancel recurring jobs left over from an aborted previous run
psql_query "UPDATE job SET state='CANCELLED', next_fire_at=NULL WHERE type LIKE 'chaos-%' AND state='PENDING'" > /dev/null

"$HERE/01-worker-kill.sh"
"$HERE/02-coordinator-kill.sh"
"$HERE/03-pause-deposed-leader.sh"

echo ""
echo ">>> CHAOS SUITE PASSED — failover timers:"
echo "    worker-1: coordinator=$(metric_of 8080 scheduler_failover_coordinator_seconds_max)s worker=$(metric_of 8080 scheduler_failover_worker_seconds_max)s fenced-writes=$(metric_of 8080 scheduler_fencing_rejected_total)"
echo "    worker-2: coordinator=$(metric_of 8081 scheduler_failover_coordinator_seconds_max)s worker=$(metric_of 8081 scheduler_failover_worker_seconds_max)s fenced-writes=$(metric_of 8081 scheduler_fencing_rejected_total)"
