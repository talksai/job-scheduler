#!/usr/bin/env bash
# Runs the full chaos suite against the five-worker compose fleet.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/lib.sh"

echo ">>> bringing up the five-worker fleet"
$COMPOSE up -d --build
stack_stable

# hygiene: cancel recurring jobs left over from an aborted previous run
psql_query "UPDATE job SET state='CANCELLED', next_fire_at=NULL WHERE type LIKE 'chaos-%' AND state='PENDING'" > /dev/null

"$HERE/01-worker-kill.sh"
"$HERE/02-coordinator-kill.sh"
"$HERE/03-pause-deposed-leader.sh"

echo ""
echo ">>> CHAOS SUITE PASSED — failover timers per node:"
for w in "${ALL_WORKERS[@]}"; do
  p=$(port_of "$w")
  echo "    $w: coordinator=$(metric_of "$p" scheduler_failover_coordinator_seconds_max)s worker=$(metric_of "$p" scheduler_failover_worker_seconds_max)s fenced-writes=$(metric_of "$p" scheduler_fencing_rejected_total)"
done
