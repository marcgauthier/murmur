#!/usr/bin/env bash
# Live SPeD-SQL multi-process node checks with isolated node directories.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
scenario=${1:-all}
binary=${SPEDSQL_BIN:-"$root/bin/spedsql"}
runtime_root=${SPEDSQL_LIVE_RUNTIME:-"$root/tests-live/runtime"}
started_at=$SECONDS

echo "======================================================================"
echo "  SPeD-SQL LIVE MULTI-PROCESS SCENARIO RUNNER"
echo "======================================================================"

# Build spedsql daemon binary if not found or out of date
if [[ ! -x "$binary" ]]; then
  echo "Building spedsql daemon binary at $binary..."
  mkdir -p "$(dirname "$binary")"
  (cd "$root" && go build -o "$binary" ./cmd/spedsql)
fi

run_scenario() {
  local scn=$1
  echo ""
  echo ">>> RUNNING LIVE SCENARIO: $scn"
  (cd "$root" && go test -v -count=1 "./tests-live/$scn")
  echo ">>> SCENARIO COMPLETED: $scn"
}

case "$scenario" in
  all)
    run_scenario "encryption"
    run_scenario "crash-recovery"
    run_scenario "three-node-sync"
    run_scenario "partition"
    run_scenario "chaos-load"
    run_scenario "addrpolicy"
    run_scenario "allow-nodes"
    run_scenario "crdt-contention"
    run_scenario "soak-slo"
    run_scenario "long-running-five-node"
    ;;
  encryption|crash-recovery|three-node-sync|partition|chaos-load|addrpolicy|allow-nodes|crdt-contention|soak-slo|long-running-five-node)
    run_scenario "$scenario"
    ;;
  *)
    if [[ -d "$root/tests-live/$scenario" ]]; then
      run_scenario "$scenario"
    else
      echo "Unknown scenario: $scenario" >&2
      echo "Available scenarios: encryption, crash-recovery, three-node-sync, partition, chaos-load, addrpolicy, allow-nodes, crdt-contention, soak-slo, long-running-five-node, all" >&2
      exit 1
    fi
    ;;
esac

echo ""
echo "======================================================================"
echo "  ALL REQUESTED LIVE SCENARIOS PASSED (Elapsed: $((SECONDS - started_at))s)"
echo "======================================================================"
