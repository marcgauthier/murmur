#!/usr/bin/env bash
# Live Murmur-SQL multi-process node checks with isolated node directories.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
scenario=${1:-all}
binary=${MURMUR_BIN:-"$root/tests-live/bin/testnode"}
tags=${MURMUR_TAGS:-"sqlite_preupdate_hook sqlite_fts5"}
race_args=()
if [[ ${MURMUR_RACE:-0} == 1 ]]; then
  race_args=(-race)
fi
runtime_root=${MURMUR_LIVE_RUNTIME:-"$root/tests-live/runtime"}
started_at=$SECONDS

echo "======================================================================"
echo "  MURMUR-SQL LIVE MULTI-PROCESS SCENARIO RUNNER"
echo "======================================================================"

# Reap leftover daemons rooted at OUR runtime dir on the way out (Ctrl-C,
# inter-scenario kills, CI cancel). Scoped by --config path so parallel
# runs with their own MURMUR_LIVE_RUNTIME roots are never touched.
# No-op on clean runs (in-test cleanup already reaped everything).
sweep_runtime_daemons() {
  local pids
  pids=$(pgrep -f "testnode agent --config $runtime_root" 2>/dev/null || true)
  if [[ -z "$pids" ]]; then
    return 0
  fi
  # shellcheck disable=SC2086
  kill -TERM $pids 2>/dev/null || true
  sleep 2
  pids=$(pgrep -f "testnode agent --config $runtime_root" 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    # shellcheck disable=SC2086
    kill -KILL $pids 2>/dev/null || true
  fi
}
if [[ "$scenario" != "reload-benchmark" && "$scenario" != "open-progress" ]]; then
  trap sweep_runtime_daemons EXIT INT TERM
fi

# Embedded startup checks launch their own test workers.
if [[ "$scenario" != "reload-benchmark" && "$scenario" != "open-progress" ]]; then
  echo "Compiling testnode fixture binary at $binary with tags: '$tags'..."
  mkdir -p "$(dirname "$binary")"
  (cd "$root" && go build "${race_args[@]}" -tags "$tags" -o "$binary" ./tests-live/harness/testnode)
  # Tags stamp lets bare `go test ./tests-live/<scenario>` runs reuse this
  # binary while sources are unchanged (see findOrBuildTestNode).
  printf '%s' "$tags" > "$binary.tags"
fi

scenario_timeout() {
  # go test timeouts sized for each scenario's longest routine form
  # (the soak target extends durations via env; see below).
  case "$1" in
    reload-benchmark) echo "6h" ;;
    endurance-chaos) echo "76h" ;;
    soak-slo) echo "3h" ;;
    long-running-five-node) echo "75m" ;;
    files-soak) echo "15m" ;;
    snapshot-resync) echo "10m" ;;
    crash-recovery|chaos-load) echo "6m" ;;
    allow-nodes) echo "8m" ;;
    compression) echo "15m" ;;
    abuse) echo "30m" ;;
    resource-exhaustion) echo "25m" ;;
    *) echo "10m" ;;
  esac
}

run_scenario() {
  local scn=$1
  echo ""
  echo ">>> RUNNING LIVE SCENARIO: $scn"
  # Under -race the compression matrix exceeds 10 m; pass -short so
  # TestPebbleCompressionSizes skips and the suite finishes cleanly.
  local short_args=()
  if [[ ${#race_args[@]} -gt 0 && "$scn" == "compression" ]]; then
    short_args=(-short)
  fi
  (cd "$root" && go test "${race_args[@]}" -tags "$tags" -v -count=1 -timeout="$(scenario_timeout "$scn")" "${short_args[@]}" "./tests-live/$scn")
  echo ">>> SCENARIO COMPLETED: $scn"
}

ALL_SCENARIOS="abuse endurance-chaos resource-exhaustion merge-policies origin-signatures addrpolicy allow-nodes api-mtls backup-restore backup-under-fire bridge-two-streams cert-lifecycle chaos-load churn-retirement clock-skew compression corrupt-snapshot crash-loop crash-recovery crdt-contention dbid-isolation delete-pruning diskfull-live dos-client downgrade-guard encryption file-permissions files-bridge files-corrupt-source files-crash files-soak flap-partition fts-crash gc-balance graceful-shutdown gorm-migrate gorm-sync gorm-tx highlow hostile-peer hostile-schema impaired-network large-payload loadshare log-boundedness long-running-five-node maintenance-under-load migration-concurrency migration-crash open-progress overload-budgets partial-mesh partition pause-resume plaintext-audit plumtree-live rejoin-storm rekey release-upgrade rolling-restart scale-mesh schema-evolution snapshot-resync soak-slo sqli-api subscribe subscribe-backlog swim-discovery tail-repair tampered-backup three-node-sync three-way-heal tls-floor txchunk-resume unlock-abuse version-skew views write-priority"

# Release-gate subset, run by CI on every push/PR against the freshly built
# daemon binary: smoke, encryption, backup/restore, partitions, version
# skew, schema upgrades, release upgrades (previous-release binaries),
# High/Low, files, scale mesh, snapshot resync, GC balance, rolling
# restart, crash recovery, discovery mesh, migration crash,
# pause/resume, graceful shutdown, delete/crash/discovery/partition
# hardening (delete-pruning, tail-repair, txchunk-resume,
# swim-discovery, flap-partition, three-way-heal, crash-loop,
# rejoin-storm), backup integrity (backup-under-fire, tampered-backup),
# security posture (plaintext-audit, file-permissions, hostile-peer,
# hostile-schema, sqli-api, unlock-abuse, corrupt-snapshot), and
# subscription/migration coverage (subscribe-backlog,
# migration-concurrency), GORM-dialect acceptance (gorm-sync,
# gorm-migrate, gorm-tx), identity-crypto hardening (cert-lifecycle,
# tls-floor, dbid-isolation), and storage/ops hardening (fts-crash,
# files-corrupt-source, maintenance-under-load). Fast suites first. Env-gated
# (clock-skew, impaired-network, diskfull-live), adversarial-load
# (dos-client, log-boundedness), and history-dependent
# (downgrade-guard) suites run via `all`, not the gate.
GATE_SCENARIOS="merge-policies origin-signatures open-progress api-mtls three-node-sync gorm-sync gorm-migrate gorm-tx encryption backup-restore partition version-skew schema-evolution release-upgrade highlow files-bridge files-soak scale-mesh subscribe loadshare migration-crash pause-resume partial-mesh rolling-restart snapshot-resync gc-balance crash-recovery chaos-load graceful-shutdown tampered-backup files-crash file-permissions plaintext-audit unlock-abuse rejoin-storm migration-concurrency sqli-api subscribe-backlog crash-loop swim-discovery txchunk-resume hostile-peer hostile-schema tail-repair three-way-heal flap-partition delete-pruning corrupt-snapshot backup-under-fire tls-floor dbid-isolation cert-lifecycle files-corrupt-source fts-crash maintenance-under-load"

case "$scenario" in
  reload-benchmark)
    MURMUR_RELOAD_BENCH=${MURMUR_RELOAD_BENCH:-1} run_scenario "reload-benchmark"
    ;;
  all)
    for scn in $ALL_SCENARIOS; do
      run_scenario "$scn"
    done
    ;;
  gate)
    for scn in $GATE_SCENARIOS; do
      run_scenario "$scn"
    done
    ;;
  soak)
    # Long acceptance runs, scheduled separately (CI cron + manual dispatch):
    # ten-minute file soak, two-hour write SLO, one-hour five-node mesh.
    MURMUR_FILES_SOAK_DURATION_SECONDS=${MURMUR_FILES_SOAK_DURATION_SECONDS:-600} \
    MURMUR_FILES_SOAK_INTERVAL_SECONDS=${MURMUR_FILES_SOAK_INTERVAL_SECONDS:-60} \
      run_scenario "files-soak"
    MURMUR_SLO_DURATION_SECONDS=${MURMUR_SLO_DURATION_SECONDS:-7200} \
    MURMUR_SLO_SETTLE_SECONDS=${MURMUR_SLO_SETTLE_SECONDS:-300} \
      run_scenario "soak-slo"
    MURMUR_FIVE_NODE_DURATION_SECONDS=${MURMUR_FIVE_NODE_DURATION_SECONDS:-3600} \
    MURMUR_FIVE_NODE_SETTLE_SECONDS=${MURMUR_FIVE_NODE_SETTLE_SECONDS:-300} \
      run_scenario "long-running-five-node"
    ;;
  stress)
    # Concurrency-repetition lane, scheduled separately (CI cron + manual
    # dispatch), NOT per-PR: the 1-in-15 deadlocks this repo has hit (session
    # close/send ABBA, dual AcceptStream race) only reproduce under
    # repetition. MURMUR_STRESS_COUNT overrides the repeat count.
    stress_count=${MURMUR_STRESS_COUNT:-5}
    for scn in rolling-restart partial-mesh crash-recovery partition snapshot-resync; do
      echo ""
      echo ">>> STRESS $scn x$stress_count"
      (cd "$root" && go test "${race_args[@]}" -tags "$tags" -count="$stress_count" -timeout="$(scenario_timeout "$scn")" "./tests-live/$scn")
    done
    echo ""
    echo ">>> STRESS rolling-restart x2 under -race"
    (cd "$root" && go test -race -tags "$tags" -count=2 -timeout=15m "./tests-live/rolling-restart")
    ;;
  *)
    if [[ " $ALL_SCENARIOS " == *" $scenario "* ]]; then
      run_scenario "$scenario"
    else
      echo "Unknown scenario: $scenario" >&2
      echo "Available scenarios: $ALL_SCENARIOS reload-benchmark all gate soak stress" >&2
      exit 1
    fi
    ;;
esac

echo ""
echo "======================================================================"
echo "  ALL REQUESTED LIVE SCENARIOS PASSED (Elapsed: $((SECONDS - started_at))s)"
echo "======================================================================"
