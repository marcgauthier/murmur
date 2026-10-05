#!/usr/bin/env bash
# Benchmark suite runner: runs ONLY the suites under tests-benchmark/
# (in-process Go benchmarks, live replication benchmark, SQLite driver
# comparison). Each test folder has its own runner; live correctness
# scenarios live under tests-live/run.sh.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
scenario=${1:-all}
binary=${MURMUR_BIN:-"$root/tests-benchmark/bin/testnode"}
tags=${MURMUR_TAGS:-"sqlite_preupdate_hook sqlite_fts5"}
race_args=()
if [[ ${MURMUR_RACE:-0} == 1 ]]; then
  race_args=(-race)
fi
runtime_root=${MURMUR_LIVE_RUNTIME:-"$root/tests-benchmark/runtime"}
failures_dir=${MURMUR_LIVE_FAILURES:-"$root/tests-benchmark/failures"}
export MURMUR_BIN="$binary"
export MURMUR_LIVE_RUNTIME="$runtime_root"
export MURMUR_LIVE_FAILURES="$failures_dir"
started_at=$SECONDS

echo "======================================================================"
echo "  MURMUR-SQL BENCHMARK SUITE RUNNER"
echo "======================================================================"

# Reap leftover daemons rooted at OUR runtime dir on the way out, scoped by
# --config path so parallel runs with their own MURMUR_LIVE_RUNTIME roots
# (or tests-live runs) are never touched.
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
trap sweep_runtime_daemons EXIT INT TERM

# Compile testnode fixture binary (used by the replication suite) to ensure
# it matches current sources & tags.
echo "Compiling testnode fixture binary at $binary with tags: '$tags'..."
mkdir -p "$(dirname "$binary")"
(cd "$root" && go build "${race_args[@]}" -tags "$tags" -o "$binary" ./tests-live/harness/testnode)

scenario_timeout() {
  case "$1" in
    benchmark) echo "20m" ;;
    perf-matrix) echo "4h" ;;
    *) echo "10m" ;;
  esac
}

run_go_suite() {
  local scn=$1
  echo ""
  echo ">>> RUNNING BENCHMARK SUITE: $scn"
  (cd "$root" && go test "${race_args[@]}" -tags "$tags" -v -count=1 -timeout="$(scenario_timeout "$scn")" "./tests-benchmark/$scn")
  echo ">>> SUITE COMPLETED: $scn"
}

run_benchmark_pkg() {
  # In-process Go suite: throughput Tests first, then the -short
  # micro-benchmark fast pass (10K datasets).
  local benchtime=${MURMUR_BENCH_BENCHTIME:-1s}
  echo ""
  echo ">>> RUNNING BENCHMARK SUITE: benchmark (throughput tests)"
  (cd "$root" && go test "${race_args[@]}" -tags "$tags" -v -count=1 -timeout="$(scenario_timeout benchmark)" "./tests-benchmark/benchmark")
  echo ""
  echo ">>> RUNNING BENCHMARK SUITE: benchmark (micro-benchmarks, -short, benchtime=$benchtime)"
  (cd "$root" && go test "${race_args[@]}" -tags "$tags" -count=1 -timeout="$(scenario_timeout benchmark)" -run '^$' -bench . -short -benchtime "$benchtime" "./tests-benchmark/benchmark")
  echo ">>> SUITE COMPLETED: benchmark"
}

run_perf_matrix() {
  # Characterization matrix only (tiers via MURMUR_PERF_TIER; the full
  # tier builds a 1M-row template and runs 10-node live cells).
  local tier=${MURMUR_PERF_TIER:-standard}
  echo ""
  echo ">>> RUNNING PERF MATRIX (tier=$tier)"
  (cd "$root" && go test "${race_args[@]}" -tags "$tags" -v -count=1 -timeout="$(scenario_timeout perf-matrix)" -run '^TestPerfMatrix$' "./tests-benchmark/benchmark")
  echo ">>> SUITE COMPLETED: perf-matrix"
}

ALL_SUITES="replication sqlite-bench benchmark"

case "$scenario" in
  all)
    for scn in $ALL_SUITES; do
      if [[ "$scn" == "benchmark" ]]; then
        run_benchmark_pkg
      else
        run_go_suite "$scn"
      fi
    done
    ;;
  benchmark)
    run_benchmark_pkg
    ;;
  perf-matrix)
    run_perf_matrix
    ;;
  *)
    if [[ " $ALL_SUITES " == *" $scenario "* ]]; then
      run_go_suite "$scenario"
    else
      echo "Unknown suite: $scenario" >&2
      echo "Available suites: $ALL_SUITES all perf-matrix" >&2
      exit 1
    fi
    ;;
esac

echo ""
echo "======================================================================"
echo "  ALL REQUESTED BENCHMARK SUITES PASSED (Elapsed: $((SECONDS - started_at))s)"
echo "======================================================================"
