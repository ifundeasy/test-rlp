#!/usr/bin/env bash
set -euo pipefail

LOGFILE="benchmark/4-read-data.log"

# Load env from prepare (BENCH_LOOKUPRES_MANAGE_USER, BENCH_LOOKUPRES_VIEW_USER, etc)
if [[ -f benchmark/bench.env ]]; then
  # shellcheck disable=SC1091
  source benchmark/bench.env
else
  echo "benchmark/bench.env not found. Run ./prepare.sh first." >&2
  exit 1
fi

# Empty the log file at the start
: > "$LOGFILE"

run() {
  local engine="$1"

  for i in {1..10}; do
    echo "❯ [${engine}] iteration ${i}/10: go run ./cmd/main.go ${engine} benchmark" | tee -a "$LOGFILE"
    # Merge stderr into stdout, then log + show to terminal
    go run ./cmd/main.go "${engine}" benchmark 2>&1 | tee -a "$LOGFILE"
    echo "" | tee -a "$LOGFILE"

    # Sleep 3 seconds between iterations, except after the last one
    if [[ "$i" -lt 10 ]]; then
      sleep 3
    fi
  done
}

run authzed_crdb_1
run authzed_pgdb_1
run clickhouse_1
run cockroachdb_1
run elasticsearch_1
run postgres_1
run mongodb_1
run scylladb_1
