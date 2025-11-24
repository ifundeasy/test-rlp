#!/usr/bin/env bash
set -euo pipefail

LOGFILE="benchmark/3-load-data.log"

# Clear the log file at the beginning
: > "$LOGFILE"

run() {
  local engine="$1"

  echo "❯ go run ./cmd/main.go ${engine} load-data" | tee -a "$LOGFILE"
  # Combine stderr to stdout, then log + display to terminal
  go run ./cmd/main.go "${engine}" load-data 2>&1 | tee -a "$LOGFILE"
  echo "" | tee -a "$LOGFILE"
}

run authzed_crdb_1
run authzed_pgdb_1
run clickhouse_1
run cockroachdb_1
run elasticsearch_1
run postgres_1
run mongodb_1
run scylladb_1
