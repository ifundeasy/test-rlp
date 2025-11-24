#!/usr/bin/env bash
set -euo pipefail

LOGFILE="benchmark/2-create-schema.log"

# Empty the log file at the start
: > "$LOGFILE"

run() {
  local engine="$1"

  echo "❯ go run ./cmd/main.go ${engine} create-schema" | tee -a "$LOGFILE"
  # Merge stderr into stdout (2>&1), then pipe to tee
  go run ./cmd/main.go "${engine}" create-schema 2>&1 | tee -a "$LOGFILE"
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
