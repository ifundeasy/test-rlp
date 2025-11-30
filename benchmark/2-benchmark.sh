#!/usr/bin/env zsh

set -euo pipefail

# Resolve directories
SCRIPT_DIR=${0:a:h}
ROOT_DIR=${SCRIPT_DIR:h}

# Log files
LOG_CREATE="$SCRIPT_DIR/2-1-create-schemas.log"
LOG_LOAD="$SCRIPT_DIR/2-2-load-data.log"
LOG_BENCH="$SCRIPT_DIR/2-3-benchmark.log"

# Delay seconds between benchmark runs
DELAY_SECS=5

# Blank line + engine heading in all logs + terminal
log_engine_header() {
	local engine="$1"
	for lf in "$LOG_CREATE" "$LOG_LOAD" "$LOG_BENCH"; do
		{ echo ""; echo "==== ENGINE: $engine ===="; } >> "$lf"
	done
	echo "==== ENGINE: $engine ===="
}

flush_logs() {
	: > "$LOG_CREATE"
	: > "$LOG_LOAD"
	: > "$LOG_BENCH"
}

source_env() {
	if [[ -f "$ROOT_DIR/.env" ]]; then
		echo "[env] load $ROOT_DIR/.env"
		set -a; source "$ROOT_DIR/.env"; set +a
	fi
	if [[ -f "$SCRIPT_DIR/bench.env" ]]; then
		echo "[env] load $SCRIPT_DIR/bench.env"
		set -a; source "$SCRIPT_DIR/bench.env"; set +a
	fi
}

run_with_log() {
	local logfile=$1; shift
	echo "[cmd] $*"
	"$@" 2>&1 | tee -a "$logfile"
}

run_no_log() {
	echo "[cmd] $*"
	"$@" 2>&1
}

# Setup containers & readiness
setup_crdb() { pushd "$ROOT_DIR/docker" >/dev/null; run_no_log docker compose down -v; run_no_log docker compose up -d cockroachdb; wait_crdb_ready || { popd >/dev/null; return 1; }; run_no_log docker compose exec cockroachdb ./cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS rlp"; run_no_log docker compose exec cockroachdb ./cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS rlp_spicedb"; run_no_log docker compose exec cockroachdb ./cockroach sql --insecure -e "SHOW DATABASES"; run_no_log docker compose exec cockroachdb ./cockroach sql --insecure -e "SET CLUSTER SETTING kv.rangefeed.enabled = true"; popd >/dev/null; }
setup_postgres() { pushd "$ROOT_DIR/docker" >/dev/null; run_no_log docker compose down -v; run_no_log docker compose up -d postgres; wait_postgres_ready || { popd >/dev/null; return 1; }; popd >/dev/null; }
setup_scylladb() { pushd "$ROOT_DIR/docker" >/dev/null; run_no_log docker compose down -v; run_no_log docker compose up -d scylladb; wait_scylladb_ready || { popd >/dev/null; return 1; }; popd >/dev/null; }
setup_clickhouse() { pushd "$ROOT_DIR/docker" >/dev/null; run_no_log docker compose down -v; run_no_log docker compose up -d clickhouse; wait_clickhouse_ready || { popd >/dev/null; return 1; }; popd >/dev/null; }
setup_mongodb() { pushd "$ROOT_DIR/docker" >/dev/null; run_no_log docker compose down -v; run_no_log docker compose up -d mongodb; wait_mongodb_ready || { popd >/dev/null; return 1; }; popd >/dev/null; }
setup_elasticsearch() { pushd "$ROOT_DIR/docker" >/dev/null; run_no_log docker compose down -v; run_no_log docker compose up -d elasticsearch; wait_elasticsearch_ready || { popd >/dev/null; return 1; }; popd >/dev/null; }

# Scenarios
scenario_cockroachdb() { log_engine_header "cockroachdb"; run_with_log "$LOG_CREATE" go run cmd/main.go cockroachdb create-schema; run_with_log "$LOG_LOAD" go run cmd/main.go cockroachdb load-data; benchmark_loop cockroachdb; }
scenario_authzed_crdb() { log_engine_header "authzed_crdb"; pushd "$ROOT_DIR/docker" >/dev/null; run_no_log docker compose down -v; run_no_log docker compose up -d cockroachdb; wait_crdb_ready || { popd >/dev/null; return 1; }; run_no_log docker compose exec cockroachdb ./cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS rlp"; run_no_log docker compose exec cockroachdb ./cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS rlp_spicedb"; run_no_log docker compose exec cockroachdb ./cockroach sql --insecure -e "SHOW DATABASES"; run_no_log docker compose exec cockroachdb ./cockroach sql --insecure -e "SET CLUSTER SETTING kv.rangefeed.enabled = true"; run_no_log docker compose run --rm spicedb-crdb migrate head; run_no_log docker compose up -d spicedb-crdb spicedb-crdb-envoy; popd >/dev/null; run_with_log "$LOG_CREATE" go run cmd/main.go authzed_crdb create-schema; run_with_log "$LOG_LOAD" go run cmd/main.go authzed_crdb load-data; benchmark_loop authzed_crdb; }
scenario_postgres() { log_engine_header "postgres"; run_with_log "$LOG_CREATE" go run cmd/main.go postgres create-schema; run_with_log "$LOG_LOAD" go run cmd/main.go postgres load-data; benchmark_loop postgres; }
scenario_authzed_pgdb() { log_engine_header "authzed_pgdb"; pushd "$ROOT_DIR/docker" >/dev/null; run_no_log docker compose down -v; run_no_log docker compose up -d postgres; wait_postgres_ready || { popd >/dev/null; return 1; }; run_no_log docker compose run --rm spicedb-pgdb migrate head; run_no_log docker compose up -d spicedb-pgdb spicedb-pgdb-envoy; popd >/dev/null; run_with_log "$LOG_CREATE" go run cmd/main.go authzed_pgdb create-schema; run_with_log "$LOG_LOAD" go run cmd/main.go authzed_pgdb load-data; benchmark_loop authzed_pgdb; }
scenario_scylladb() { log_engine_header "scylladb"; run_with_log "$LOG_CREATE" go run cmd/main.go scylladb create-schema; run_with_log "$LOG_LOAD" go run cmd/main.go scylladb load-data; benchmark_loop scylladb; }
scenario_clickhouse() { log_engine_header "clickhouse"; run_with_log "$LOG_CREATE" go run cmd/main.go clickhouse create-schema; run_with_log "$LOG_LOAD" go run cmd/main.go clickhouse load-data; benchmark_loop clickhouse; }
scenario_mongodb() { log_engine_header "mongodb"; run_with_log "$LOG_CREATE" go run cmd/main.go mongodb create-schema; run_with_log "$LOG_LOAD" go run cmd/main.go mongodb load-data; benchmark_loop mongodb; }
scenario_elasticsearch() { log_engine_header "elasticsearch"; run_with_log "$LOG_CREATE" go run cmd/main.go elasticsearch create-schema; run_with_log "$LOG_LOAD" go run cmd/main.go elasticsearch load-data; benchmark_loop elasticsearch; }

benchmark_loop() {
	local engine="$1"
	for i in {1..5}; do
		echo "[benchmark][$engine] run $i/5 (delay ${DELAY_SECS}s after)" | tee -a "$LOG_BENCH"
		run_with_log "$LOG_BENCH" go run cmd/main.go "$engine" benchmark
		if [[ $i -lt 5 ]]; then
			echo "[benchmark][$engine] sleep ${DELAY_SECS}s" | tee -a "$LOG_BENCH"
			run_no_log sleep $DELAY_SECS
		fi
	done
}

# Readiness helpers
wait_crdb_ready() { local tries=30; for i in {1..$tries}; do if docker compose exec cockroachdb ./cockroach sql --insecure -e "SELECT 1" >/dev/null 2>&1; then echo "[ready] cockroachdb"; return 0; fi; echo "[wait] cockroachdb ($i/$tries)"; sleep 2; done; echo "[error] cockroachdb not ready"; return 1; }
wait_postgres_ready() { local tries=30; for i in {1..$tries}; do if docker compose exec postgres pg_isready -U postgres >/dev/null 2>&1; then echo "[ready] postgres"; return 0; fi; echo "[wait] postgres ($i/$tries)"; sleep 2; done; echo "[error] postgres not ready"; return 1; }
wait_scylladb_ready() { local tries=40; for i in {1..$tries}; do if docker compose exec scylladb bash -c "cqlsh -e 'SELECT now() FROM system.local;'" >/dev/null 2>&1; then echo "[ready] scylladb"; return 0; fi; echo "[wait] scylladb ($i/$tries)"; sleep 3; done; echo "[error] scylladb not ready"; return 1; }
wait_clickhouse_ready() { local tries=30; for i in {1..$tries}; do if docker compose exec clickhouse clickhouse-client --query 'SELECT 1' >/dev/null 2>&1; then echo "[ready] clickhouse"; return 0; fi; echo "[wait] clickhouse ($i/$tries)"; sleep 2; done; echo "[error] clickhouse not ready"; return 1; }
wait_mongodb_ready() { local tries=40; for i in {1..$tries}; do if docker compose exec mongodb mongosh --quiet --eval 'db.runCommand({ping:1})' >/dev/null 2>&1; then echo "[ready] mongodb"; return 0; fi; echo "[wait] mongodb ($i/$tries)"; sleep 2; done; echo "[error] mongodb not ready"; return 1; }
wait_elasticsearch_ready() { local tries=40; for i in {1..$tries}; do if curl -s localhost:9200 >/dev/null 2>&1 || docker compose exec elasticsearch curl -s localhost:9200 >/dev/null 2>&1; then echo "[ready] elasticsearch"; return 0; fi; echo "[wait] elasticsearch ($i/$tries)"; sleep 3; done; echo "[error] elasticsearch not ready"; return 1; }

usage() { echo "Usage: $0 [all|cockroachdb|authzed_crdb|postgres|authzed_pgdb|scylladb|clickhouse|mongodb|elasticsearch]"; exit 1; }

main() {
	local choice=${1:-all}
	source_env
	flush_logs
	case "$choice" in
		all)
			# Ordered sequential run; uncomment scenarios to include others
			echo "[run] authzed_crdb"; scenario_authzed_crdb
			echo "[run] authzed_pgdb"; scenario_authzed_pgdb
                        echo "[run] scylladb"; setup_scylladb; scenario_scylladb
			echo "[run] cockroachdb"; setup_crdb; scenario_cockroachdb
			echo "[run] postgres"; setup_postgres; scenario_postgres
			echo "[run] mongodb"; setup_mongodb; scenario_mongodb
			echo "[run] clickhouse"; setup_clickhouse; scenario_clickhouse
			echo "[run] elasticsearch"; setup_elasticsearch; scenario_elasticsearch
			;;
		cockroachdb) setup_crdb; scenario_cockroachdb ;;
		authzed_crdb|authzed-crdb) scenario_authzed_crdb ;;
		postgres) setup_postgres; scenario_postgres ;;
		authzed_pgdb|authzed-pgdb) scenario_authzed_pgdb ;;
		scylladb) setup_scylladb; scenario_scylladb ;;
		clickhouse) setup_clickhouse; scenario_clickhouse ;;
		mongodb) setup_mongodb; scenario_mongodb ;;
		elasticsearch) setup_elasticsearch; scenario_elasticsearch ;;
		*) usage ;;
	esac
}

main "$@"

