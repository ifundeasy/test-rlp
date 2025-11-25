package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"test-tls/cmd/authzed_crdb_1"
	"test-tls/cmd/authzed_pgdb_1"
	"test-tls/cmd/clickhouse_1"
	"test-tls/cmd/cockroachdb_1"
	"test-tls/cmd/csv"
	"test-tls/cmd/elasticsearch_1"
	"test-tls/cmd/mongodb_1"
	"test-tls/cmd/postgres_1"
	"test-tls/cmd/scylladb_1"
)

// handler is a function that handles a module/subcommand.
type handler func(args []string) error

// modules maps module names to their handlers.
var modules = map[string]handler{
	"csv":             runCsv,
	"authzed_crdb_1":  runAuthzedCrdb1,
	"authzed_pgdb_1":  runAuthzedPgdb1,
	"clickhouse_1":    runClickhouse1,
	"cockroachdb_1":   runCockroachdb1,
	"postgres_1":      runPostgres1,
	"mongodb_1":       runMongodb1,
	"scylladb_1":      runScylladb1,
	"elasticsearch_1": runElasticsearch1,
}

func main() {
	// Configure global logger to include date, time and sub-second precision.
	// Use microsecond precision (includes milliseconds) for readable timing.
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	if err := dispatch(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		fmt.Fprintln(os.Stderr)
		usage()
		os.Exit(1)
	}
}

// dispatch picks the module from args[0] and forwards the rest to it.
func dispatch(args []string) error {
	if len(args) == 0 {
		return errors.New("missing module")
	}

	moduleName := args[0]
	handler, ok := modules[moduleName]
	if !ok {
		return fmt.Errorf("unknown module: %s", moduleName)
	}

	return handler(args[1:])
}

func runCsv(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for csv (expected: "load-data")`)
	}

	action := args[0]

	switch action {
	case "load-data":
		csv.CsvCreateData()
		return nil
	default:
		return fmt.Errorf("unknown action for csv: %s", action)
	}
}

func runAuthzedCrdb1(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for authzed_crdb_1 (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		authzed_crdb_1.AuthzedDropSchemas()
	case "create-schema":
		authzed_crdb_1.AuthzedCreateSchema()
	case "load-data":
		authzed_crdb_1.AuthzedCreateData()
	case "benchmark":
		authzed_crdb_1.AuthzedBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for authzed_crdb_1: %s", action)
	}

	return nil
}

func runAuthzedPgdb1(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for authzed_pgdb_1 (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		authzed_pgdb_1.AuthzedDropSchemas()
	case "create-schema":
		authzed_pgdb_1.AuthzedCreateSchema()
	case "load-data":
		authzed_pgdb_1.AuthzedCreateData()
	case "benchmark":
		authzed_pgdb_1.AuthzedBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for authzed_pgdb_1: %s", action)
	}

	return nil
}

func runClickhouse1(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for clickhouse_1 (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		clickhouse_1.ClickhouseDropSchemas()
	case "create-schema":
		clickhouse_1.ClickhouseCreateSchemas()
	case "load-data":
		clickhouse_1.ClickhouseCreateData()
	case "benchmark":
		clickhouse_1.ClickhouseBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for clickhouse_1: %s", action)
	}

	return nil
}

func runCockroachdb1(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for cockroachdb_1 (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		cockroachdb_1.CockroachdbDropSchemas()
	case "create-schema":
		cockroachdb_1.CockroachdbCreateSchemas()
	case "load-data":
		cockroachdb_1.CockroachdbCreateData()
	case "benchmark":
		cockroachdb_1.CockroachdbBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for cockroachdb_1: %s", action)
	}

	return nil
}

func runPostgres1(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for postgres_1 (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		postgres_1.PostgresDropSchemas()
	case "create-schema":
		postgres_1.PostgresCreateSchemas()
	case "load-data":
		postgres_1.PostgresCreateData()
	case "benchmark":
		postgres_1.PostgresBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for postgres_1: %s", action)
	}

	return nil
}

func runMongodb1(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for mongodb_1 (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		mongodb_1.MongodbDropSchemas()
	case "create-schema":
		mongodb_1.MongodbCreateSchemas()
	case "load-data":
		mongodb_1.MongodbCreateData()
	case "benchmark":
		mongodb_1.MongodbBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for scylla_1: %s", action)
	}

	return nil
}

func runScylladb1(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for scylladb_1 (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		scylladb_1.ScylladbDropSchemas()
	case "create-schema":
		scylladb_1.ScylladbCreateSchemas()
	case "load-data":
		scylladb_1.ScylladbCreateData()
	case "benchmark":
		scylladb_1.ScylladbBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for scylla_1: %s", action)
	}

	return nil
}

func runElasticsearch1(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for elasticsearch_1 (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		elasticsearch_1.ElasticsearchDropSchemas()
	case "create-schema":
		elasticsearch_1.ElasticsearchCreateSchemas()
	case "load-data":
		elasticsearch_1.ElasticsearchCreateData()
	case "benchmark":
		elasticsearch_1.ElasticsearchBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for elasticsearch_1: %s", action)
	}

	return nil
}

func usage() {
	prog := os.Args[0]
	fmt.Println("usage:")
	fmt.Printf("  %s csv load-data\n", prog)
	fmt.Printf("  %s authzed_crdb_1 drop\n", prog)
	fmt.Printf("  %s authzed_crdb_1 create-schema\n", prog)
	fmt.Printf("  %s authzed_crdb_1 load-data\n", prog)
	fmt.Printf("  %s authzed_crdb_1 benchmark\n", prog)
}
