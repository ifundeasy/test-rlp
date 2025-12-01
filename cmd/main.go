package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"test-tls/cmd/authzed_crdb"
	"test-tls/cmd/authzed_pgdb"
	"test-tls/cmd/clickhouse"
	"test-tls/cmd/cockroachdb"
	"test-tls/cmd/csv"
	"test-tls/cmd/elasticsearch"
	"test-tls/cmd/mongodb"
	"test-tls/cmd/postgres"
	"test-tls/cmd/scylladb"
)

// handler is a function that handles a module/subcommand.
type handler func(args []string) error

// modules maps module names to their handlers.
var modules = map[string]handler{
	"csv":           runCsv,
	"authzed_crdb":  runAuthzedCrdb,
	"authzed_pgdb":  runAuthzedPgdb,
	"clickhouse":    runClickhouse,
	"cockroachdb":   runCockroachdb,
	"postgres":      runPostgres,
	"mongodb":       runMongodb,
	"scylladb":      runScylladb,
	"elasticsearch": runElasticsearch,
}

func main() {
	// Configure global logger to include date, time and sub-second precision.
	// Use microsecond precision (includes milliseconds) for readable timing.
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	// Load root .env first, then benchmark env overrides if present.
	if err := loadEnvFile(".env"); err != nil {
		log.Printf("WARN: could not load env file .env: %v", err)
	}

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
		return errors.New(`missing action for csv (expected: "generate")`)
	}

	action := args[0]

	switch action {
	case "generate":
		csv.CsvCreateData()
		return nil
	default:
		return fmt.Errorf("unknown action for csv: %s", action)
	}
}

func runAuthzedCrdb(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for authzed_crdb (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		authzed_crdb.AuthzedDropSchemas()
	case "create-schema":
		authzed_crdb.AuthzedCreateSchema()
	case "load-data":
		authzed_crdb.AuthzedCreateData()
	case "benchmark":
		authzed_crdb.AuthzedBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for authzed_crdb: %s", action)
	}

	return nil
}

func runAuthzedPgdb(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for authzed_pgdb (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		authzed_pgdb.AuthzedDropSchemas()
	case "create-schema":
		authzed_pgdb.AuthzedCreateSchema()
	case "load-data":
		authzed_pgdb.AuthzedCreateData()
	case "benchmark":
		authzed_pgdb.AuthzedBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for authzed_pgdb: %s", action)
	}

	return nil
}

func runClickhouse(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for clickhouse (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		clickhouse.ClickhouseDropSchemas()
	case "create-schema":
		clickhouse.ClickhouseCreateSchemas()
	case "load-data":
		clickhouse.ClickhouseCreateData()
	case "benchmark":
		clickhouse.ClickhouseBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for clickhouse: %s", action)
	}

	return nil
}

func runCockroachdb(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for cockroachdb (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		cockroachdb.CockroachdbDropSchemas()
	case "create-schema":
		cockroachdb.CockroachdbCreateSchemas()
	case "load-data":
		cockroachdb.CockroachdbCreateData()
		cockroachdb.CockroachdbRefreshUserResourcePermissions()
	case "benchmark":
		cockroachdb.CockroachdbBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for cockroachdb: %s", action)
	}

	return nil
}

func runPostgres(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for postgres (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		postgres.PostgresDropSchemas()
	case "create-schema":
		postgres.PostgresCreateSchemas()
	case "load-data":
		postgres.PostgresCreateData()
	case "benchmark":
		postgres.PostgresBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for postgres: %s", action)
	}

	return nil
}

func runMongodb(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for mongodb (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		mongodb.MongodbDropSchemas()
	case "create-schema":
		mongodb.MongodbCreateSchemas()
	case "load-data":
		mongodb.MongodbCreateData()
	case "benchmark":
		mongodb.MongodbBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for scylla: %s", action)
	}

	return nil
}

func runScylladb(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for scylladb (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		scylladb.ScylladbDropSchemas()
	case "create-schema":
		scylladb.ScylladbCreateSchemas()
	case "load-data":
		scylladb.ScylladbCreateData()
	case "benchmark":
		scylladb.ScylladbBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for scylla: %s", action)
	}

	return nil
}

func runElasticsearch(args []string) error {
	if len(args) == 0 {
		return errors.New(`missing action for elasticsearch (expected: "drop|create-schema|load-data|benchmark")`)
	}

	action := args[0]

	switch action {
	case "drop":
		elasticsearch.ElasticsearchDropSchemas()
	case "create-schema":
		elasticsearch.ElasticsearchCreateSchemas()
	case "load-data":
		elasticsearch.ElasticsearchCreateData()
	case "benchmark":
		elasticsearch.ElasticsearchBenchmarkReads()
	default:
		return fmt.Errorf("unknown action for elasticsearch: %s", action)
	}

	return nil
}

func usage() {
	prog := os.Args[0]
	fmt.Println("usage:")
	fmt.Printf("  %s csv generate\n", prog)
	fmt.Printf("  %s authzed_crdb drop\n", prog)
	fmt.Printf("  %s authzed_crdb create-schema\n", prog)
	fmt.Printf("  %s authzed_crdb load-data\n", prog)
	fmt.Printf("  %s authzed_crdb benchmark\n", prog)
}

// loadEnvFile reads a simple KEY=VALUE env file and sets variables.
// Lines starting with '#' are treated as comments; blank lines are skipped.
// Values can be quoted with single or double quotes; surrounding quotes are trimmed.
func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		// If the file doesn't exist, return a descriptive error so caller can warn.
		if os.IsNotExist(err) {
			return fmt.Errorf("env file not found: %s", path)
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Support export KEY=VALUE lines
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		// Split on first '=' only
		eq := strings.IndexRune(line, '=')
		if eq <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		// Trim surrounding quotes
		if len(val) >= 2 {
			if (val[0] == '\'' && val[len(val)-1] == '\'') || (val[0] == '"' && val[len(val)-1] == '"') {
				val = val[1 : len(val)-1]
			}
		}
		// Expand existing env references like ${VAR}
		val = os.ExpandEnv(val)
		_ = os.Setenv(key, val)
	}
	return scanner.Err()
}
