package clickhouse

import (
	"context"
	"database/sql"
	"log"
	"os"
	"strings"
	"time"

	"test-tls/infrastructure"
)

// ClickhouseCreateSchemas creates all tables and indexes in ClickHouse
// optimized for RLS-style "check" and "list" queries that walk the
// org -> group -> user -> resource chains.
func ClickhouseCreateSchemas() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, cleanup, err := infrastructure.NewClickhouseFromEnv(ctx)
	if err != nil {
		log.Fatalf("[clickhouse] create_schemas: connect failed: %v", err)
	}
	defer cleanup()

	// Read SQL statements from external file (allows easier editing and
	// keeps raw DDL separate from Go code).
	schemaFile := "cmd/clickhouse/schemas.sql"
	content, err := os.ReadFile(schemaFile)
	if err != nil {
		log.Fatalf("[clickhouse] read %s failed: %v", schemaFile, err)
	}

	// Naive split on semicolons; skip empty statements and comments.
	raw := string(content)
	parts := strings.Split(raw, ";")
	for _, p := range parts {
		stmt := strings.TrimSpace(p)
		if stmt == "" {
			continue
		}
		execWithTimeout(ctx, db, stmt, 30*time.Second)
	}

	log.Println("[clickhouse] Schemas created successfully.")
}

func execWithTimeout(parent context.Context, db *sql.DB, stmt string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	if _, err := db.ExecContext(ctx, stmt); err != nil {
		log.Fatalf("[clickhouse] executing %q failed: %v", stmt, err)
	}
}
