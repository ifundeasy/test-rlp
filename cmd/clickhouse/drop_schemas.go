package clickhouse

import (
	"context"
	"database/sql"
	"log"
	"time"

	"test-tls/infrastructure"
)

// ClickhouseDropSchemas drops all tables and the materialized view created
// by the ClickHouse schema for benchmarks. Logging mirrors other modules.
func ClickhouseDropSchemas() {
	ctx := context.Background()

	db, cleanup, err := infrastructure.NewClickhouseFromEnv(ctx)
	if err != nil {
		log.Fatalf("[clickhouse] drop_schemas: connect failed: %v", err)
	}
	defer cleanup()

	start := time.Now()
	log.Printf("[clickhouse] == Starting ClickHouse drop schemas ==")

	// Drop materialized view first
	stmts := []string{
		`DROP VIEW IF EXISTS user_resource_permissions_mv`,
		// Tables (children first where applicable)
		`DROP TABLE IF EXISTS user_resource_permissions`,
		`DROP TABLE IF EXISTS resource_acl`,
		`DROP TABLE IF EXISTS resources`,
		`DROP TABLE IF EXISTS group_members_expanded`,
		`DROP TABLE IF EXISTS group_hierarchy`,
		`DROP TABLE IF EXISTS group_memberships`,
		`DROP TABLE IF EXISTS org_memberships`,
		`DROP TABLE IF EXISTS groups`,
		`DROP TABLE IF EXISTS users`,
		`DROP TABLE IF EXISTS organizations`,
	}

	for _, s := range stmts {
		if err := execTimeout(ctx, db, s, 60*time.Second); err != nil {
			log.Printf("[clickhouse] warning: executing %q failed: %v", s, err)
			continue
		}
		log.Printf("[clickhouse] Executed: %s", s)
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[clickhouse] ClickHouse drop schemas DONE: elapsed=%s", elapsed)
}

func execTimeout(parent context.Context, db *sql.DB, stmt string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	_, err := db.ExecContext(ctx, stmt)
	return err
}
