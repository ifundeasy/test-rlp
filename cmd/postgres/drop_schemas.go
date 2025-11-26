package postgres

import (
	"context"
	"database/sql"
	"log"
	"time"

	"test-tls/infrastructure"
)

// PostgresDropSchemas drops all ACL-related tables used by postgres benchmarks.
// It does NOT drop the database or the public schema, only the tables we created.
func PostgresDropSchemas() {
	ctx := context.Background()

	db, cleanup, err := infrastructure.NewPostgresFromEnv(ctx)
	if err != nil {
		log.Fatalf("[postgres] failed to create postgres client: %v", err)
	}
	defer cleanup()

	log.Printf("[postgres] == Dropping Postgres ACL tables ==")

	// Drop order: children -> parents to avoid FK issues, plus CASCADE for safety.
	// Objects added by schemas.sql include:
	// - materialized view: user_resource_permissions
	// - helper function: refresh_user_resource_permissions()
	// - table: group_hierarchy
	// We drop the materialized view and function first, then tables.
	statements := []string{
		// materialized view and its refresh helper
		`DROP MATERIALIZED VIEW IF EXISTS user_resource_permissions`,
		`DROP FUNCTION IF EXISTS refresh_user_resource_permissions()`,

		// ACL tables (children before parents)
		`DROP TABLE IF EXISTS resource_acl CASCADE`,
		`DROP TABLE IF EXISTS resources CASCADE`,
		`DROP TABLE IF EXISTS group_memberships CASCADE`,
		`DROP TABLE IF EXISTS group_hierarchy CASCADE`,
		`DROP TABLE IF EXISTS org_memberships CASCADE`,
		`DROP TABLE IF EXISTS groups CASCADE`,
		`DROP TABLE IF EXISTS users CASCADE`,
		`DROP TABLE IF EXISTS organizations CASCADE`,
	}

	for _, stmt := range statements {
		if err := execWithTimeout(ctx, db, stmt, 30*time.Second); err != nil {
			log.Fatalf("[postgres] executing %q failed: %v", stmt, err)
		}
		log.Printf("[postgres] Executed: %s", stmt)
	}

	log.Printf("[postgres] Postgres ACL tables drop DONE")
}

func execWithTimeout(parent context.Context, db *sql.DB, stmt string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	_, err := db.ExecContext(ctx, stmt)
	return err
}
