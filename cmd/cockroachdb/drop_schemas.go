package cockroachdb

import (
	"context"
	"database/sql"
	"log"
	"time"

	"test-tls/infrastructure"
)

// CockroachdbDropSchemas drops all ACL-related tables used by cockroachdb benchmarks.
// It does NOT drop the database or the public schema, only the tables we created.
func CockroachdbDropSchemas() {
	ctx := context.Background()

	db, cleanup, err := infrastructure.NewCockroachDBFromEnv(ctx)
	if err != nil {
		log.Fatalf("[cockroachdb] failed to create cockroachdb client: %v", err)
	}
	defer cleanup()

	log.Printf("[cockroachdb] == Dropping Cockroachdb ACL tables ==")

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
			log.Fatalf("[cockroachdb] executing %q failed: %v", stmt, err)
		}
		log.Printf("[cockroachdb] Executed: %s", stmt)
	}

	log.Printf("[cockroachdb] Cockroachdb ACL tables drop DONE")
}

func execWithTimeout(parent context.Context, db *sql.DB, stmt string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	_, err := db.ExecContext(ctx, stmt)
	return err
}
