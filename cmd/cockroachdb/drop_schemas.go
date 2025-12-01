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

	start := time.Now()
	log.Printf("[cockroachdb] == Starting CockroachDB drop schemas ==")

	// Drop indexes explicitly, then materialized view, then tables (children first).
	statements := []string{
		// Indexes
		`DROP INDEX IF EXISTS uq_user_resource_permissions`,
		`DROP INDEX IF EXISTS idx_urp_user_rel_res`,
		`DROP INDEX IF EXISTS idx_urp_org_user_rel`,
		`DROP INDEX IF EXISTS idx_group_hierarchy_parent`,
		`DROP INDEX IF EXISTS idx_group_hierarchy_child`,
		`DROP INDEX IF EXISTS idx_resource_acl_res_rel_type_subject`,
		`DROP INDEX IF EXISTS idx_resource_acl_by_subject`,
		`DROP INDEX IF EXISTS idx_resource_acl_by_resource_subject`,
		`DROP INDEX IF EXISTS idx_resources_org`,
		`DROP INDEX IF EXISTS idx_group_memberships_user`,
		`DROP INDEX IF EXISTS idx_org_memberships_user`,
		`DROP INDEX IF EXISTS idx_users_org`,

		// Materialized view (Cockroach supports this; no function present)
		`DROP MATERIALIZED VIEW IF EXISTS user_resource_permissions`,

		// Tables (children before parents)
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

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[cockroachdb] CockroachDB drop schemas DONE: elapsed=%s", elapsed)
}

func execWithTimeout(parent context.Context, db *sql.DB, stmt string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	_, err := db.ExecContext(ctx, stmt)
	return err
}
