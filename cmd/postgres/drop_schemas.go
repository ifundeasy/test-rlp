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

	start := time.Now()
	log.Printf("[postgres] == Starting Postgres drop schemas ==")

	// Drop indexes explicitly (although dropping tables/views removes their indexes, this ensures clean state when partial objects exist).
	indexDrops := []string{
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
	}

	for _, stmt := range indexDrops {
		if err := execWithTimeout(ctx, db, stmt, 30*time.Second); err != nil {
			log.Printf("[postgres] warning: executing %q failed: %v", stmt, err)
			continue
		}
		log.Printf("[postgres] Executed: %s", stmt)
	}

	// Drop materialized view and function.
	objDrops := []string{
		`DROP MATERIALIZED VIEW IF EXISTS user_resource_permissions`,
		`DROP FUNCTION IF EXISTS refresh_user_resource_permissions()`,
	}
	for _, stmt := range objDrops {
		if err := execWithTimeout(ctx, db, stmt, 60*time.Second); err != nil {
			log.Printf("[postgres] warning: executing %q failed: %v", stmt, err)
			continue
		}
		log.Printf("[postgres] Executed: %s", stmt)
	}

	// Drop tables (children first), using CASCADE for safety.
	tableDrops := []string{
		`DROP TABLE IF EXISTS resource_acl CASCADE`,
		`DROP TABLE IF EXISTS resources CASCADE`,
		`DROP TABLE IF EXISTS group_memberships CASCADE`,
		`DROP TABLE IF EXISTS group_hierarchy CASCADE`,
		`DROP TABLE IF EXISTS org_memberships CASCADE`,
		`DROP TABLE IF EXISTS groups CASCADE`,
		`DROP TABLE IF EXISTS users CASCADE`,
		`DROP TABLE IF EXISTS organizations CASCADE`,
	}
	for _, stmt := range tableDrops {
		if err := execWithTimeout(ctx, db, stmt, 60*time.Second); err != nil {
			log.Printf("[postgres] warning: executing %q failed: %v", stmt, err)
			continue
		}
		log.Printf("[postgres] Executed: %s", stmt)
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[postgres] Postgres drop schemas DONE: elapsed=%s", elapsed)
}

func execWithTimeout(parent context.Context, db *sql.DB, stmt string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	_, err := db.ExecContext(ctx, stmt)
	return err
}
