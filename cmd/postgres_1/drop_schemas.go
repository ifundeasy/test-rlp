package postgres_1

import (
	"context"
	"database/sql"
	"log"
	"time"

	"test-tls/infrastructure"
)

// PostgresDropSchemas drops all ACL-related tables used by postgres_1 benchmarks.
// It does NOT drop the database or the public schema, only the tables we created.
func PostgresDropSchemas() {
	ctx := context.Background()

	db, cleanup, err := infrastructure.NewPostgresFromEnv(ctx)
	if err != nil {
		log.Fatalf("[postgres_1] failed to create postgres client: %v", err)
	}
	defer cleanup()

	log.Printf("[postgres_1] == Dropping Postgres ACL tables ==")

	// Drop order: children -> parents to avoid FK issues, plus CASCADE for safety.
	statements := []string{
		`DROP TABLE IF EXISTS resource_acl CASCADE`,
		`DROP TABLE IF EXISTS resources CASCADE`,
		`DROP TABLE IF EXISTS group_memberships CASCADE`,
		`DROP TABLE IF EXISTS org_memberships CASCADE`,
		`DROP TABLE IF EXISTS groups CASCADE`,
		`DROP TABLE IF EXISTS users CASCADE`,
		`DROP TABLE IF EXISTS organizations CASCADE`,
	}

	for _, stmt := range statements {
		if err := execWithTimeout(ctx, db, stmt, 30*time.Second); err != nil {
			log.Fatalf("[postgres_1] executing %q failed: %v", stmt, err)
		}
		log.Printf("[postgres_1] Executed: %s", stmt)
	}

	log.Printf("[postgres_1] Postgres ACL tables drop DONE")
}

func execWithTimeout(parent context.Context, db *sql.DB, stmt string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	_, err := db.ExecContext(ctx, stmt)
	return err
}
