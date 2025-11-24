package cockroachdb_1

import (
	"context"
	"database/sql"
	"log"
	"time"

	"test-tls/infrastructure"
)

// CockroachdbDropSchemas drops all ACL-related tables used by cockroachdb_1 benchmarks.
// It does NOT drop the database or the public schema, only the tables we created.
func CockroachdbDropSchemas() {
	ctx := context.Background()

	db, cleanup, err := infrastructure.NewCockroachFromEnv(ctx)
	if err != nil {
		log.Fatalf("[cockroachdb_1] failed to create cockroachdb client: %v", err)
	}
	defer cleanup()

	log.Printf("[cockroachdb_1] == Dropping Cockroachdb ACL tables ==")

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
			log.Fatalf("[cockroachdb_1] executing %q failed: %v", stmt, err)
		}
		log.Printf("[cockroachdb_1] Executed: %s", stmt)
	}

	log.Printf("[cockroachdb_1] Cockroachdb ACL tables drop DONE")
}

func execWithTimeout(parent context.Context, db *sql.DB, stmt string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	_, err := db.ExecContext(ctx, stmt)
	return err
}
