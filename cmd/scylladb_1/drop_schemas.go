package scylladb_1

import (
	"context"
	"log"
	"time"

	"test-tls/infrastructure"
)

// ScylladbDropSchemas drops all tables used by the scylladb_1 benchmarks.
//
// It does NOT drop the keyspace itself, only the benchmark tables created in
// ScylladbCreateSchemas so the command is safe to re-run.
func ScylladbDropSchemas() {
	ctx := context.Background()

	session, cleanup, err := infrastructure.NewScyllaFromEnv(ctx)
	if err != nil {
		log.Fatalf("[scylladb_1] failed to create scylla session: %v", err)
	}
	defer cleanup()

	log.Printf("[scylladb_1] == Dropping ScyllaDB tables for benchmarks ==")

	// Keep this list in sync with ScylladbCreateSchemas.
	tables := []string{
		"organizations",
		"users",
		"groups",
		"org_memberships",
		"group_memberships",
		"resources",
		"resource_acl_by_resource",
		"resource_acl_by_subject",
		"user_resource_perms_by_user",
		"user_resource_perms_by_resource",
	}

	for _, tbl := range tables {
		cql := "DROP TABLE IF EXISTS " + tbl

		dropCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := session.Query(cql).WithContext(dropCtx).Exec(); err != nil {
			cancel()
			log.Fatalf("[scylladb_1] DropTable %s failed: %v", tbl, err)
		}
		cancel()

		log.Printf("[scylladb_1] Dropped table: %s", tbl)
	}

	log.Printf("[scylladb_1] ScyllaDB benchmark tables drop DONE")
}
