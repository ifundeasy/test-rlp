package scylladb

import (
	"context"
	"log"
	"time"

	"test-tls/infrastructure"
)

// ScylladbDropSchemas drops all tables used by the scylladb benchmarks.
//
// It does NOT drop the keyspace itself, only the benchmark tables created in
// ScylladbCreateSchemas so the command is safe to re-run.
func ScylladbDropSchemas() {
	ctx := context.Background()

	session, cleanup, err := infrastructure.NewScyllaFromEnv(ctx)
	if err != nil {
		log.Fatalf("[scylladb] failed to create scylla session: %v", err)
	}
	defer cleanup()

	start := time.Now()
	log.Printf("[scylladb] == Starting ScyllaDB drop schemas ==")

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
			log.Fatalf("[scylladb] DropTable %s failed: %v", tbl, err)
		}
		cancel()

		log.Printf("[scylladb] Dropped table: %s", tbl)
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[scylladb] ScyllaDB drop schemas DONE: elapsed=%s", elapsed)
}
