package scylladb

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"test-tls/infrastructure"
)

// ScylladbCreateSchemas creates the CQL tables used for the ScyllaDB benchmarks.
//
// Design goals:
//
//   - Optimize for the two main RLS patterns:
//     1) "check"  : can user U manage/view resource R?
//     2) "list"   : which resources can user U manage/view?
//   - Avoid cross-partition joins at read time by denormalizing where it helps.
//   - Preserve the same logical semantics as the relational/Zanzibar model.
//
// Tables:
//
//	organizations(org_id)
//
//	users(user_id, org_id)
//
//	groups(group_id, org_id)
//
//	org_memberships(org_id, user_id, role)
//	  - PK: (org_id, user_id, role) so we can cheaply test "is U admin/member of org O?"
//
//	group_memberships(user_id, group_id, role)
//	  - PK: (user_id, group_id) so we can list "all groups for user U" without ALLOW FILTERING.
//
//	resources(resource_id, org_id)
//	  - PK: (resource_id) so we can resolve a resource's org.
//
//	resource_acl_by_resource(resource_id, relation, subject_type, subject_id)
//	  - Direct ACL edges as in resource_acl.csv, partitioned by resource.
//	  - Lets us expand "who directly manages/views R?" or check a specific subject on R.
//
//	resource_acl_by_subject(subject_type, subject_id, relation, resource_id)
//	  - Same ACL edges, but partitioned by subject for subject-centric scans.
//
//	user_resource_perms_by_user(user_id, resource_id, can_manage, can_view)
//	  - Fully compiled permission closure per user -> resources.
//	  - Optimized for list operations (RLS list).
//
//	user_resource_perms_by_resource(resource_id, user_id, can_manage, can_view)
//	  - Same closure but partitioned by resource.
//	  - Optimized for check operations (RLS check).
func ScylladbCreateSchemas() {
	ctx := context.Background()

	session, cleanup, err := infrastructure.NewScyllaFromEnv(ctx)
	if err != nil {
		log.Fatalf("[scylladb] NewScyllaFromEnv failed: %v", err)
	}
	defer cleanup()

	log.Printf("[scylladb] == Creating ScyllaDB schemas ==")

	// Read schemas from schemas.cql file
	schemaFile := "cmd/scylladb/schemas.cql"
	content, err := os.ReadFile(schemaFile)
	if err != nil {
		log.Fatalf("[scylladb] read schemas.cql failed: %v", err)
	}

	// Parse CQL statements (split by semicolon and filter out comments)
	lines := strings.Split(string(content), "\n")
	var currentStatement strings.Builder
	queries := []string{}

	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		// Skip empty lines and comment lines
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "--") {
			continue
		}

		// Append line to current statement
		currentStatement.WriteString(line)
		currentStatement.WriteString("\n")

		// If line ends with semicolon, we have a complete statement
		if strings.HasSuffix(trimmedLine, ";") {
			stmt := strings.TrimSpace(currentStatement.String())
			stmt = strings.TrimSuffix(stmt, ";")
			stmt = strings.TrimSpace(stmt)
			if stmt != "" {
				queries = append(queries, stmt)
			}
			currentStatement.Reset()
		}
	}

	for _, q := range queries {
		ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := session.Query(q).WithContext(ctxTimeout).Exec(); err != nil {
			cancel()
			log.Fatalf("[scylladb] create_schemas: exec failed for query: %v\n%s", err, q)
		}
		cancel()
	}

	log.Printf("[scylladb] ScyllaDB schemas created successfully.")
}
