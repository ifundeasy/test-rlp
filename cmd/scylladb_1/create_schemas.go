package scylladb_1

import (
	"context"
	"log"
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
		log.Fatalf("[scylladb_1] NewScyllaFromEnv failed: %v", err)
	}
	defer cleanup()

	log.Printf("[scylladb_1] == Creating ScyllaDB schemas ==")

	// We keep replication/KS setup outside this function; here we only manage tables.
	queries := []string{
		// Core entities
		`CREATE TABLE IF NOT EXISTS organizations (
			org_id int,
			PRIMARY KEY (org_id)
		)`,
		`CREATE TABLE IF NOT EXISTS users (
			user_id int,
			org_id int,
			PRIMARY KEY (user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS groups (
			group_id int,
			org_id int,
			PRIMARY KEY (group_id)
		)`,

		// Memberships
		`CREATE TABLE IF NOT EXISTS org_memberships (
			org_id int,
			user_id int,
			role text,
			PRIMARY KEY (org_id, user_id, role)
		)`,
		`CREATE TABLE IF NOT EXISTS group_memberships (
			user_id int,
			group_id int,
			role text,
			PRIMARY KEY (user_id, group_id)
		)`,

		// Resources
		`CREATE TABLE IF NOT EXISTS resources (
			resource_id int,
			org_id int,
			PRIMARY KEY (resource_id)
		)`,

		// Direct ACL graph (Zanzibar-style edges, denormalized for both directions)
		`CREATE TABLE IF NOT EXISTS resource_acl_by_resource (
			resource_id int,
			relation text,
			subject_type text,
			subject_id int,
			PRIMARY KEY ((resource_id), relation, subject_type, subject_id)
		)`,
		`CREATE TABLE IF NOT EXISTS resource_acl_by_subject (
			subject_type text,
			subject_id int,
			relation text,
			resource_id int,
			PRIMARY KEY ((subject_type, subject_id), relation, resource_id)
		)`,

		// Fully compiled permissions closure
		`CREATE TABLE IF NOT EXISTS user_resource_perms_by_user (
			user_id int,
			resource_id int,
			can_manage boolean,
			can_view boolean,
			PRIMARY KEY ((user_id), resource_id)
		)`,
		`CREATE TABLE IF NOT EXISTS user_resource_perms_by_resource (
			resource_id int,
			user_id int,
			can_manage boolean,
			can_view boolean,
			PRIMARY KEY ((resource_id), user_id)
		)`,
	}

	for _, q := range queries {
		ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := session.Query(q).WithContext(ctxTimeout).Exec(); err != nil {
			cancel()
			log.Fatalf("[scylladb_1] create_schemas: exec failed for %q: %v", q, err)
		}
		cancel()
	}

	log.Printf("[scylladb_1] ScyllaDB schemas created successfully.")
}
