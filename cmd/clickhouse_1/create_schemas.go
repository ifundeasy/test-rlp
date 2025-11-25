package clickhouse_1

import (
	"context"
	"database/sql"
	"log"
	"time"

	"test-tls/infrastructure"
)

// ClickhouseCreateSchemas creates all tables and indexes in ClickHouse
// optimized for RLS-style "check" and "list" queries that walk the
// org -> group -> user -> resource chains.
func ClickhouseCreateSchemas() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, cleanup, err := infrastructure.NewClickhouseFromEnv(ctx)
	if err != nil {
		log.Fatalf("[clickhouse_1] create_schemas: connect failed: %v", err)
	}
	defer cleanup()

	stmts := []string{
		// 1) organizations: small dimension
		`CREATE TABLE IF NOT EXISTS organizations (
			org_id UInt32
		) ENGINE = MergeTree
		ORDER BY (org_id)`,

		// 2) users: global users, primary_org_id mostly for analysis
		`CREATE TABLE IF NOT EXISTS users (
			user_id UInt32,
			primary_org_id UInt32
		) ENGINE = MergeTree
		ORDER BY (user_id)`,

		// 3) groups: per org
		`CREATE TABLE IF NOT EXISTS groups (
			group_id UInt32,
			org_id UInt32
		) ENGINE = MergeTree
		PARTITION BY org_id
		ORDER BY (org_id, group_id)`,

		// 4) org_memberships:
		//    used heavily for org admin / membership checks
		`CREATE TABLE IF NOT EXISTS org_memberships (
			org_id UInt32,
			user_id UInt32,
			role Enum8('member' = 1, 'admin' = 2)
		) ENGINE = MergeTree
		PARTITION BY org_id
		ORDER BY (org_id, user_id, role)`,

		`CREATE INDEX IF NOT EXISTS idx_org_memberships_user
			ON org_memberships (user_id)
			TYPE minmax GRANULARITY 1`,

		// 5) group_memberships:
		//    used for user->groups and group->users
		`CREATE TABLE IF NOT EXISTS group_memberships (
			group_id UInt32,
			user_id UInt32,
			role Enum8('member' = 1, 'admin' = 2)
		) ENGINE = MergeTree
		ORDER BY (user_id, group_id, role)`,

		`CREATE INDEX IF NOT EXISTS idx_group_memberships_group
			ON group_memberships (group_id)
			TYPE minmax GRANULARITY 1`,

		// 6) resources:
		//    core for org -> resources and direct resource lookup
		`CREATE TABLE IF NOT EXISTS resources (
			resource_id UInt32,
			org_id UInt32
		) ENGINE = MergeTree
		PARTITION BY org_id
		ORDER BY (org_id, resource_id)`,

		`CREATE INDEX IF NOT EXISTS idx_resources_resource
			ON resources (resource_id)
			TYPE minmax GRANULARITY 1`,

		// 7) resource_acl:
		//    Zanzibar-style edges; org_id is denormalized for tenant-scoping
		`CREATE TABLE IF NOT EXISTS resource_acl (
			resource_id UInt32,
			org_id UInt32,
			subject_type Enum8('user' = 1, 'group' = 2),
			subject_id UInt32,
			relation Enum8('viewer' = 1, 'manager' = 2)
		) ENGINE = MergeTree
		PARTITION BY org_id
		ORDER BY (org_id, resource_id, relation, subject_type, subject_id)`,

		// Projection for subject-centric access (list all resources for a user/group in an org).
		`ALTER TABLE resource_acl
			ADD PROJECTION IF NOT EXISTS resource_acl_by_subject
			(
				SELECT
					org_id,
					subject_type,
					subject_id,
					relation,
					resource_id
				ORDER BY (org_id, subject_type, subject_id, relation, resource_id)
			)`,

		// Bloom filter index for (subject_type, subject_id) equality checks.
		`CREATE INDEX IF NOT EXISTS idx_resource_acl_subject_bf
			ON resource_acl (subject_type, subject_id)
			TYPE bloom_filter
			GRANULARITY 8`,

		// Minmax index for resource_id inside partitions.
		`CREATE INDEX IF NOT EXISTS idx_resource_acl_resource_minmax
			ON resource_acl (resource_id)
			TYPE minmax
			GRANULARITY 1`,

		// 8) user_resource_permissions: flattened per-user permissions
		// This materialized view expands group memberships and org-based
		// permissions into a simple (user_id, resource_id, relation) table
		// optimized for point-checks and resource lookups by user.
		`CREATE TABLE IF NOT EXISTS user_resource_permissions (
			resource_id UInt32,
			user_id UInt32,
			relation Enum8('viewer' = 1, 'manager' = 2)
		) ENGINE = MergeTree
		PARTITION BY intDiv(user_id, 10000)
		ORDER BY (user_id, resource_id, relation)`,

		// Materialized view that populates user_resource_permissions from
		// resource_acl, group_memberships and org_memberships. It should be
		// created before bulk-loading the underlying tables so inserts
		// automatically populate the flattened table.
		`CREATE MATERIALIZED VIEW IF NOT EXISTS user_resource_permissions_mv
		TO user_resource_permissions AS
		SELECT
			ra.resource_id AS resource_id,
			ra.subject_id AS user_id,
			ra.relation AS relation
		FROM resource_acl AS ra
		WHERE ra.subject_type = 'user'
		UNION ALL
		SELECT
			ra.resource_id AS resource_id,
			gm.user_id AS user_id,
			ra.relation AS relation
		FROM resource_acl AS ra
		JOIN group_memberships AS gm
			ON gm.group_id = ra.subject_id
		WHERE ra.subject_type = 'group'
		UNION ALL
		-- org-admins become managers
		SELECT
			r.resource_id AS resource_id,
			om.user_id AS user_id,
			'manager' AS relation
		FROM resources AS r
		JOIN org_memberships AS om
			ON om.org_id = r.org_id
		WHERE om.role = 'admin'
		UNION ALL
		-- org-members (and admins) become viewers
		SELECT
			r.resource_id AS resource_id,
			om.user_id AS user_id,
			'viewer' AS relation
		FROM resources AS r
		JOIN org_memberships AS om
			ON om.org_id = r.org_id
		WHERE om.role = 'member' OR om.role = 'admin'`,
	}

	for _, stmt := range stmts {
		execWithTimeout(ctx, db, stmt, 30*time.Second)
	}

	log.Println("[clickhouse_1] Schemas created successfully.")
}

func execWithTimeout(parent context.Context, db *sql.DB, stmt string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	if _, err := db.ExecContext(ctx, stmt); err != nil {
		log.Fatalf("[clickhouse_1] executing %q failed: %v", stmt, err)
	}
}
