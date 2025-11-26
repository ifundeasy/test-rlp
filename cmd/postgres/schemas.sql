-- cmd/postgres/schemas.sql
-- Schema for the RLS dataset (mirror of cmd/csv/load_data.go)

-- 1) Core entities

CREATE TABLE IF NOT EXISTS organizations (
    org_id   INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    org_id  INTEGER NOT NULL REFERENCES organizations(org_id)
);

CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER PRIMARY KEY,
    org_id   INTEGER NOT NULL REFERENCES organizations(org_id)
);

-- 2) Memberships

-- role in {'member','admin'}
CREATE TABLE IF NOT EXISTS org_memberships (
    org_id  INTEGER NOT NULL REFERENCES organizations(org_id),
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    role    TEXT    NOT NULL,
    PRIMARY KEY (org_id, user_id)
);

-- role currently always 'member' in the CSV, stored generically
CREATE TABLE IF NOT EXISTS group_memberships (
    group_id INTEGER NOT NULL REFERENCES groups(group_id),
    user_id  INTEGER NOT NULL REFERENCES users(user_id),
    role     TEXT    NOT NULL,
    PRIMARY KEY (group_id, user_id)
);

-- 2b) Group hierarchy (nested groups)
-- Each row means: parent includes child according to the named relation.
-- relation in {'member_group','manager_group'} where:
--  - 'member_group' means parent.member includes child.member
--  - 'manager_group' means parent.manager includes child.manager
CREATE TABLE IF NOT EXISTS group_hierarchy (
    parent_group_id INTEGER NOT NULL REFERENCES groups(group_id),
    child_group_id  INTEGER NOT NULL REFERENCES groups(group_id),
    relation        TEXT    NOT NULL,
    PRIMARY KEY (parent_group_id, child_group_id, relation)
);

-- 3) Resources

CREATE TABLE IF NOT EXISTS resources (
    resource_id INTEGER PRIMARY KEY,
    org_id      INTEGER NOT NULL REFERENCES organizations(org_id)
);

-- 4) ACL

-- subject_type in {'user','group'}
-- relation values expected from the CSV/loader:
--  - user subjects: 'manager_user', 'viewer_user'
--  - group subjects: 'manager_group', 'viewer_group'
-- (legacy/alternate values 'manager'/'viewer' are also accepted by queries)
CREATE TABLE IF NOT EXISTS resource_acl (
    resource_id  INTEGER NOT NULL REFERENCES resources(resource_id),
    subject_type TEXT    NOT NULL,
    subject_id   INTEGER NOT NULL,
    relation     TEXT    NOT NULL,
    PRIMARY KEY (resource_id, subject_type, subject_id, relation)
);

-- ============================
-- Indexes for common queries
-- ============================

-- org_memberships: lookup by user (org-admin / org-member)
CREATE INDEX IF NOT EXISTS idx_org_memberships_user
    ON org_memberships (user_id, org_id, role);

-- group_memberships: resolve groups for a user
CREATE INDEX IF NOT EXISTS idx_group_memberships_user
    ON group_memberships (user_id, group_id, role);

-- resources: mapping org -> resources
CREATE INDEX IF NOT EXISTS idx_resources_org
    ON resources (org_id, resource_id);

-- resource_acl: check permission for (resource, subject)
CREATE INDEX IF NOT EXISTS idx_resource_acl_by_resource_subject
    ON resource_acl (resource_id, subject_type, subject_id, relation);

-- resource_acl: list resources by subject
CREATE INDEX IF NOT EXISTS idx_resource_acl_by_subject
    ON resource_acl (subject_type, subject_id, relation, resource_id);

-- ----------------------------------------
-- Additional recommended indexes
-- ----------------------------------------

-- 1) Lookup permission by (resource, relation, subject)
CREATE INDEX IF NOT EXISTS idx_resource_acl_res_rel_type_subject
    ON resource_acl (resource_id, relation, subject_type, subject_id);

-- 2) Fast lookup of users by primary org (helps org-scoped queries/joins)
CREATE INDEX IF NOT EXISTS idx_users_org
    ON users (org_id);

-- ----------------------------------------
-- Materialized view: resolved user permissions
-- This precomputes effective permissions per (user,resource,relation)
-- It expands group ACLs into user entries using `group_memberships`.
-- Use `REFRESH MATERIALIZED VIEW user_resource_permissions;` to populate.
-- For large datasets consider refreshing concurrently after creating the unique index.
-- ----------------------------------------

-- Materialized view: resolved user permissions (handles nested groups)
-- This precomputes effective permissions per (user,resource,relation).
-- It expands group ACLs into user entries using `group_memberships` and
-- recursively follows `group_hierarchy` edges to support nested groups.
-- Mapping rules applied:
--  - resource_acl subject_type='user' with 'manager_user' -> relation 'manager'
--  - resource_acl subject_type='user' with 'viewer_user'  -> relation 'viewer'
--  - resource_acl subject_type='group' with 'manager_group' -> expand to effective managers -> 'manager'
--  - resource_acl subject_type='group' with 'viewer_group'  -> expand to effective members -> 'viewer'
--  - managers are included as members (manager => member)
-- Use `REFRESH MATERIALIZED VIEW user_resource_permissions;` to populate.
CREATE MATERIALIZED VIEW IF NOT EXISTS user_resource_permissions AS
WITH RECURSIVE
-- effective managers per group: start with direct_manager users
mgr_users AS (
  SELECT gm.group_id AS root_group, gm.user_id
  FROM group_memberships gm
  WHERE gm.role = 'direct_manager'

  UNION ALL

  -- parent.manager includes child.manager when relation = 'manager_group'
  SELECT gh.parent_group_id AS root_group, mu.user_id
  FROM group_hierarchy gh
  JOIN mgr_users mu ON gh.child_group_id = mu.root_group
  WHERE gh.relation = 'manager_group'
),

-- effective members per group: include direct_member users, recursively include child.member
-- and include managers (managers are also members)
member_users AS (
    -- non-recursive base: direct members + managers (managers are also members)
    SELECT gm.group_id AS root_group, gm.user_id
    FROM group_memberships gm
    WHERE gm.role = 'direct_member'

    UNION

    SELECT m.root_group, m.user_id FROM mgr_users m

    UNION ALL

    -- recursive term: parent.member includes child.member when relation = 'member_group'
    SELECT gh.parent_group_id AS root_group, mu.user_id
    FROM group_hierarchy gh
    JOIN member_users mu ON gh.child_group_id = mu.root_group
    WHERE gh.relation = 'member_group'
)

-- Now produce permission rows
SELECT r.resource_id, r.org_id, ra.subject_id AS user_id,
  CASE WHEN ra.relation LIKE 'manager%' THEN 'manager' ELSE 'viewer' END AS relation
FROM resource_acl ra
JOIN resources r ON r.resource_id = ra.resource_id
WHERE ra.subject_type = 'user' AND ra.relation IN ('manager_user', 'viewer_user', 'manager', 'viewer')

UNION

-- group ACL -> managers
SELECT r.resource_id, r.org_id, mu.user_id, 'manager' AS relation
FROM resource_acl ra
JOIN resources r ON r.resource_id = ra.resource_id
JOIN mgr_users mu ON ra.subject_type = 'group' AND ra.subject_id = mu.root_group
WHERE ra.relation = 'manager_group' OR ra.relation = 'manager'

UNION

-- group ACL -> viewers (expand to effective members; managers included by member_users)
SELECT r.resource_id, r.org_id, mem.user_id, 'viewer' AS relation
FROM resource_acl ra
JOIN resources r ON r.resource_id = ra.resource_id
JOIN member_users mem ON ra.subject_type = 'group' AND ra.subject_id = mem.root_group
WHERE ra.relation = 'viewer_group' OR ra.relation = 'viewer';

-- Ensure uniqueness (the UNION above deduplicates, but a unique index
-- allows CONCURRENT refreshes and fast lookups)
CREATE UNIQUE INDEX IF NOT EXISTS uq_user_resource_permissions
    ON user_resource_permissions (resource_id, user_id, relation);

-- Useful access patterns on the materialized view
CREATE INDEX IF NOT EXISTS idx_urp_user_rel_res
    ON user_resource_permissions (user_id, relation, resource_id);

CREATE INDEX IF NOT EXISTS idx_urp_org_user_rel
    ON user_resource_permissions (org_id, user_id, relation, resource_id);

-- Indexes to support fast group expansion and lookups
CREATE INDEX IF NOT EXISTS idx_group_hierarchy_parent
    ON group_hierarchy (parent_group_id, relation, child_group_id);

CREATE INDEX IF NOT EXISTS idx_group_hierarchy_child
    ON group_hierarchy (child_group_id, relation, parent_group_id);

-- Convenience function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_user_resource_permissions()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  REFRESH MATERIALIZED VIEW user_resource_permissions;
END;
$$;
