-- cmd/scylladb/schemas.cql
-- ScyllaDB schema for RLS benchmark
-- Tables designed for fast permission reads (lookup operations)

-- Core entities
CREATE TABLE IF NOT EXISTS organizations (
    org_id int,
        PRIMARY KEY (org_id)
);

CREATE TABLE IF NOT EXISTS users (
        user_id int,
        org_id int,
        PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS groups (
        group_id int,
        org_id int,
        PRIMARY KEY (group_id)
);

-- Memberships
CREATE TABLE IF NOT EXISTS org_memberships (
        org_id int,
        user_id int,
        role text,
        PRIMARY KEY (org_id, user_id, role)
);

CREATE TABLE IF NOT EXISTS group_memberships (
        user_id int,
        group_id int,
        role text,
        PRIMARY KEY (user_id, group_id, role)
);

-- Group hierarchy (nested groups)
-- relation in {'member_group','manager_group'} where:
--  - 'member_group' means parent.member includes child.member
--  - 'manager_group' means parent.manager includes child.manager
CREATE TABLE IF NOT EXISTS group_hierarchy (
        parent_group_id int,
        child_group_id int,
        relation text,
        PRIMARY KEY ((parent_group_id, child_group_id), relation)
);

-- Precomputed effective group members (transitive closure expanded)
-- Materialized during load_data for O(1) lookups instead of recursive expansion
-- role in {'member', 'manager'} where:
--  - 'member' means user is effective member (direct or via hierarchy)
--  - 'manager' means user is effective manager (direct or via hierarchy)
CREATE TABLE IF NOT EXISTS group_members_expanded (
        group_id int,
        user_id int,
        role text,
        PRIMARY KEY ((group_id), user_id, role)
);

-- Resources
CREATE TABLE IF NOT EXISTS resources (
        resource_id int,
        org_id int,
        PRIMARY KEY (resource_id)
);

-- Direct ACL graph (Zanzibar-style edges, denormalized for both directions)
-- relation values: manager_user, viewer_user, manager_group, viewer_group
CREATE TABLE IF NOT EXISTS resource_acl_by_resource (
        resource_id int,
        relation text,
        subject_type text,
        subject_id int,
        PRIMARY KEY ((resource_id), relation, subject_type, subject_id)
);

CREATE TABLE IF NOT EXISTS resource_acl_by_subject (
        subject_type text,
        subject_id int,
        relation text,
        resource_id int,
        PRIMARY KEY ((subject_type, subject_id), relation, resource_id)
);

-- Fully compiled permissions closure (optimized for fast reads)
CREATE TABLE IF NOT EXISTS user_resource_perms_by_user (
        user_id int,
        resource_id int,
        can_manage boolean,
        can_view boolean,
        PRIMARY KEY ((user_id), resource_id)
);

CREATE TABLE IF NOT EXISTS user_resource_perms_by_resource (
        resource_id int,
        user_id int,
        can_manage boolean,
        can_view boolean,
        PRIMARY KEY ((resource_id), user_id)
);

-- ============================
-- Indexes for optimized queries
-- ============================

-- Index on group_members_expanded for reverse lookups (user -> groups)
-- This is a valid single-column secondary index and complements the PK (group_id,...)
CREATE INDEX IF NOT EXISTS idx_group_members_expanded_user
        ON group_members_expanded (user_id);

-- Index on resource_acl_by_resource for relation lookups
-- NOTE (Scylla): multi-column secondary index (resource_id, relation) is not supported.
-- Also not needed, because PRIMARY KEY ((resource_id), relation, subject_type, subject_id)
-- already optimizes the pattern:
--   SELECT ... FROM resource_acl_by_resource
--   WHERE resource_id = ? AND relation = ?;
-- So we do NOT create an additional index here.

-- Index on org_memberships for role-based lookups
-- NOTE (Scylla): multi-column secondary index (org_id, role) is also not supported.
-- For patterns:
--   - point-check:  SELECT ... WHERE org_id = ? AND user_id = ?   --> PK is already optimal.
--   - "all admins in org": SELECT ... WHERE org_id = ? AND role='admin'
--     this can already leverage PK (org_id, user_id, role) with ALLOW FILTERING,
--     or if truly high performance is needed, better to create a denormalized table
--     org_admins_by_org(org_id, user_id) rather than a secondary index.
-- No CREATE INDEX here to keep it valid and predictable in Scylla.

-- Index on group_memberships for role-based lookups
-- NOTE (Scylla): same as org_memberships, multi-column index (group_id, role)
-- is not valid. PRIMARY KEY (user_id, group_id, role) already optimizes pattern:
--   - "groups for user": WHERE user_id = ?;
-- If "users for group" is needed, use another table (e.g. group_members_by_group)
-- or use group_members_expanded + user_id index above. No additional index
-- created here.

-- Index on group_hierarchy for reverse lookups (find parent groups)
-- This is a valid single-column index for pattern:
--   SELECT parent_group_id,... FROM group_hierarchy WHERE child_group_id = ?;
CREATE INDEX IF NOT EXISTS idx_group_hierarchy_child
        ON group_hierarchy (child_group_id);

-- Index on user_resource_perms_by_user for permission checks by user and relation
-- NOTE (Scylla): PRIMARY KEY ((user_id), resource_id) already makes all lookups
--   WHERE user_id = ? AND resource_id = ?
-- very cheap. Query pattern "all resources where can_manage=TRUE for user X"
-- only scans 1 partition (user_id) without additional index; in Scylla this is already cheap.
-- Multi-column secondary index (user_id, can_manage) is not supported, and a secondary
-- index on can_manage alone is not appealing. So no CREATE INDEX here.

-- Index on user_resource_perms_by_resource for permission checks by resource and relation
-- NOTE (Scylla): same as above, PRIMARY KEY ((resource_id), user_id)
-- is sufficient for:
--   WHERE resource_id = ? [AND user_id = ?]
-- Pattern "all users who can_manage resource X" just scans 1 partition.
-- Multi-column index (resource_id, can_manage) is not valid, so no additional
-- index created here.

