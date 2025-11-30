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
-- NOTE (Scylla): multi-column secondary index (resource_id, relation) tidak didukung.
-- Juga tidak perlu, karena PRIMARY KEY ((resource_id), relation, subject_type, subject_id)
-- sudah mengoptimalkan pattern:
--   SELECT ... FROM resource_acl_by_resource
--   WHERE resource_id = ? AND relation = ?;
-- Jadi kita TIDAK membuat index tambahan di sini.

-- Index on org_memberships for role-based lookups
-- NOTE (Scylla): multi-column secondary index (org_id, role) juga tidak didukung.
-- Untuk pattern:
--   - point-check:  SELECT ... WHERE org_id = ? AND user_id = ?   --> PK sudah optimal.
--   - "all admins in org": SELECT ... WHERE org_id = ? AND role='admin'
--     ini sudah bisa memanfaatkan PK (org_id, user_id, role) dengan ALLOW FILTERING,
--     atau kalau butuh bener-bener kencang, sebaiknya bikin tabel denormalized khusus
--     org_admins_by_org(org_id, user_id) daripada secondary index.
-- Tidak ada CREATE INDEX di sini supaya tetap valid dan predictable di Scylla.

-- Index on group_memberships for role-based lookups
-- NOTE (Scylla): sama seperti org_memberships, multi-column index (group_id, role)
-- tidak valid. PRIMARY KEY (user_id, group_id, role) sudah mengoptimalkan pattern:
--   - "groups for user": WHERE user_id = ?;
-- Kalau butuh "users for group", gunakan tabel lain (mis. group_members_by_group)
-- atau pakai group_members_expanded + index user_id di atas. Tidak dibuat index
-- tambahan di sini.

-- Index on group_hierarchy for reverse lookups (find parent groups)
-- Ini single-column index yang valid untuk pattern:
--   SELECT parent_group_id,... FROM group_hierarchy WHERE child_group_id = ?;
CREATE INDEX IF NOT EXISTS idx_group_hierarchy_child
    ON group_hierarchy (child_group_id);

-- Index on user_resource_perms_by_user for permission checks by user and relation
-- NOTE (Scylla): PRIMARY KEY ((user_id), resource_id) sudah bikin semua lookup
--   WHERE user_id = ? AND resource_id = ?
-- sangat murah. Query pattern "semua resource yg can_manage=TRUE untuk user X"
-- cukup scan 1 partition (user_id) tanpa index tambahan; di Scylla ini sudah murah.
-- Multi-column secondary index (user_id, can_manage) tidak didukung, dan secondary
-- index di can_manage saja tidak menarik. Jadi tidak ada CREATE INDEX di sini.

-- Index on user_resource_perms_by_resource for permission checks by resource and relation
-- NOTE (Scylla): sama seperti di atas, PRIMARY KEY ((resource_id), user_id)
-- sudah cukup untuk:
--   WHERE resource_id = ? [AND user_id = ?]
-- Pattern "semua user yang can_manage resource X" tinggal scan 1 partition.
-- Multi-column index (resource_id, can_manage) tidak valid, jadi tidak dibuat
-- index tambahan di sini.
