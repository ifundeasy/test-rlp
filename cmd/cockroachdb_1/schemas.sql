-- cmd/cockroachdb_1/schemas.sql
-- Schema untuk dataset RLS (mirror dari cmd/csv/load_data.go)

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

-- role saat ini selalu 'member' di CSV, tapi disimpan generic
CREATE TABLE IF NOT EXISTS group_memberships (
    group_id INTEGER NOT NULL REFERENCES groups(group_id),
    user_id  INTEGER NOT NULL REFERENCES users(user_id),
    role     TEXT    NOT NULL,
    PRIMARY KEY (group_id, user_id)
);

-- 3) Resources

CREATE TABLE IF NOT EXISTS resources (
    resource_id INTEGER PRIMARY KEY,
    org_id      INTEGER NOT NULL REFERENCES organizations(org_id)
);

-- 4) ACL

-- subject_type in {'user','group'}
-- relation in {'manager','viewer'}
CREATE TABLE IF NOT EXISTS resource_acl (
    resource_id  INTEGER NOT NULL REFERENCES resources(resource_id),
    subject_type TEXT    NOT NULL,
    subject_id   INTEGER NOT NULL,
    relation     TEXT    NOT NULL,
    PRIMARY KEY (resource_id, subject_type, subject_id, relation)
);

-- ============================
-- Indexes untuk query umum
-- ============================

-- org_memberships: lookup by user (org-admin / org-member)
CREATE INDEX IF NOT EXISTS idx_org_memberships_user
    ON org_memberships (user_id, org_id, role);

-- group_memberships: resolve groups untuk user
CREATE INDEX IF NOT EXISTS idx_group_memberships_user
    ON group_memberships (user_id, group_id, role);

-- resources: mapping org -> resources
CREATE INDEX IF NOT EXISTS idx_resources_org
    ON resources (org_id, resource_id);

-- resource_acl: cek permission untuk (resource, subject)
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
-- ----------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS user_resource_permissions AS
-- direct user ACLs
SELECT r.resource_id, r.org_id, ra.subject_id AS user_id, ra.relation
FROM resource_acl ra
JOIN resources r ON r.resource_id = ra.resource_id
WHERE ra.subject_type = 'user'

UNION

-- group ACLs expanded to users
SELECT r.resource_id, r.org_id, gm.user_id, ra.relation
FROM resource_acl ra
JOIN resources r ON r.resource_id = ra.resource_id
JOIN group_memberships gm ON ra.subject_type = 'group' AND ra.subject_id = gm.group_id;

-- Useful access patterns on the materialized view
CREATE INDEX IF NOT EXISTS idx_urp_user_rel_res
    ON user_resource_permissions (user_id, relation, resource_id);

CREATE INDEX IF NOT EXISTS idx_urp_org_user_rel
    ON user_resource_permissions (org_id, user_id, relation, resource_id);
