-- ClickHouse schema for RLP benchmarks
-- Supports nested groups via group_hierarchy + group_members_expanded

CREATE TABLE IF NOT EXISTS organizations (
    org_id UInt32
) ENGINE = MergeTree
ORDER BY (org_id);

CREATE TABLE IF NOT EXISTS users (
    user_id UInt32,
    primary_org_id UInt32
) ENGINE = MergeTree
ORDER BY (user_id);

CREATE TABLE IF NOT EXISTS groups (
    group_id UInt32,
    org_id UInt32
) ENGINE = MergeTree
PARTITION BY org_id
ORDER BY (org_id, group_id);

CREATE TABLE IF NOT EXISTS org_memberships (
    org_id UInt32,
    user_id UInt32,
    role Enum8('member' = 1, 'admin' = 2)
) ENGINE = MergeTree
PARTITION BY org_id
ORDER BY (org_id, user_id, role);

CREATE INDEX IF NOT EXISTS idx_org_memberships_user
    ON org_memberships (user_id)
    TYPE minmax GRANULARITY 1;

CREATE TABLE IF NOT EXISTS group_memberships (
    group_id UInt32,
    user_id UInt32,
    role Enum8('member' = 1, 'manager' = 2)
) ENGINE = MergeTree
ORDER BY (user_id, group_id, role);

CREATE INDEX IF NOT EXISTS idx_group_memberships_group
    ON group_memberships (group_id)
    TYPE minmax GRANULARITY 1;

-- support nested groups
CREATE TABLE IF NOT EXISTS group_hierarchy (
    parent_group_id UInt32,
    child_group_id UInt32,
    relation Enum8('member_group' = 1, 'manager_group' = 2)
) ENGINE = MergeTree
ORDER BY (parent_group_id, child_group_id);

-- precomputed transitive closure for nested groups.. populated by loader
CREATE TABLE IF NOT EXISTS group_members_expanded (
    group_id UInt32,
    user_id UInt32,
    role Enum8('member' = 1, 'manager' = 2)
) ENGINE = MergeTree
ORDER BY (group_id, user_id, role);

CREATE INDEX IF NOT EXISTS idx_group_members_expanded_user
    ON group_members_expanded (user_id)
    TYPE minmax GRANULARITY 1;

CREATE TABLE IF NOT EXISTS resources (
    resource_id UInt32,
    org_id UInt32
) ENGINE = MergeTree
PARTITION BY org_id
ORDER BY (org_id, resource_id);

CREATE INDEX IF NOT EXISTS idx_resources_resource
    ON resources (resource_id)
    TYPE minmax GRANULARITY 1;

CREATE TABLE IF NOT EXISTS resource_acl (
    resource_id UInt32,
    org_id UInt32,
    subject_type Enum8('user' = 1, 'group' = 2),
    subject_id UInt32,
    relation Enum8('viewer' = 1, 'manager' = 2)
) ENGINE = MergeTree
PARTITION BY org_id
ORDER BY (org_id, resource_id, relation, subject_type, subject_id);

ALTER TABLE resource_acl
    ADD PROJECTION IF NOT EXISTS resource_acl_by_subject
    (
        SELECT
            org_id,
            subject_type,
            subject_id,
            relation,
            resource_id
        ORDER BY (org_id, subject_type, subject_id, relation, resource_id)
    );

CREATE INDEX IF NOT EXISTS idx_resource_acl_subject_bf
    ON resource_acl (subject_type, subject_id)
    TYPE bloom_filter
    GRANULARITY 8;

CREATE INDEX IF NOT EXISTS idx_resource_acl_resource_minmax
    ON resource_acl (resource_id)
    TYPE minmax
    GRANULARITY 1;

CREATE TABLE IF NOT EXISTS user_resource_permissions (
    resource_id UInt32,
    user_id UInt32,
    relation Enum8('viewer' = 1, 'manager' = 2)
) ENGINE = MergeTree
PARTITION BY intDiv(user_id, 10000)
ORDER BY (user_id, resource_id, relation);

-- Materialized view populates user_resource_permissions. When subject is
-- a group, it joins into the precomputed group_members_expanded table so
-- nested groups are supported without recursive runtime expansion.
CREATE MATERIALIZED VIEW IF NOT EXISTS user_resource_permissions_mv
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
    gme.user_id AS user_id,
    ra.relation AS relation
FROM resource_acl AS ra
JOIN group_members_expanded AS gme
    ON gme.group_id = ra.subject_id
WHERE ra.subject_type = 'group'
UNION ALL
SELECT
    r.resource_id AS resource_id,
    om.user_id AS user_id,
    'manager' AS relation
FROM resources AS r
JOIN org_memberships AS om
    ON om.org_id = r.org_id
WHERE om.role = 'admin'
UNION ALL
SELECT
    r.resource_id AS resource_id,
    om.user_id AS user_id,
    'viewer' AS relation
FROM resources AS r
JOIN org_memberships AS om
    ON om.org_id = r.org_id
WHERE om.role = 'member' OR om.role = 'admin';
