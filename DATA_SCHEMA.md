# Row-Level Permission Benchmark – Formal Schema Compendium

This document provides a formally structured, engine‑specific description of the eight distinct schema implementations used in the Row‑Level Permission (RLP) benchmark. Each database engine projects the same logical CSV source model into a physical form optimized for its execution characteristics. Diagrams are rendered in Mermaid. For relational / tabular engines and document stores, `erDiagram` is used; for Authzed (graph / relationship model) `flowchart` is used. Each diagram embeds index (or equivalent optimization) metadata as pseudo‑attributes prefixed with `index:` (or `projection:` / `mv:` where applicable).

The logical source dataset (CSV) comprises: organizations, users, groups, group hierarchy, organization memberships, group memberships, resources, resource ACL, and derived permission closures. Engines differ mainly in their handling of denormalization, transitive expansion, and permission closure materialization.

---

## 1. PostgreSQL Schema
Source: `cmd/postgres/schemas.sql`

Features: Fully normalized core entities, Zanzibar‑style ACL edge table, recursive materialized view (`user_resource_permissions`) for nested groups and group role propagation, comprehensive B‑tree indexing for common access patterns.

```mermaid
erDiagram
  ORGANIZATIONS {
    int org_id PK
    index: PK(org_id)
  }
  USERS {
    int user_id PK
    int org_id FK
    index: PK(user_id)
    index: idx_users_org(org_id)
  }
  GROUPS {
    int group_id PK
    int org_id FK
    index: PK(group_id)
  }
  ORG_MEMBERSHIPS {
    int org_id FK
    int user_id FK
    text role
    index: PK(org_id,user_id)
    index: idx_org_memberships_user(user_id,org_id,role)
  }
  GROUP_MEMBERSHIPS {
    int group_id FK
    int user_id FK
    text role
    index: PK(group_id,user_id)
    index: idx_group_memberships_user(user_id,group_id,role)
  }
  GROUP_HIERARCHY {
    int parent_group_id FK
    int child_group_id FK
    text relation
    index: PK(parent_group_id,child_group_id,relation)
    index: idx_group_hierarchy_parent(parent_group_id,relation,child_group_id)
    index: idx_group_hierarchy_child(child_group_id,relation,parent_group_id)
  }
  RESOURCES {
    int resource_id PK
    int org_id FK
    index: PK(resource_id)
    index: idx_resources_org(org_id,resource_id)
  }
  RESOURCE_ACL {
    int resource_id FK
    text subject_type
    int subject_id
    text relation
    index: PK(resource_id,subject_type,subject_id,relation)
    index: idx_resource_acl_by_resource_subject(resource_id,subject_type,subject_id,relation)
    index: idx_resource_acl_by_subject(subject_type,subject_id,relation,resource_id)
    index: idx_resource_acl_res_rel_type_subject(resource_id,relation,subject_type,subject_id)
  }
  USER_RESOURCE_PERMISSIONS {
    int resource_id FK
    int org_id FK
    int user_id FK
    text relation
    mv: recursive expansion of groups & hierarchy
    index: uq_user_resource_permissions(resource_id,user_id,relation)
    index: idx_urp_user_rel_res(user_id,relation,resource_id)
    index: idx_urp_org_user_rel(org_id,user_id,relation,resource_id)
  }
  ORGANIZATIONS ||--o{ USERS : org_id
  ORGANIZATIONS ||--o{ GROUPS : org_id
  ORGANIZATIONS ||--o{ RESOURCES : org_id
  ORGANIZATIONS ||--o{ ORG_MEMBERSHIPS : org_id
  USERS ||--o{ ORG_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_MEMBERSHIPS : group_id
  USERS ||--o{ GROUP_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_HIERARCHY : parent_group_id
  GROUPS ||--o{ GROUP_HIERARCHY : child_group_id
  RESOURCES ||--o{ RESOURCE_ACL : resource_id
  RESOURCES ||--o{ USER_RESOURCE_PERMISSIONS : resource_id
  USERS ||--o{ USER_RESOURCE_PERMISSIONS : user_id
```

---

## 2. CockroachDB Schema
Source: `cmd/cockroachdb/schemas.sql`

Logical structure mirrors PostgreSQL verbatim (including recursive CTE materialized view). Differences: absence of PL/pgSQL refresh function; manual `REFRESH MATERIALIZED VIEW` required. Index set identical to PostgreSQL.

```mermaid
erDiagram
  ORGANIZATIONS {
    int org_id PK
    index: PK(org_id)
  }
  USERS {
    int user_id PK
    int org_id FK
    index: PK(user_id)
    index: idx_users_org(org_id)
  }
  GROUPS {
    int group_id PK
    int org_id FK
    index: PK(group_id)
  }
  ORG_MEMBERSHIPS {
    int org_id FK
    int user_id FK
    text role
    index: PK(org_id,user_id)
    index: idx_org_memberships_user(user_id,org_id,role)
  }
  GROUP_MEMBERSHIPS {
    int group_id FK
    int user_id FK
    text role
    index: PK(group_id,user_id)
    index: idx_group_memberships_user(user_id,group_id,role)
  }
  GROUP_HIERARCHY {
    int parent_group_id FK
    int child_group_id FK
    text relation
    index: PK(parent_group_id,child_group_id,relation)
    index: idx_group_hierarchy_parent(parent_group_id,relation,child_group_id)
    index: idx_group_hierarchy_child(child_group_id,relation,parent_group_id)
  }
  RESOURCES {
    int resource_id PK
    int org_id FK
    index: PK(resource_id)
    index: idx_resources_org(org_id,resource_id)
  }
  RESOURCE_ACL {
    int resource_id FK
    text subject_type
    int subject_id
    text relation
    index: PK(resource_id,subject_type,subject_id,relation)
    index: idx_resource_acl_by_resource_subject(resource_id,subject_type,subject_id,relation)
    index: idx_resource_acl_by_subject(subject_type,subject_id,relation,resource_id)
    index: idx_resource_acl_res_rel_type_subject(resource_id,relation,subject_type,subject_id)
  }
  USER_RESOURCE_PERMISSIONS {
    int resource_id FK
    int org_id FK
    int user_id FK
    text relation
    mv: recursive expansion identical to PostgreSQL
    index: uq_user_resource_permissions(resource_id,user_id,relation)
    index: idx_urp_user_rel_res(user_id,relation,resource_id)
    index: idx_urp_org_user_rel(org_id,user_id,relation,resource_id)
  }
  ORGANIZATIONS ||--o{ USERS : org_id
  ORGANIZATIONS ||--o{ GROUPS : org_id
  ORGANIZATIONS ||--o{ RESOURCES : org_id
  ORGANIZATIONS ||--o{ ORG_MEMBERSHIPS : org_id
  USERS ||--o{ ORG_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_MEMBERSHIPS : group_id
  USERS ||--o{ GROUP_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_HIERARCHY : parent_group_id
  GROUPS ||--o{ GROUP_HIERARCHY : child_group_id
  RESOURCES ||--o{ RESOURCE_ACL : resource_id
  RESOURCES ||--o{ USER_RESOURCE_PERMISSIONS : resource_id
  USERS ||--o{ USER_RESOURCE_PERMISSIONS : user_id
```

---

## 3. ScyllaDB Schema
Sources: `cmd/scylladb/schemas.cql`, `cmd/scylladb/create_schemas.go`

Design emphasizes partition‑localized access (denormalization) and dual‑direction ACL plus compiled permission closures. Limited secondary indexes are added only where valid (single‑column) and beneficial.

```mermaid
erDiagram
  ORGANIZATIONS {
    int org_id PK(partition)
    index: PK(org_id)
  }
  USERS {
    int user_id PK(partition)
    int org_id
    index: PK(user_id)
  }
  GROUPS {
    int group_id PK(partition)
    int org_id
    index: PK(group_id)
  }
  ORG_MEMBERSHIPS {
    int org_id partition
    int user_id clustering
    text role clustering
    index: PK(org_id,user_id,role)
  }
  GROUP_MEMBERSHIPS {
    int user_id partition
    int group_id clustering
    text role clustering
    index: PK(user_id,group_id,role)
  }
  GROUP_HIERARCHY {
    int parent_group_id partition
    int child_group_id partition
    text relation clustering
    index: PK((parent_group_id,child_group_id),relation)
    index: idx_group_hierarchy_child(child_group_id)
  }
  GROUP_MEMBERS_EXPANDED {
    int group_id partition
    int user_id clustering
    text role clustering
    index: PK(group_id,user_id,role)
    index: idx_group_members_expanded_user(user_id)
  }
  RESOURCES {
    int resource_id PK(partition)
    int org_id
    index: PK(resource_id)
  }
  RESOURCE_ACL_BY_RESOURCE {
    int resource_id partition
    text relation clustering
    text subject_type clustering
    int subject_id clustering
    index: PK(resource_id,relation,subject_type,subject_id)
  }
  RESOURCE_ACL_BY_SUBJECT {
    text subject_type partition
    int subject_id partition
    text relation clustering
    int resource_id clustering
    index: PK((subject_type,subject_id),relation,resource_id)
  }
  USER_RESOURCE_PERMS_BY_USER {
    int user_id partition
    int resource_id clustering
    boolean can_manage
    boolean can_view
    index: PK(user_id,resource_id)
  }
  USER_RESOURCE_PERMS_BY_RESOURCE {
    int resource_id partition
    int user_id clustering
    boolean can_manage
    boolean can_view
    index: PK(resource_id,user_id)
  }
  ORGANIZATIONS ||--o{ USERS : org_id
  ORGANIZATIONS ||--o{ GROUPS : org_id
  ORGANIZATIONS ||--o{ RESOURCES : org_id
  ORGANIZATIONS ||--o{ ORG_MEMBERSHIPS : org_id
  USERS ||--o{ ORG_MEMBERSHIPS : user_id
  USERS ||--o{ GROUP_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_MEMBERSHIPS : group_id
  GROUPS ||--o{ GROUP_HIERARCHY : parent_group_id
  GROUPS ||--o{ GROUP_HIERARCHY : child_group_id
  GROUPS ||--o{ GROUP_MEMBERS_EXPANDED : group_id
  USERS ||--o{ GROUP_MEMBERS_EXPANDED : user_id
  RESOURCES ||--o{ RESOURCE_ACL_BY_RESOURCE : resource_id
  USERS ||--o{ RESOURCE_ACL_BY_SUBJECT : subject_id
  GROUPS ||--o{ RESOURCE_ACL_BY_SUBJECT : subject_id
  USERS ||--o{ USER_RESOURCE_PERMS_BY_USER : user_id
  RESOURCES ||--o{ USER_RESOURCE_PERMS_BY_RESOURCE : resource_id
```

---

## 4. MongoDB Schema
Source: `cmd/mongodb/create_schemas.go`

Document collections with multikey and compound indexes to accelerate membership and ACL expansion queries. Permission closure is embedded directly inside resource documents during load, but indexes target array fields for access operations.

```mermaid
erDiagram
  ORGANIZATIONS {
    int org_id
    array admin_user_ids
    array admin_group_ids
    array member_user_ids
    array member_group_ids
    index: org_id_unique(org_id) UNIQUE
    index: admin_user_ids_idx(admin_user_ids)
    index: admin_group_ids_idx(admin_group_ids)
    index: member_user_ids_idx(member_user_ids)
    index: member_group_ids_idx(member_group_ids)
  }
  USERS {
    int user_id
    array org_ids
    array group_ids
    index: user_id_unique(user_id) UNIQUE
    index: org_ids_idx(org_ids)
    index: group_ids_idx(group_ids)
  }
  GROUPS {
    int group_id
    int org_id
    array direct_member_user_ids
    array direct_manager_user_ids
    array member_group_ids
    array manager_group_ids
    index: group_id_unique(group_id) UNIQUE
    index: org_id_idx(org_id)
    index: direct_member_users_idx(direct_member_user_ids)
    index: direct_manager_users_idx(direct_manager_user_ids)
    index: member_group_ids_idx(member_group_ids)
    index: manager_group_ids_idx(manager_group_ids)
  }
  RESOURCES {
    int resource_id
    int org_id
    array manager_user_ids
    array viewer_user_ids
    array manager_group_ids
    array viewer_group_ids
    index: resource_id_unique(resource_id) UNIQUE
    index: org_id_idx(org_id)
    index: manager_user_ids_idx(manager_user_ids)
    index: viewer_user_ids_idx(viewer_user_ids)
    index: manager_group_ids_idx(manager_group_ids)
    index: viewer_group_ids_idx(viewer_group_ids)
    index: org_manage_user_idx(org_id,manager_user_ids)
    index: org_view_user_idx(org_id,viewer_user_ids)
    index: org_manage_group_idx(org_id,manager_group_ids)
    index: org_view_group_idx(org_id,viewer_group_ids)
  }
  ORGANIZATIONS ||--o{ GROUPS : org_id
  ORGANIZATIONS ||--o{ USERS : org_ids
  ORGANIZATIONS ||--o{ RESOURCES : org_id
  GROUPS ||--o{ RESOURCES : manager_group_ids
  GROUPS ||--o{ RESOURCES : viewer_group_ids
  USERS ||--o{ GROUPS : direct_member_user_ids
  USERS ||--o{ GROUPS : direct_manager_user_ids
  USERS ||--o{ RESOURCES : manager_user_ids
  USERS ||--o{ RESOURCES : viewer_user_ids
```

---

## 5. ClickHouse Schema
Source: `cmd/clickhouse/schemas.sql`

Column‑oriented denormalization with projections and specialized index types (minmax, bloom filter) plus a materialized view stream populating the permission closure table.

```mermaid
erDiagram
  ORGANIZATIONS {
    UInt32 org_id
    engine: MergeTree
    order: (org_id)
  }
  USERS {
    UInt32 user_id
    UInt32 primary_org_id
    engine: MergeTree
    order: (user_id)
  }
  GROUPS {
    UInt32 group_id
    UInt32 org_id
    engine: MergeTree
    partition: org_id
    order: (org_id,group_id)
  }
  ORG_MEMBERSHIPS {
    UInt32 org_id
    UInt32 user_id
    Enum8 role(member=1,admin=2)
    engine: MergeTree
    partition: org_id
    order: (org_id,user_id,role)
    index: idx_org_memberships_user(user_id) minmax
  }
  GROUP_MEMBERSHIPS {
    UInt32 group_id
    UInt32 user_id
    Enum8 role(member=1,manager=2)
    engine: MergeTree
    order: (user_id,group_id,role)
    index: idx_group_memberships_group(group_id) minmax
  }
  GROUP_HIERARCHY {
    UInt32 parent_group_id
    UInt32 child_group_id
    Enum8 relation(member_group=1,manager_group=2)
    engine: MergeTree
    order: (parent_group_id,child_group_id)
  }
  GROUP_MEMBERS_EXPANDED {
    UInt32 group_id
    UInt32 user_id
    Enum8 role(member=1,manager=2)
    engine: MergeTree
    order: (group_id,user_id,role)
    index: idx_group_members_expanded_user(user_id) minmax
  }
  RESOURCES {
    UInt32 resource_id
    UInt32 org_id
    engine: MergeTree
    partition: org_id
    order: (org_id,resource_id)
    index: idx_resources_resource(resource_id) minmax
  }
  RESOURCE_ACL {
    UInt32 resource_id
    UInt32 org_id
    Enum8 subject_type(user=1,group=2)
    UInt32 subject_id
    Enum8 relation(viewer=1,manager=2)
    engine: MergeTree
    partition: org_id
    order: (org_id,resource_id,relation,subject_type,subject_id)
    projection: resource_acl_by_subject(org_id,subject_type,subject_id,relation,resource_id)
    index: idx_resource_acl_subject_bf(subject_type,subject_id) bloom_filter
    index: idx_resource_acl_resource_minmax(resource_id) minmax
  }
  USER_RESOURCE_PERMISSIONS {
    UInt32 resource_id
    UInt32 user_id
    Enum8 relation(viewer=1,manager=2)
    engine: MergeTree
    partition: intDiv(user_id,10000)
    order: (user_id,resource_id,relation)
    mv: user_resource_permissions_mv (UNION ALL + joins)
  }
  ORGANIZATIONS ||--o{ GROUPS : org_id
  ORGANIZATIONS ||--o{ RESOURCES : org_id
  ORGANIZATIONS ||--o{ ORG_MEMBERSHIPS : org_id
  USERS ||--o{ ORG_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_MEMBERSHIPS : group_id
  USERS ||--o{ GROUP_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_HIERARCHY : parent_group_id
  GROUPS ||--o{ GROUP_HIERARCHY : child_group_id
  GROUPS ||--o{ GROUP_MEMBERS_EXPANDED : group_id
  USERS ||--o{ GROUP_MEMBERS_EXPANDED : user_id
  RESOURCES ||--o{ RESOURCE_ACL : resource_id
  RESOURCES ||--o{ USER_RESOURCE_PERMISSIONS : resource_id
  USERS ||--o{ USER_RESOURCE_PERMISSIONS : user_id
```

---

## 6. Elasticsearch Schema
Source: `cmd/elasticsearch/create_schemas.go`

Single index `resources` with nested ACL documents and flattened arrays of allowed user identifiers per permission tier. Elasticsearch automatically inverts indexed fields; explicit index declarations are implicit.

```mermaid
erDiagram
  RESOURCE_DOCUMENT {
    int resource_id PK(logical)
    int org_id
    array allowed_manage_user_id
    array allowed_view_user_id
    nested acl.subject_type(keyword)
    nested acl.subject_id(integer)
    nested acl.relation(keyword)
    index: default_inverted(resource_id,org_id,allowed_* fields)
    index: nested_acl(subject_type,subject_id,relation)
  }
```

---

## 7. Authzed (PostgreSQL Backend) Graph Schema
Source: `cmd/authzed_pgdb/schemas.zed`

Logical authorization graph expressed as object definitions, relations, and computed permissions. Underlying persistence leverages PostgreSQL storage primitives internally; explicit index management is abstracted.

```mermaid
flowchart LR
  user([user])
  usergroup([usergroup])
  organization([organization])
  resource([resource])

  usergroup -->|direct_member_user| user
  usergroup -->|direct_manager_user| user
  usergroup -->|member_group| usergroup
  usergroup -->|manager_group| usergroup

  organization -->|admin_user| user
  organization -->|admin_group (usergroup#manager)| usergroup
  organization -->|member_user| user
  organization -->|member_group (usergroup#member)| usergroup

  resource -->|org| organization
  resource -->|manager_user| user
  resource -->|viewer_user| user
  resource -->|manager_group (usergroup#manager)| usergroup
  resource -->|viewer_group (usergroup#member)| usergroup

  %% Permissions (computed expressions)
  usergroup -.-> member_perm([permission member = direct_member_user + member_group->member + manager])
  usergroup -.-> manager_perm([permission manager = direct_manager_user + manager_group->manager])
  organization -.-> org_admin_perm([permission admin = admin_user + admin_group])
  organization -.-> org_member_perm([permission member = member_user + member_group + admin])
  resource -.-> res_manage_perm([permission manage = manager_user + manager_group + org->admin])
  resource -.-> res_view_perm([permission view = viewer_user + viewer_group + manage + org->member])
```

---

## 8. Authzed (CockroachDB Backend) Graph Schema
Source: `cmd/authzed_crdb/schemas.zed`

Identical logical model to PostgreSQL backend; storage engine differences are transparent to the schema language. Computed permission expressions mirror those above.

```mermaid
flowchart LR
  user([user])
  usergroup([usergroup])
  organization([organization])
  resource([resource])

  usergroup -->|direct_member_user| user
  usergroup -->|direct_manager_user| user
  usergroup -->|member_group| usergroup
  usergroup -->|manager_group| usergroup

  organization -->|admin_user| user
  organization -->|admin_group (usergroup#manager)| usergroup
  organization -->|member_user| user
  organization -->|member_group (usergroup#member)| usergroup

  resource -->|org| organization
  resource -->|manager_user| user
  resource -->|viewer_user| user
  resource -->|manager_group (usergroup#manager)| usergroup
  resource -->|viewer_group (usergroup#member)| usergroup

  usergroup -.-> member_perm([permission member = direct_member_user + member_group->member + manager])
  usergroup -.-> manager_perm([permission manager = direct_manager_user + manager_group->manager])
  organization -.-> org_admin_perm([permission admin = admin_user + admin_group])
  organization -.-> org_member_perm([permission member = member_user + member_group + admin])
  resource -.-> res_manage_perm([permission manage = manager_user + manager_group + org->admin])
  resource -.-> res_view_perm([permission view = viewer_user + viewer_group + manage + org->member])
```

---

## 9. Comparative Summary

| Engine | Core Modeling Strategy | Closure Handling | Index / Optimization Highlights |
|--------|------------------------|------------------|---------------------------------|
| PostgreSQL | Normalized relational + MV | Recursive CTE → MV | Multi‑column B‑tree + specialized ACL indexes |
| CockroachDB | Same as PostgreSQL | Recursive CTE → MV | Same as PostgreSQL (no function) |
| ScyllaDB | Denormalized partitions | Precomputed tables | Partition keys + selective secondary indexes |
| MongoDB | Document collections | Embedded arrays | Multikey & compound indexes on arrays |
| ClickHouse | Columnar denormalized | MV stream union | Minmax, bloom filter, projection, partitioning |
| Elasticsearch | Single index document | Arrays of user IDs | Inverted index + nested ACL mapping |
| Authzed (PG) | Relation graph | Computed permissions | Internal storage; expressions replace closure table |
| Authzed (CRDB) | Relation graph | Computed permissions | Same as PG backend |

All eight implementations originate from one logical access control specification but diverge to exploit engine‑specific strengths (recursive SQL, partition locality, multikey arrays, columnar projections, graph expressions, and inverted indexing).

