# Row-Level Permission Benchmark – Database Schema Overview

This document summarizes the logical data model used by your Go benchmarking project for Row Level Permission (RLP) across the different database engines. It focuses **only on relationship schemas**, not on program flow.

The core idea: you generate the same logical dataset from CSV files and then project it into different physical models depending on the engine.

---

## 0. Logical Schema (From `cmd/csv/load_data.go`)

Base CSVs and their logical tables:

* `organizations.csv` → `organizations(org_id)`
* `users.csv` → `users(user_id, org_id)`
* `groups.csv` → `groups(group_id, org_id)`
* `org_memberships.csv` → `org_memberships(org_id, user_id, role)`
* `group_memberships.csv` → `group_memberships(group_id, user_id, role)`
* `resources.csv` → `resources(resource_id, org_id)`
* `resource_acl.csv` → `resource_acl(resource_id, subject_type, subject_id, relation)`
* Closure / materialized permission tables (depending on engine):

  * `user_resource_perms` or `user_resource_perms_*` with `(user_id, resource_id, can_manage, can_view, …)`

Conceptually:

* An **organization** owns many users, groups, and resources.
* **Org membership** and **group membership** are bridge tables between users and organizations/groups.
* **Resource ACL** describes directed edges from a resource to subjects (users or groups) with a relation (e.g., manager, viewer).
* Some engines precompute **user-resource-permission closure** to accelerate checks and listings.

---

## 1. PostgreSQL & CockroachDB

PostgreSQL and CockroachDB share the same logical schema (same `schemas.sql`), only differing by the engine.

```mermaid
erDiagram
  ORGANIZATIONS {
    int org_id
  }
  USERS {
    int user_id
    int org_id
  }
  GROUPS {
    int group_id
    int org_id
  }
  ORG_MEMBERSHIPS {
    int org_id
    int user_id
    text role
  }
  GROUP_MEMBERSHIPS {
    int group_id
    int user_id
    text role
  }
  RESOURCES {
    int resource_id
    int org_id
  }
  RESOURCE_ACL {
    int resource_id
    text subject_type
    int subject_id
    text relation
  }
  ORGANIZATIONS ||--o{ USERS : org_id
  ORGANIZATIONS ||--o{ GROUPS : org_id
  ORGANIZATIONS ||--o{ RESOURCES : org_id
  ORGANIZATIONS ||--o{ ORG_MEMBERSHIPS : org_id
  USERS ||--o{ ORG_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_MEMBERSHIPS : group_id
  USERS ||--o{ GROUP_MEMBERSHIPS : user_id
  RESOURCES ||--o{ RESOURCE_ACL : resource_id
```

Key points:

* All users, groups, and resources are anchored to an organization.
* Org and group memberships are many-to-many via bridge tables.
* `resource_acl` is a Zanzibar-style edge table `(resource) -[relation]-> (user|group)`.

---

## 2. ScyllaDB

ScyllaDB keeps the same logical entities, but adds denormalized ACL and permission-closure tables tuned for specific access patterns (by resource and by subject).

```mermaid
erDiagram
  ORGANIZATIONS {
    int org_id
  }
  USERS {
    int user_id
    int org_id
  }
  GROUPS {
    int group_id
    int org_id
  }
  ORG_MEMBERSHIPS {
    int org_id
    int user_id
    text role
  }
  GROUP_MEMBERSHIPS {
    int group_id
    int user_id
    text role
  }
  RESOURCES {
    int resource_id
    int org_id
  }
  RESOURCE_ACL_BY_RESOURCE {
    int resource_id
    text relation
    text subject_type
    int subject_id
  }
  RESOURCE_ACL_BY_SUBJECT {
    text subject_type
    int subject_id
    text relation
    int resource_id
  }
  USER_RESOURCE_PERMS_BY_USER {
    int user_id
    int resource_id
    bool can_manage
    bool can_view
  }
  USER_RESOURCE_PERMS_BY_RESOURCE {
    int resource_id
    int user_id
    bool can_manage
    bool can_view
  }
  ORGANIZATIONS ||--o{ USERS : org_id
  ORGANIZATIONS ||--o{ GROUPS : org_id
  ORGANIZATIONS ||--o{ RESOURCES : org_id
  ORGANIZATIONS ||--o{ ORG_MEMBERSHIPS : org_id
  USERS ||--o{ ORG_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_MEMBERSHIPS : group_id
  USERS ||--o{ GROUP_MEMBERSHIPS : user_id
  RESOURCES ||--o{ RESOURCE_ACL_BY_RESOURCE : resource_id
  RESOURCES ||--o{ RESOURCE_ACL_BY_SUBJECT : resource_id
  USERS ||--o{ RESOURCE_ACL_BY_SUBJECT : subject_id
  GROUPS ||--o{ RESOURCE_ACL_BY_SUBJECT : subject_id
  USERS ||--o{ USER_RESOURCE_PERMS_BY_USER : user_id
  RESOURCES ||--o{ USER_RESOURCE_PERMS_BY_USER : resource_id
  RESOURCES ||--o{ USER_RESOURCE_PERMS_BY_RESOURCE : resource_id
  USERS ||--o{ USER_RESOURCE_PERMS_BY_RESOURCE : user_id
```

Highlights in ScyllaDB:

* ACL is stored in two denormalized tables:

  * `resource_acl_by_resource`: partitioned by resource.
  * `resource_acl_by_subject`: partitioned by subject (user/group).
* Permissions closure is also stored two ways:

  * `user_resource_perms_by_user`: all resources for a given user.
  * `user_resource_perms_by_resource`: all users for a given resource.

This design is explicitly tuned for both "check" and "list" queries without joins.

---

## 3. MongoDB

MongoDB mirrors the CSV dataset closely, including a closure collection for user-resource permissions.

```mermaid
erDiagram
  ORGANIZATIONS {
    int _id
  }
  USERS {
    int _id
    int org_id
  }
  GROUPS {
    int _id
    int org_id
  }
  ORG_MEMBERSHIPS {
    int org_id
    int user_id
    string role
  }
  GROUP_MEMBERSHIPS {
    int group_id
    int user_id
    string role
  }
  RESOURCES {
    int _id
    int org_id
  }
  RESOURCE_ACL {
    int resource_id
    string subject_type
    int subject_id
    string relation
  }
  USER_RESOURCE_PERMS {
    int user_id
    int resource_id
    bool can_manage
    bool can_view
  }
  ORGANIZATIONS ||--o{ USERS : org_id
  ORGANIZATIONS ||--o{ GROUPS : org_id
  ORGANIZATIONS ||--o{ RESOURCES : org_id
  ORGANIZATIONS ||--o{ ORG_MEMBERSHIPS : org_id
  USERS ||--o{ ORG_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_MEMBERSHIPS : group_id
  USERS ||--o{ GROUP_MEMBERSHIPS : user_id
  RESOURCES ||--o{ RESOURCE_ACL : resource_id
  USERS ||--o{ USER_RESOURCE_PERMS : user_id
  RESOURCES ||--o{ USER_RESOURCE_PERMS : resource_id
```

MongoDB is essentially a direct document-shaped translation of the relational model plus a closure collection.

---

## 4. ClickHouse

ClickHouse uses a relational-style schema with some denormalization and projections focused on analytics and high-speed scans.

```mermaid
erDiagram
  ORGANIZATIONS {
    UInt32 org_id
  }
  USERS {
    UInt32 user_id
    UInt32 primary_org_id
  }
  GROUPS {
    UInt32 group_id
    UInt32 org_id
  }
  ORG_MEMBERSHIPS {
    UInt32 org_id
    UInt32 user_id
    enum role
  }
  GROUP_MEMBERSHIPS {
    UInt32 group_id
    UInt32 user_id
    enum role
  }
  RESOURCES {
    UInt32 resource_id
    UInt32 org_id
  }
  RESOURCE_ACL {
    UInt32 resource_id
    UInt32 org_id
    enum subject_type
    UInt32 subject_id
    enum relation
  }
  ORGANIZATIONS ||--o{ USERS : primary_org_id
  ORGANIZATIONS ||--o{ GROUPS : org_id
  ORGANIZATIONS ||--o{ RESOURCES : org_id
  ORGANIZATIONS ||--o{ ORG_MEMBERSHIPS : org_id
  USERS ||--o{ ORG_MEMBERSHIPS : user_id
  GROUPS ||--o{ GROUP_MEMBERSHIPS : group_id
  USERS ||--o{ GROUP_MEMBERSHIPS : user_id
  RESOURCES ||--o{ RESOURCE_ACL : resource_id
```

Key detail: `org_id` is duplicated into `resource_acl` to support partitioning and efficient org-scoped queries.

---

## 5. Elasticsearch

Elasticsearch uses a single index with one document per resource. ACL and user closure are embedded inside the document.

```mermaid
erDiagram
    ORG {
        long org_id PK
    }

    USER {
        long user_id PK
        string role
    }

    GROUP {
        long group_id PK
    }

    RESOURCE {
        long resource_id PK
        long org_id FK
        string allowed_user_ids_manage
        string allowed_user_ids_view
    }

    ACL {
        string relation
        string subject_type
        long subject_id
    }

    ORG ||--o{ RESOURCE : owns
    ORG ||--o{ USER : has
    GROUP ||--o{ USER : includes
    RESOURCE ||--o{ ACL : defines
    ACL }o--|| USER : may_grant
    ACL }o--|| GROUP : may_grant
```

Here:

* Each `ResourceDocument` embeds:

  * The resource metadata.
  * The ACL edges as nested documents.
  * The precomputed closure as arrays of user IDs per permission.

---

## 6. Authzed (PostgreSQL & CockroachDB Backends)

Authzed uses a **relation graph schema**, not explicit SQL tables. The storage engine may be PostgreSQL or CockroachDB, but the logical model is defined in `.zed` schema files.

```mermaid
flowchart TD
  %% Object Types
  user[User]
  usergroup[UserGroup]
  organization[Organization]
  resource[Resource]

  %% Relations: UserGroup
  user -->|member_of| usergroup
  user -->|manager_of| usergroup
  usergroup -->|includes| usergroup
  usergroup -->|member_user| user
  usergroup -->|manager_user| user

  %% Relations: Organization
  user -->|member_of_org| organization
  user -->|admin_of_org| organization
  usergroup -->|member_group| organization
  usergroup -->|admin_group| organization

  %% Relations: Resource
  organization -->|owns| resource
  user -->|viewer_user| resource
  user -->|manager_user| resource
  usergroup -->|viewer_group| resource
  usergroup -->|manager_group| resource

  %% Permissions (computed)
  resource -.->|compute: manage = manager_user + manager_group + org->admin| manage_perm
  resource -.->|compute: view = viewer_user + viewer_group + manage + org->member| view_perm
  organization -.->|compute: admin = admin_user + admin_group| org_admin_perm
  organization -.->|compute: member = member_user + member_group + admin| org_member_perm

  %% Permission nodes (for readability)
  manage_perm(["Permission: manage"])
  view_perm(["Permission: view"])
  org_admin_perm(["Permission: org admin"])
  org_member_perm(["Permission: org member"])
```

In Authzed:

* Membership and ACL are all stored as relationships in a global authorization graph.
* Permissions (`manage`, `view`, `member`, `admin`, etc.) are **computed** from expressions over these relationships, not stored as explicit boolean columns.

---

## 7. Summary

* **PostgreSQL & CockroachDB**: classic relational model mirroring the CSVs, with ACL as an edge table.
* **MongoDB**: document model mirroring the relational structure, plus a dedicated closure collection.
* **ScyllaDB**: relational-ish entities but heavily denormalized around access patterns:

  * ACL by resource and by subject.
  * Permissions closure by user and by resource.
* **ClickHouse**: relational layout optimized for analytical scans, with org-scoped denormalization in ACL.
* **Elasticsearch**: single index, one document per resource, with nested ACL and flattened permission closure arrays.
* **Authzed**: authorization graph schema where users, groups, organizations, and resources are all objects, and permissions are expressions over relationships.

This gives you a clean, engine-by-engine view of how the same RLP problem is modeled at the data/schema level across your benchmark.
