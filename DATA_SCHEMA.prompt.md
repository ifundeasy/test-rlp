# Prompt: Generate Formal Schema Compendium for RLP Benchmark

Objective: Produce a formal, engine-specific schema compendium for the Row-Level Permission (RLP) benchmark, rendered as Mermaid diagrams per database engine, matching the repository’s schemas 1:1.

Language: Use formal English throughout.

Scope: Include eight engines:
- PostgreSQL
- CockroachDB
- ScyllaDB
- MongoDB
- ClickHouse
- Elasticsearch
- Authzed (PostgreSQL backend)
- Authzed (CockroachDB backend)

Source Files:
- Prefer `cmd/*/schemas.sql`, `cmd/*/schemas.cql`, `cmd/*/schemas.zed`.
- If any of those are missing for an engine, extract the schema (tables/collections, indexes, views/projections) from `cmd/*/create_schemas.go`.

Diagram Rules:
- Use Mermaid `erDiagram` for relational/tabular and document engines.
- Use Mermaid `flowchart` for Authzed (relation graph schema).
- Provide **exactly one** diagram per engine.
- Each diagram must include complete entities/tables/collections/views with fields and optimization metadata.
- Annotate optimization metadata as pseudo-attributes:
  - `index:` for indexes (include columns and type where relevant).
  - `projection:` for ClickHouse projections.
  - `mv:` for materialized views or MV streams.
- Reflect engine-specific details:
  - PostgreSQL/CockroachDB: PKs, FKs, ACL table, `user_resource_permissions` MV with indexes; CockroachDB does not include PL/pgSQL functions.
  - ScyllaDB: partition/clustering keys, dual ACL tables, permission closure tables, only valid single-column secondary indexes.
  - MongoDB: collections and index specs from `create_schemas.go` (unique, multikey, compound) against array fields.
  - ClickHouse: table engines, partitions, orders, index types (minmax, bloom), projections; MV stream union target.
  - Elasticsearch: single index mapping with arrays of allowed user IDs and nested `acl` fields; indexing is implicit but annotate as `index:`.
  - Authzed: object definitions, relations, and computed permission expressions from `.zed` files.

Primary Key Presentation Rules:
- Show only single-column primary keys with `PK` in diagrams.
- For composite primary keys, do not mark fields with `PK` in diagrams; move these keys to the section’s Indexes table as `pk_<table>` entries listing all key columns in order.
- Apply consistently across engines (PostgreSQL, CockroachDB, ScyllaDB, ClickHouse). Elasticsearch has no PK flag.

Indexes and Views Table Format:
- Each engine section must include an Indexes table with columns: `No`, `Index Name`, `Table`, `Columns` (add `Type` when relevant, e.g., ClickHouse MINMAX/BLOOM, MongoDB UNIQUE/MULTIKEY/COMPOUND).
- Include composite primary keys as `pk_*` entries in the Indexes table per the rules above.
- For engines with materialized views or projections, include a dedicated MV/Projection table with columns: `Final Field (Output MV)`, `Source Expression`, `Operation / Transformation`, `Real Table(s) Used`, `Explanation`. For ClickHouse also add a `Table Engines and Partitioning` table.

Content Structure:
- Title: “Row-Level Permission Benchmark – Formal Schema Compendium”.
- Intro: Explain logical CSV source model and how engines project it differently (denormalization, transitive expansion, closure materialization).
- Eight sections, one per engine:
  - Header: “## N. <Engine Name> Schema”.
  - "Source:" line with the repository file path(s).
  - One to two sentences describing design features (e.g., closure strategy, denormalization, indexing).
  - Mermaid diagram following the rules above and mirroring the actual schema 1:1, respecting Primary Key Presentation Rules.
  - An Indexes table matching the “Indexes and Views Table Format”, populated from source DDL/code, including `pk_*` rows for composite primary keys.
  - For engines with materialized views/projections, include a dedicated MV/Projection table using the specified columns.
- Comparative summary table with columns: Engine, Core Modeling Strategy, Closure Handling, Index/Optimization Highlights.

Accuracy Requirements:
- Ensure 1:1 fidelity with the repository definitions:
  - PostgreSQL/CockroachDB: include all tables, indexes, and the materialized view definitions and indexes; CockroachDB section should note lack of PL/pgSQL refresh function.
  - ScyllaDB: include exactly the tables and keys; list only valid single-column secondary indexes; move composite keys to Indexes as `pk_*` and omit PK flags from diagram fields.
  - MongoDB: include collections and every index created in `create_schemas.go`.
  - ClickHouse: include engines, partitions, orders, index types, projection, and materialized view details; represent composite keys in the Indexes table if defined.
  - Elasticsearch: include mapping fields (`resource_id`, `org_id`, arrays of allowed user IDs, nested `acl`) as defined.
  - Authzed (PG/CRDB): include relations and computed permissions exactly as in `.zed`.

Output:
- Emit a single Markdown document intended to replace the contents of `DATA_SCHEMA.md`.
- Preserve the structure and diagram content exactly as generated from these instructions.

Notes:
- Index, projection, and mv entries in diagrams serve as documentation annotations; actual DDL/mappings remain in the repository source files.
- If any discrepancies are found during generation, prefer the repository source of truth and update the diagram accordingly.
 - Ensure output matches the formatting/style used in `DATA_SCHEMA.md` (tables, headers, column names), and that PK flags follow the rules above.
