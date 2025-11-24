# Executive Summary

This benchmark evaluates **Row Level Permission (RLP)** performance across seven data engines using a realistic multi-tenant dataset. The tests include **dataset ingestion**, **point-check authorization**, and **fan-out permission listings**, all extracted directly from the logs inside `benchmark.tar.gz`. The dataset contains **457k ACL edges**, **29k resources**, and **1.1k users**, designed to stress engines across relational, document, search, columnar, and graph-based authorization models.

**Key Findings:**

* **PostgreSQL is the strongest overall engine**, combining fast ingest (~54k rows/s), sub-millisecond point-checks, and high-throughput fan-out listings (6–12 ms for 4k+ rows).
* **MongoDB performs very well in point-checks** but becomes slow in large fan-outs due to the document and index structure (~40–50 ms for 4k+ listings).
* **Authzed/SpiceDB provides centralized cross-service authorization** but adds noticeable latency (2–3× slower checks, 5–10× slower listings). PostgreSQL backend is significantly faster than Cockroach.
* **Elasticsearch dominates listing/search workloads** (best for “what can I see?” UX) but is not suitable as an authoritative RLP backend.
* **ClickHouse ingests the fastest (~405k rows/s)** but is the slowest for RLP reads. It is ideal for analytics, not hot-path enforcement.
* **CockroachDB and ScyllaDB struggle heavily** with this workload: Cockroach is slow in both ingest and reads; ScyllaDB fails to ingest the dataset.

**Recommended Architecture:**
Use **PostgreSQL for authoritative row-level checks**, **SpiceDB for cross-service relation graphs**, **Elasticsearch for listing/search**, and **ClickHouse for compliance and auditing**. This hybrid model plays to the strengths of each engine.

---

# Row Level Permission (RLP) Benchmark Report

A detailed technical analysis combining ingest metrics, point-check latency, fan-out behavior, and architectural suitability.

---

## 1. Dataset & Scenario Overview

The benchmark simulates realistic multi-tenant authorization operations.

### 1.1 Dataset composition

Extracted from `1-prepare.log`:

* **Organizations:** 16
* **Users:** 1,141
* **Groups:** 332
* **Resources:** 29,370
* **Org memberships:** 2,420
* **Group memberships:** 9,110
* **ACL entries:** 457,366

### 1.2 Permission graph model

The dataset includes:

* Direct management relationships (manager → employee)
* Group-based permissions (group → resource)
* Organization-wide administrative permissions
* User/group → resource → relation mapping

The system evaluates queries like:

* *Can Alice manage Bob?*
* *Is user 88 an org admin?*
* *What resources can user 705 manage?*
* *What resources can user 804 view?*

---

## 2. Benchmark Workloads

All extracted from `4-read-data.log`.

### 2.1 Point-check authorization

Fast boolean permission checks:

* **`check_manage_direct_user`** (1000 iterations, 83k pairs)
* **`check_manage_org_admin`** (1000 iterations, 29k pairs)
* **`check_view_via_group_member`** (1000 iterations, 88k pairs)

These simulate real-time API authorization checks.

### 2.2 Fan-out listing queries

Simulates dashboard or listing views with pagination cost:

* **`lookup_resources_manage_super`** — heavy manager listing (~4.8k resources)
* **`lookup_resources_view_regular`** — regular user listing (~3.1k resources)

Each runs 10 rounds per engine.

---

## 3. Ingest / Load Performance

Extracted from `3-load-data.log`.

### 3.1 Raw ingest times

| Engine            | Data Volume    | Load Time   | Interpretation                                                    |
| ----------------- | -------------- | ----------- | ----------------------------------------------------------------- |
| **ClickHouse**    | 499,755 rows   | **1.231s**  | Extremely fast; optimized columnar ingest                         |
| **Postgres**      | 499,755 rows   | **9.185s**  | Strong transactional ingest performance                           |
| **MongoDB**       | 3,644,328 docs | **122s**    | Includes generating a 3.1M document materialized permission table |
| **Authzed PG**    | 498,598 rels   | **35s**     | Faster backend for SpiceDB                                        |
| **Authzed CRDB**  | 498,598 rels   | **65s**     | ~2× slower than PG backend                                        |
| **CockroachDB**   | 499,755 rows   | **101.71s** | Slowest successful ingest                                         |
| **Elasticsearch** | 29,370 docs    | **17s**     | Index-only ingest; does not load ACL graph                        |
| **ScyllaDB**      | —              | **FAILED**  | Timeout on ACL wide-table insert                                  |

### 3.2 Throughput summary

* **ClickHouse:** ~405k rows/s
* **Postgres:** ~54k rows/s
* **MongoDB:** ~30k docs/s (after heavy precomputation)
* **Authzed PG:** ~14k rel/s
* **Authzed CRDB:** ~7.7k rel/s
* **CockroachDB:** ~4.9k rows/s
* **ES:** ~1.7k docs/s

### 3.3 Interpretation

ClickHouse ingests an order-of-magnitude faster but cannot execute millisecond-level RLP checks. Postgres remains the most balanced engine. MongoDB's ingest expands permissions into millions of documents, trading storage for faster check operations.

---

## 4. Authorization Performance (Query Benchmarks)

Values represent **mean latency over 10 rounds**.

### 4.1 `check_manage_direct_user`

Point-check: “Can manager **M** directly manage user **U**?” (1000 iterations per run, ~83k candidate pairs).

| Engine        | Mean Latency (ms) | p95 Latency (ms) | Mean Throughput (ops/s) | Runs | Notes                           |
| ------------- | ----------------: | ---------------: | ----------------------: | ---: | ------------------------------- |
| Postgres      |             0.998 |            1.442 |                  1026.3 |   10 | Fastest inline RLS              |
| MongoDB       |             1.075 |            1.652 |                   971.1 |   10 | Close to Postgres; doc fetch    |
| CockroachDB   |             1.801 |            2.545 |                   571.5 |   10 | Slower execution path           |
| Authzed PG    |             2.125 |            2.613 |                   479.9 |   10 | Graph traversal overhead        |
| Elasticsearch |             2.860 |            4.548 |                   385.9 |   10 | Search engine; not OLTP         |
| Authzed CRDB  |             3.278 |            4.124 |                   313.5 |   10 | CRDB backend adds latency       |
| ClickHouse    |             6.460 |            7.923 |                   156.2 |   10 | Columnar; poor for point checks |

### 4.2 `check_manage_org_admin`

Point-check: “Is user **U** an organization admin?” (1000 iterations per run, 29k pairs).

| Engine       | Mean Latency (ms) | p95 Latency (ms) | Mean Throughput (ops/s) | Runs | Notes                           |
| ------------ | ----------------: | ---------------: | ----------------------: | ---: | ------------------------------- |
| Postgres     |             1.326 |            4.324 |                   932.8 |   10 | Strong RLS join performance     |
| MongoDB      |             1.550 |            2.143 |                   662.3 |   10 | Similar band to Postgres        |
| Authzed PG   |             2.266 |            2.906 |                   450.7 |   10 | Extra relation tuple resolution |
| CockroachDB  |             2.661 |            3.960 |                   386.8 |   10 | Heavier nested lookup cost      |
| Authzed CRDB |             2.943 |            4.598 |                   353.5 |   10 | Backend overhead visible        |
| ClickHouse   |             7.834 |            8.841 |                   128.5 |   10 | Too slow for point checks       |

### 4.3 `check_view_via_group_member`

Point-check: “Can user **U** view resource **R** via group membership?” (1000 iterations per run, 88k pairs).

| Engine       | Mean Latency (ms) | p95 Latency (ms) | Mean Throughput (ops/s) | Runs | Notes                         |
| ------------ | ----------------: | ---------------: | ----------------------: | ---: | ----------------------------- |
| Postgres     |             1.161 |            1.682 |                   878.2 |   10 | Best latency with group joins |
| MongoDB      |             1.880 |            2.773 |                   552.9 |   10 | Extra indirection vs Postgres |
| Authzed PG   |             2.150 |            2.870 |                   469.7 |   10 | Multi-edge graph traversal    |
| CockroachDB  |             2.877 |            3.311 |                   350.2 |   10 | Slower KV access              |
| Authzed CRDB |             3.733 |            5.229 |                   275.2 |   10 | CRDB backend + graph cost     |
| ClickHouse   |             8.799 |           10.208 |                   114.5 |   10 | Columnar, not OLTP-oriented   |

### 4.4 `lookup_resources_manage_super`

Fan-out: “List all resources managed by a **heavy manager user**.” (~4.1k–4.85k results for DB/graph engines; ~100 for Elasticsearch). 10 iterations per run.

| Engine        | Mean Latency (ms) | p95 Latency (ms) | Mean Throughput (ops/s) | Runs | Notes                              |
| ------------- | ----------------: | ---------------: | ----------------------: | ---: | ---------------------------------- |
| Elasticsearch |             5.715 |           10.245 |                   190.2 |   10 | Very fast but on ~100 results only |
| Postgres      |             6.306 |            9.124 |                   163.1 |   10 | Excellent for ~4.8k resources      |
| CockroachDB   |            12.613 |           19.460 |                    82.8 |   10 | Roughly 2× slower than Postgres    |
| ClickHouse    |            26.792 |           35.003 |                    38.1 |   10 | Columnar scan overhead             |
| MongoDB       |            38.998 |           59.064 |                    26.5 |   10 | Document fan-out inefficient       |
| Authzed PG    |            62.470 |          100.197 |                    16.7 |   10 | Graph expansion dominates          |
| Authzed CRDB  |            78.626 |          151.563 |                    13.9 |   10 | Slowest: CRDB + graph costs        |

### 4.5 `lookup_resources_view_regular`

Fan-out: “List all resources that a **regular viewer user** can see.” (~3.1k results per call). 10 iterations per run.

| Engine        | Mean Latency (ms) | p95 Latency (ms) | Mean Throughput (ops/s) | Runs | Notes                           |
| ------------- | ----------------: | ---------------: | ----------------------: | ---: | ------------------------------- |
| Elasticsearch |             7.440 |           12.421 |                   147.0 |   10 | Best overall UX listing latency |
| Postgres      |            11.760 |           21.973 |                    93.0 |   10 | Strong scaling at ~3.1k rows    |
| CockroachDB   |            18.977 |           23.762 |                    53.4 |   10 | Noticeably slower scans         |
| ClickHouse    |            45.299 |           54.090 |                    22.2 |   10 | Analytics-only characteristics  |
| MongoDB       |            47.148 |           58.206 |                    21.4 |   10 | Penalized by document structure |
| Authzed PG    |            71.276 |          131.199 |                    15.8 |   10 | Heavy relation expansion        |
| Authzed CRDB  |            88.772 |          165.580 |                    13.4 |   10 | Worst latency; backend + graph  |

## 5. Cross-Engine Behavior Patterns

## 5. Cross-Engine Behavior Patterns

### 5.1 Inline RLS (Postgres/Mongo) vs Authzed Graph

* Inline Postgres/Mongo yield **1–2 ms** checks + **6–12 ms** fan-outs.
* Authzed introduces **2–3× slowdown** for checks and **5–10× slowdowns** for fan-outs.
* Trade-off: latency vs centralized multi-service authorization model.

### 5.2 Postgres vs CockroachDB

Cockroach consistently underperforms:

* ~2× slower ingest
* ~2× slower point-checks
* ~2–3× slower fan-outs

### 5.3 MongoDB Characteristics

* Very fast single-row checks
* Slow large fan-outs → index and document modeling bottleneck

### 5.4 Elasticsearch Specialization

* Ideal for search and fan-out listing
* Not suitable as authoritative RBAC/RLP engine

### 5.5 ClickHouse

* Best ingest
* Worst hot-path latency

### 5.6 Jitter Analysis

Some engines show 3–5× variability between best and worst runs; ordering remains stable.

---

## 6. Architectural Recommendations

### 6.1 Authoritative RLP Engine (Hot Path)

**Use PostgreSQL**:

* RLS
* Fast joins
* Predictable latency
* Strong ingest

### 6.2 Decoupled Cross-Service Authorization

**Use Authzed/SpiceDB with PostgreSQL backend**:

* For multi-domain, multi-product B2B rules
* For tenant isolation across microservices

### 6.3 Listing and Search Views

**Use Elasticsearch** as a projection layer:

* "Shared with me"
* Permission-aware search
* Large fan-out queries

### 6.4 Analytics & Compliance

**Use ClickHouse**:

* Permission change timelines
* Monthly audit reports
* Security forensics

### 6.5 Avoid in Hot Path

* CockroachDB (slow ingest & reads)
* ScyllaDB (ingest failures)

---

## 7. Recommended Multi-Tier Permission Architecture

**Layered model leveraging the strengths of each engine:**

* **Postgres** → authoritative RLP (RLS, policy joins)
* **SpiceDB/Authzed** → global relation tuples & policy graph
* **Elasticsearch** → fast listing/search projection
* **ClickHouse** → analytics, history, compliance dashboards

This decomposition provides speed, correctness, and scalability for enterprise-grade multi-tenant authorization.
