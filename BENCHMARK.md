# Read & Write Benchmark Report

## 1. Objectives
- Measure write (ingest/initial load) and read performance (permission checks, listings, relationship traversals) on the current dataset to establish a baseline.
- Compare multiple integrated database/storage backends (Authzed CRDB, Authzed PG, CockroachDB, Postgres, ScyllaDB, ClickHouse, Elasticsearch, MongoDB, etc.) objectively using consistent metrics.
- Identify primary bottlenecks (CPU, I/O, locks, network, GC, query planning) and high‑impact optimization opportunities.
- Determine correlations between data structure (group hierarchy, fan‑out, ACL density) and variance in latency/throughput.
- Evaluate performance stability (inter‑run variation, jitter, p95) for production requirements.
- Propose initial configuration recommendations (pool size, batch size, indexes/views) that are practical to apply.

### 1.1 Scope
- In‑scope: bulk schema + initial data load, per‑resource permission checks, ACL listings/traversals, MV refresh (if applicable), resource utilization observations.
- Out‑of‑scope: failover/HA scenarios, durability stress tests (crash recovery), multi‑region latency, security/auth overhead.

### 1.2 Key Metrics
- Write: total load time, objects/s throughput, mean & p95 per‑batch latency, final storage footprint.
- Read: mean/median/p95 latency per query type, checks/s throughput, error & timeout rate.
- Stability: coefficient of variation (stddev/mean) across runs, maximum spikes.
- Efficiency: CPU% per **1K** operations, memory/MB per **1K** objects, index build time.

### 1.3 Success Criteria
- p95 read gap between best and worst backends < **X×**.
- Minimum write throughput meets target **N objects/s** for **1M** entities.
- Error/timeout rate ≤ **0.1%** during measured runs.
- Latency variability (p95) within acceptable bounds.

## 2. Test Environment
### 2.1 Host Hardware & OS
- Capture timestamp: **2025-12-01 05:34:51+0700**
- Model: MacBookPro16,1
- CPU: Intel(R) Core(TM) i9‑9880H @ **2.30GHz** (**8** physical / **16** logical cores)
- Physical memory: **16GB** (**17179869184** bytes)
- Disk: **1000GB** total, **878GB** free (internal SSD assumed)
- OS: macOS **26.1** (Build **25B78**)
- Kernel: Darwin **25.1.0** (xnu‑12377.41.6~2) arch x86_64

### 2.2 Go Toolchain
- Go version: **1.25.4** (from `go.mod`)
- Main module: `module test-tls`

### 2.3 Docker Runtime & Allocation
- Docker Client/Server version: **28.5.1**
- Docker Compose: **2.40.3-desktop.1**
- CPUs allocated to Docker: **8**
- Memory allocated to Docker: **7.8GB**
- Host swap (visible to VM): total **2048MB**; used **528MB**; free **1519MB**
- Deployment mode: Docker Compose (single node, local workstation)

### 2.4 Database / Service Images
- SpiceDB (CRDB backend): `authzed/spicedb:v1.46.2`
- SpiceDB (Postgres backend): `authzed/spicedb:v1.46.2`
- Envoy (CRDB): `envoyproxy/envoy:v1.33-latest`
- Envoy (Postgres): `envoyproxy/envoy:v1.33-latest`
- SpiceDB Playground: `ghcr.io/authzed/spicedb-playground:v0.2.0`
- CockroachDB: `cockroachdb/cockroach:v25.3.2` (single-node, insecure)
- Postgres: `postgres:17.2`
- ClickHouse: `clickhouse/clickhouse-server:25.10`
- ScyllaDB: `scylladb/scylla:2025.3`
- Elasticsearch: `elasticsearch:9.2.1` (single-node, security enabled)
- MongoDB: `mongo:8.2.1`
- (Debug SpiceDB image commented out: `authzed/spicedb:v1.46.2-debug`)

Note: All versions are sourced from `docker/docker-compose.yaml` and `docker/docker-compose.base.yaml` as of the baseline date. Validate with `docker compose images` before the final run.

### 2.5 Special Configuration
- Connection pool size per backend: **1**
- Max open/idle connections: **1**
- Cache size / in‑memory index: **1**
- Thread/goroutine concurrency target: **1**
- Materialized view refresh interval (CockroachDB/ClickHouse): on‑the‑fly


## 3. Dataset & Schema
Dataset size summary (generated at **2025-12-01 05:34:59**):
- Organizations: **16**
- Users (globally unique): **1,950**
- Organization memberships: **4,117**
- Groups: **170**
- Group memberships: **13,441**
- Group hierarchy edges: **14**
- Resources: **22,084**
- Resource ACL entries: **364,983**

Detailed per‑backend implementation (MV, denormalization, projections, embedding, graph edges) is provided in [DATA_SCHEMA.md](./DATA_SCHEMA.md). Methodology (Section 4) explains how distribution characteristics are used in measurement.

## 4. Benchmark Methodology
### 4.1 Execution Phases
- Prepare: `benchmark/1-prepare.sh` (generate CSV, create schema, bulk load, optional MV refresh)
- Benchmark: `benchmark/2-benchmark.sh <backend> <mode>` (modes: write/read/mixed — TBD if expanded)

### 4.2 Dataset Generator Configuration
Generation config: Orgs=**16**, Users/Org=**200**, Groups/Org=**20**, Resources/Org=**2000**, Groups/User≈**3**, Admins/Org=**10**, ManagerUsers/Res=**2**, ManagerGroups/Res=**1**, ViewerUsers/Res=**10**, ViewerGroups/Res=**3**, AvgOrgs/User≈**2**.

### 4.3 Distribution Characteristics (for Result Interpretation)
| Relationship | Mean | Min | Max | Notes |
|--------------|------|-----|-----|-------|
| org→users | **257.31** | **31** | **535** | User distribution per org (high‑skew upper bound) |
| user→orgs | **2.11** | **1** | **8** | Light multi‑tenancy; most users in **1–2** orgs |
| group→direct_members | **78.89** | **18** | **306** | Some groups are very large (**306**) → potential fan‑out hotspots |
| group→direct_managers | **1.43** | **1** | **3** | Only **21** groups have managers; minor closure impact |
| user→groups | **6.89** | **1** | **31** | Users with **31** groups increase expansion cost |
| parent_group→child_groups | **1.17** | **1** | **2** | Only **12** parents; shallow hierarchy |
| user→resources | **156.22** | **1** | **875** | **1,837** users with ≥**1** resource; outlier **875** drives tail |
| resource→users | **12.99** | **2** | **25** | Moderate ACL fan‑out; indexes & closure matter for tail |

Note: High skew (user with **875** resources, group with **306** members) is used to analyze tail latency and the effectiveness of closure/indexing.

### 4.4 Scenario Lookup IDs
- `BENCH_LOOKUPRES_MANAGE_USER` = **1388** (high‑manage exposure)
- `BENCH_LOOKUPRES_VIEW_USER` = **1539** (representative viewer)
Used as anchors for query reproducibility (e.g., resource listing or permission enumeration for contrasting users).

### 4.5 Write & Read Phases
- Write: schema creation, bulk ingest, optional MV/closure build (PostgreSQL/CockroachDB recursive MV, ClickHouse streaming MV, Scylla expansion tables, Mongo embedding, SpiceDB relationship ingestion).
- Read: permission check batches (single resource→user, user→resource enumeration, relation‑based listings), membership traversal, fan‑out sampling.

### 4.6 Refresh / Rebuild Closure
- CockroachDB/PostgreSQL: `REFRESH MATERIALIZED VIEW` executed post‑ingest.
- ClickHouse: streaming MV; an ingest‑to‑closure availability lag is observed.
- ScyllaDB: perms‑by‑user/resource tables populated via build process (timing TBD).
- MongoDB: direct embedding during load; no refresh required.
- SpiceDB: eventual consistency depends on commit sequence; readiness measured via sampled permission checks.

## 5. Metrics Captured
- Write throughput: rows/s or objects/s during ingest.
- Write latency: mean, p95 per batch insert/transaction.
- Read throughput: checks/s or queries/s.
- Read latency: mean, median, p95 per query type.
- Error rate: operation failures/timeouts/retries.
- Resource usage: CPU%, memory RSS, disk I/O, network I/O.
- Final data size: total size per backend (on‑disk/logical).

## 6. Results Summary
### 6.1 Write Performance (Ingest & Closure Build)
| Backend | Ingest Time (s) | Objects/s (approx) | p95 Batch Latency (s) | MV/Closure Build Time | Notes |
|---------|-----------------|--------------------|----------------------|-----------------------|---------|
| ClickHouse   | 3.62 | 111,700 | 0.098 | ~0.06s (expanded members) | Batch=10k ACL rels; 35 diffs |
| Elasticsearch| 6.61 | 61,230  | 0.061 | N/A | Batch=**100k** ACL rels; **3** progress samples |
| Postgres     | 21.50 | 18,926  | 0.011 | 15.23s (MV refresh) | Batch=**10k** ACL rels; tight distribution |
| Authzed PG   | 26.35 | 15,371  | 0.73  | N/A (streaming) | Batch=10k ACL rels; p95 (34th/35) |
| CockroachDB  | 34.26 | 11,879  | 0.89* | 356.46s (MV refresh) | *Approx p95 per **5k** ACL rels (est) |
| ScyllaDB     | 39.03 | 10,373  | N/A   | 32.59s (closure + perms build) | No incremental ACL progress lines |
| Authzed CRDB | 47.66 | 8,489   | 1.56  | N/A (streaming) | Batch=10k ACL rels; p95 from 35 diffs |
| MongoDB      | 53.97 | 7,501   | 1.78  | N/A (embedded) | Batch=**10k** ACL rels; p95 (**34th/35**) |

Monitor Summary (CPU/RAM)
| Backend | CPU Mean (%) | CPU Min (%) | CPU Max (%) | RAM Mean (%) | RAM Min (%) | RAM Max (%) | Samples |
|---------|---------------|-------------|-------------|--------------|-------------|-------------|---------|
| authzed_crdb | 10.40 | 0.00 | 45.75 | 5.35 | 0.00 | 18.77 | 259 |
| authzed_pgdb | 5.81 | 0.00 | 58.78 | 1.64 | 0.00 | 18.76 | 127 |
| cockroachdb | 16.14 | 0.00 | 58.16 | 24.78 | 0.00 | 29.33 | 272 |
| postgres | 8.21 | 0.02 | 25.50 | 2.80 | 0.33 | 7.91 | 33 |
| scylladb | 37.63 | 0.00 | 111.07 | 47.74 | 0.00 | 72.88 | 46 |
| clickhouse | 19.73 | 0.14 | 51.84 | 13.83 | 0.91 | 17.40 | 68 |
| elasticsearch | 12.18 | 0.19 | 50.92 | 15.87 | 14.14 | 16.08 | 48 |
| mongodb | 8.69 | 0.00 | 16.21 | 6.32 | 0.00 | 9.93 | 62 |

### 6.2 Read Performance & Scenario Latency
This section breaks down each read benchmark scenario into its own subsection with descriptions and metric tables.

#### 6.2.1 Scenario: check_manage_direct_user
Description: Measures `CheckPermission(permission="manage")` latency on resources where the user is directly assigned as `manager_user` (shortest path without org/group hierarchy).
Optimization: if `BENCH_LOOKUPRES_MANAGE_USER` is set, the resource list for that user is streamed first, then checks proceed on‑the‑fly; otherwise, stream all `resource.manager_user` relations.
Environment vars: `BENCH_CHECK_DIRECT_SUPER_ITER` (iterations), `BENCH_LOOKUPRES_MANAGE_USER` (optional heavy user), `BENCH_LOOKUP_SAMPLE_LIMIT` (per‑lookup stream cap), per‑check timeout **2s**.
| Backend | Iterations (cfg) | Mean (ms) | p95 (ms) | Min (ms) | Max (ms) | Sample Limit | Resources/User (heavy) | Notes |
|---------|------------------|-----------|----------|----------|----------|--------------|--------------------------|-------|
| Elasticsearch | 1000 | 0.000065 | 0.000198 | 0.000036 | 0.000331 | 100 | — | Samples every 100; **3 runs**; nanoscale durations |
| MongoDB      | 1000 | 0.6335 | 0.9312 | 0.4708 | 1.0704 | 100 | — | Samples every 100; **3 runs** |
| ScyllaDB     | 1000 | 1.7895 | 4.6829 | 0.6937 | 9.8409 | 100 | 221 | Samples every 100; **3 runs** |
| Postgres     | 1000 | 1.9171 | 9.9439 | 0.7061 | 10.64 | 100 | 821 | Samples every 100; **3 runs** |
| CockroachDB  | 1000 | 2.2785 | 4.3518 | 1.3896 | 6.3295 | 100 | 221 | Samples every 100; **3 runs** |
| Authzed PG   | 1000 | 2.6305 | 5.0641 | 1.1290 | 9.9066 | 100 | 821 | Samples every 100; **3 runs** |
| ClickHouse   | 1000 | 6.8327 | 9.4276 | 4.1136 | 10.72 | 100 | 221 | Samples every 100; **3 runs** |
| Authzed CRDB | 1000 | 9.2648 | 14.93 | 4.7677 | 19.64 | 100 | 821 | Sampled every 100; aggregated **3 runs** |

Sampling Notes 6.2.1: mean/p95 are computed from samples taken every **100** iterations (iter=**0,100,..,900**) across **3 runs** (≈**30** samples/backend). p95 uses the sorted index approximation; Min/Max reflect sampled range; true tail may differ slightly.

Monitor Summary (CPU/RAM)
| Backend | CPU Mean (%) | CPU Min (%) | CPU Max (%) | RAM Mean (%) | RAM Min (%) | RAM Max (%) | Samples |
|---------|---------------|-------------|-------------|--------------|-------------|-------------|---------|
| authzed_crdb | 10.40 | 0.00 | 45.75 | 5.35 | 0.00 | 18.77 | 259 |
| authzed_pgdb | 5.81 | 0.00 | 58.78 | 1.64 | 0.00 | 18.76 | 127 |
| cockroachdb | 16.14 | 0.00 | 58.16 | 24.78 | 0.00 | 29.33 | 272 |
| postgres | 8.21 | 0.02 | 25.50 | 2.80 | 0.33 | 7.91 | 33 |
| scylladb | 37.63 | 0.00 | 111.07 | 47.74 | 0.00 | 72.88 | 46 |
| clickhouse | 19.73 | 0.14 | 51.84 | 13.83 | 0.91 | 17.40 | 68 |
| elasticsearch | 12.18 | 0.19 | 50.92 | 15.87 | 14.14 | 16.08 | 48 |
| mongodb | 8.69 | 0.00 | 16.21 | 6.32 | 0.00 | 9.93 | 62 |

#### 6.2.2 Scenario: check_manage_org_admin
Description: Measures `CheckPermission(permission="manage")` latency via the organization path: resources relate to `resource.org` and the user receives permission via `organization.admin_user`. Each resource requires streaming discovery of the organization’s first admin (no cache). The admin lookup overhead influences tail latency.
Environment vars: `BENCH_CHECK_ORGADMIN_ITER`, `BENCH_LOOKUPRES_MANAGE_USER` (optional), `BENCH_LOOKUP_SAMPLE_LIMIT`.
| Backend | Iterations (cfg) | Mean (ms) | p95 (ms) | Admin Overhead (ms mean) | Notes |
|---------|------------------|-----------|----------|--------------------------|-------|
| Elasticsearch | 1000 | 0.000056 | 0.000140 | ~0.000015 | Query + filter chain (nano-scale) |
| MongoDB      | 1000 | 0.7087 | 1.1171 | 0.56 | Embedded admin array lookup; **3 runs** |
| Postgres     | 1000 | 0.9000 | 1.1103 | -0.77 | Index join cost minimized; **3 runs** |
| ScyllaDB     | 1000 | 0.9630 | 1.1299 | -0.33 | Partition scan + cache benefit; **3 runs** |
| Authzed PG   | 1000 | 1.5176 | 1.9755 | -0.54 | Streaming + txn overhead; **3 runs** |
| CockroachDB  | 1000 | 2.3442 | 3.0288 | -0.26 | MV + relation join; **3 runs** |
| ClickHouse   | 1000 | 5.4736 | 7.4860 | -2.22 | Columnar filter; **3 runs** |
| Authzed CRDB | 1000 | 8.7624 | 13.99 | 0.32 | Sampled every 100; overhead vs direct mean; **3 runs** |

Monitor Summary (CPU/RAM)
| Backend | CPU Mean (%) | CPU Min (%) | CPU Max (%) | RAM Mean (%) | RAM Min (%) | RAM Max (%) | Samples |
|---------|---------------|-------------|-------------|--------------|-------------|-------------|---------|
| authzed_crdb | 10.40 | 0.00 | 45.75 | 5.35 | 0.00 | 18.77 | 259 |
| authzed_pgdb | 5.81 | 0.00 | 58.78 | 1.64 | 0.00 | 18.76 | 127 |
| cockroachdb | 16.14 | 0.00 | 58.16 | 24.78 | 0.00 | 29.33 | 272 |
| postgres | 8.21 | 0.02 | 25.50 | 2.80 | 0.33 | 7.91 | 33 |
| scylladb | 37.63 | 0.00 | 111.07 | 47.74 | 0.00 | 72.88 | 46 |
| clickhouse | 19.73 | 0.14 | 51.84 | 13.83 | 0.91 | 17.40 | 68 |
| elasticsearch | 12.18 | 0.19 | 50.92 | 15.87 | 14.14 | 16.08 | 48 |
| mongodb | 8.69 | 0.00 | 16.21 | 6.32 | 0.00 | 9.93 | 62 |

#### 6.2.3 Scenario: check_view_via_group_member
Description: Measures `CheckPermission(permission="view")` via the group path: resources have `viewer_group`, and the user is a direct member (or manager) of that group. No transitive expansion; only direct relationships (`direct_member_user` or fallback `direct_manager_user`).
Environment vars: `BENCH_CHECK_VIEW_GROUP_ITER`, `BENCH_LOOKUPRES_VIEW_USER`, `BENCH_LOOKUP_SAMPLE_LIMIT`.
| Backend | Iterations (cfg) | Mean (ms) | p95 (ms) | Member Discovery (ms mean) | Notes |
|---------|------------------|-----------|----------|---------------------------|-------|
| Elasticsearch | 1000 | 0.000058 | 0.000153 | 0.000043 | Term + membership query; **3 runs** |
| ScyllaDB     | 1000 | 1.0156 | 1.9060 | -0.23 | Partition + perms table; **3 runs** |
| Postgres     | 1000 | 1.0938 | 1.3767 | -0.74 | Index nested loop (faster); **3 runs** |
| MongoDB      | 1000 | 1.4092 | 1.7445 | 1.11 | Embedded group membership; **3 runs** |
| CockroachDB  | 1000 | 2.7267 | 2.5154 | -0.22 | MV assisted join; **3 runs** |
| Authzed PG   | 1000 | 3.1226 | 6.1854 | 0.21 | Two-hop relational; **3 runs** |
| ClickHouse   | 1000 | 5.1168 | 7.8557 | -2.59 | Columnar filter sequence; **3 runs** |
| Authzed CRDB | 1000 | 10.44 | 14.17 | 1.24 | Sampled every 100; two-hop path; **3 runs** |

Monitor Summary (CPU/RAM)
| Backend | CPU Mean (%) | CPU Min (%) | CPU Max (%) | RAM Mean (%) | RAM Min (%) | RAM Max (%) | Samples |
|---------|---------------|-------------|-------------|--------------|-------------|-------------|---------|
| authzed_crdb | 10.40 | 0.00 | 45.75 | 5.35 | 0.00 | 18.77 | 259 |
| authzed_pgdb | 5.81 | 0.00 | 58.78 | 1.64 | 0.00 | 18.76 | 127 |
| cockroachdb | 16.14 | 0.00 | 58.16 | 24.78 | 0.00 | 29.33 | 272 |
| postgres | 8.21 | 0.02 | 25.50 | 2.80 | 0.33 | 7.91 | 33 |
| scylladb | 37.63 | 0.00 | 111.07 | 47.74 | 0.00 | 72.88 | 46 |
| clickhouse | 19.73 | 0.14 | 51.84 | 13.83 | 0.91 | 17.40 | 68 |
| elasticsearch | 12.18 | 0.19 | 50.92 | 15.87 | 14.14 | 16.08 | 48 |
| mongodb | 8.69 | 0.00 | 16.21 | 6.32 | 0.00 | 9.93 | 62 |

#### 6.2.4 Scenario: lookup_resources_manage_super
Description: Measures resource enumeration for heavy‑exposure users with permission "manage" using `LookupResources`. Focus on streaming throughput and total resource count. Each iteration streams all accessible resources without full buffering (counting only).
Environment vars: `BENCH_LOOKUPRES_MANAGE_ITER`, `BENCH_LOOKUPRES_MANAGE_USER`, per‑iteration timeout **60s**.
| Backend | Iterations | Mean Iter Dur (ms) | p95 Iter Dur (ms) | Total Resources | Mean Resources/Iter | Last Iter Count | Notes |
|---------|-----------|--------------------|-------------------|-----------------|----------------------|------------------|-------|
| ScyllaDB     | 10 | 1.2667 | 2.0000 | 2210 | 221 | 221 | Partition enumeration; **3 runs** |
| CockroachDB  | 10 | 1.4333 | 2.0000 | 2210 | 221 | 221 | MV + scan cost; **3 runs** |
| Postgres     | 10 | 1.5667 | 3.0000 | 8210 | 821 | 821 | Sequential/Index scan; **3 runs** |
| ClickHouse   | 10 | 12.07 | 17.00 | 2210 | 221 | 221 | Columnar large scan; **3 runs** |
| Authzed PG   | 10 | 15.20 | 28.00 | 8210 | 821 | 821 | Backend PG path; **3 runs** |
| Authzed CRDB | 10 | 28.60 | 38.00 | 8210 | 821 | 821 | Heavy fan-in streaming; **3 runs** |
| MongoDB      | 10 | 320.20 | 474.00 | 620 | 62 | 62 | Cursor enumeration; **3 runs** |
| Elasticsearch| 10 | 937.10 | 1205.00 | 98210 | 9821 | 9821 | Scroll/scan throughput; **3 runs** |

Monitor Summary (CPU/RAM)
| Backend | CPU Mean (%) | CPU Min (%) | CPU Max (%) | RAM Mean (%) | RAM Min (%) | RAM Max (%) | Samples |
|---------|---------------|-------------|-------------|--------------|-------------|-------------|---------|
| authzed_crdb | 10.40 | 0.00 | 45.75 | 5.35 | 0.00 | 18.77 | 259 |
| authzed_pgdb | 5.81 | 0.00 | 58.78 | 1.64 | 0.00 | 18.76 | 127 |
| cockroachdb | 16.14 | 0.00 | 58.16 | 24.78 | 0.00 | 29.33 | 272 |
| postgres | 8.21 | 0.02 | 25.50 | 2.80 | 0.33 | 7.91 | 33 |
| scylladb | 37.63 | 0.00 | 111.07 | 47.74 | 0.00 | 72.88 | 46 |
| clickhouse | 19.73 | 0.14 | 51.84 | 13.83 | 0.91 | 17.40 | 68 |
| elasticsearch | 12.18 | 0.19 | 50.92 | 15.87 | 14.14 | 16.08 | 48 |
| mongodb | 8.69 | 0.00 | 16.21 | 6.32 | 0.00 | 9.93 | 62 |

#### 6.2.5 Scenario: lookup_resources_view_regular
Description: Measures resource enumeration for regular users with permission "view" as the baseline for average access density. Compares latency/resource counts to the heavy‑user scenario.
Environment vars: `BENCH_LOOKUPRES_VIEW_ITER`, `BENCH_LOOKUPRES_VIEW_USER`, per‑iteration timeout **60s**.
| Backend | Iterations | Mean Iter Dur (ms) | p95 Iter Dur (ms) | Total Resources | Mean Resources/Iter | Last Iter Count | Notes |
|---------|-----------|--------------------|-------------------|-----------------|----------------------|------------------|-------|
| ScyllaDB     | 10 | 0.5667 | 1.0000 | 950 | 95 | 95 | Wide partition scan; **3 runs** |
| CockroachDB  | 10 | 2.1333 | 4.0000 | 950 | 95 | 95 | MV read path; **3 runs** |
| Postgres     | 10 | 3.7000 | 5.0000 | 23600 | 2360 | 2360 | Index scan baseline; **3 runs** |
| ClickHouse   | 10 | 11.53 | 16.00 | 950 | 95 | 95 | Columnar projection; **3 runs** |
| Authzed PG   | 10 | 60.37 | 84.00 | 31450 | 3145 | 3145 | Baseline PG path; **3 runs** |
| Authzed CRDB | 10 | 121.50 | 182.00 | 31450 | 3145 | 3145 | Baseline streaming; **3 runs** |
| MongoDB      | 10 | 160.40 | 239.00 | 660 | 66 | 66 | Cursor baseline; **3 runs** |
| Elasticsearch| 10 | 1081.03 | 1275.00 | 100000 | 10000 | 10000 | Query/filter baseline; **3 runs** |

Monitor Summary (CPU/RAM)
| Backend | CPU Mean (%) | CPU Min (%) | CPU Max (%) | RAM Mean (%) | RAM Min (%) | RAM Max (%) | Samples |
|---------|---------------|-------------|-------------|--------------|-------------|-------------|---------|
| authzed_crdb | 10.40 | 0.00 | 45.75 | 5.35 | 0.00 | 18.77 | 259 |
| authzed_pgdb | 5.81 | 0.00 | 58.78 | 1.64 | 0.00 | 18.76 | 127 |
| cockroachdb | 16.14 | 0.00 | 58.16 | 24.78 | 0.00 | 29.33 | 272 |
| postgres | 8.21 | 0.02 | 25.50 | 2.80 | 0.33 | 7.91 | 33 |
| scylladb | 37.63 | 0.00 | 111.07 | 47.74 | 0.00 | 72.88 | 46 |
| clickhouse | 19.73 | 0.14 | 51.84 | 13.83 | 0.91 | 17.40 | 68 |
| elasticsearch | 12.18 | 0.19 | 50.92 | 15.87 | 14.14 | 16.08 | 48 |
| mongodb | 8.69 | 0.00 | 16.21 | 6.32 | 0.00 | 9.93 | 62 |

#### 6.2.6 Comparison Summary
| Scenario | Mean (ms) | p95 (ms) | Workload Type | Dominant Overhead | Notes |
|----------|-----------|----------|---------------|------------------|---------|
| check_manage_direct_user | 7.57 | 10.82 | Single check | RPC path | Sampled every 100; 5x1000 iters |
| check_manage_org_admin | 7.89 | 10.79 | Single check | Admin discovery | Overhead ~0.32ms vs direct |
| check_view_via_group_member | 8.81 | 14.12 | Single check | Group member fetch | Two-hop; member discovery ~1.24ms |
| lookup_resources_manage_super | 27.30 | 49 | Lookup streaming | Enumeration volume | 50 iters (5x10); heavy user enumeration |
| lookup_resources_view_regular | 104.43 | 129 | Lookup streaming | Enumeration volume | 50 iters (5x10); regular user enumeration |

Initial Interpretation Notes:
- Direct manager path is expected to be fastest; heavy lookups highlight streaming throughput limits.
- p95 differences across scenarios indicate effectiveness of backend data structures.

## 7. Write Phase Analysis
- ClickHouse: fastest ingest (**3.62s**) with batch p95 **0.098s**; streaming MV exposes closures quickly. Suited to large loads needing rapid availability for columnar queries.
- Elasticsearch: fast ingest (**6.61s**) with p95 **0.061s** and no MV. Good for large search/listing scenarios; permission modeling requires disciplined queries for accuracy.
- Postgres: moderate ingest (**21.5s**), very low batch p95 (**0.011s**). `REFRESH MATERIALIZED VIEW` costs **15.23s**—significant yet predictable. Strong for OLTP plus MV‑accelerated lookups.
- CockroachDB: ingest **34.26s**; `REFRESH MATERIALIZED VIEW` is very expensive (**356.46s**), raising read‑readiness TCO post‑load. Needs incremental/alternative MV strategy.
- ScyllaDB: ingest **39.03s**; closure/perms build **32.59s**. Precomputed tables yield excellent read latency at a separate build cost.
- Authzed (SpiceDB) PG: ingest **26.35s**; batch p95 **0.73s** (streaming). No MV; relies on internal traversal/cache.
- Authzed (SpiceDB) CRDB: ingest **47.66s**; batch p95 **1.56s** (streaming). CRDB path is slower than PG on this dataset.
- MongoDB: ingest **53.97s**; p95 **1.78s**. Simple embedding strategy, but slower ingest and higher p95.

Implications: For pipelines prioritizing read‑ready time post‑ingest, Postgres+MV and ClickHouse stand out. To control operational cost, avoid large full MV refreshes in CockroachDB or use incremental alternatives.

## 8. Read Phase Analysis
- Single‑check (direct user): fastest Elasticsearch (≈**0.000065ms**; likely in‑process nanosecond measurement), followed by MongoDB (**0.6335ms**), ScyllaDB (**1.7895ms**), Postgres (**1.9171ms**), CockroachDB (**2.2785ms**), Authzed PG (**2.6305ms**), ClickHouse (**6.8327ms**), and Authzed CRDB (**9.2648ms**).
- Single‑check (org admin): ordering shows Postgres (**0.9000ms**), Scylla (**0.9630ms**), Authzed PG (**1.5176ms**), Cockroach (**2.3442ms**), ClickHouse (**5.4736ms**), Authzed CRDB (**8.7624ms**). Elasticsearch remains nanosecond‑scale (**0.000056ms**).
- Single‑check (via group member): best Postgres (**1.0938ms**), then Scylla (**1.0156ms**), MongoDB (**1.4092ms**), Cockroach (**2.7267ms**), Authzed PG (**3.1226ms**), ClickHouse (**5.1168ms**), Authzed CRDB (**10.44ms**). Elasticsearch is nanosecond‑scale (**0.000058ms**).
- Enumeration heavy (manage_super): Scylla (**1.2667ms**), Cockroach (**1.4333ms**), Postgres (**1.5667ms**), ClickHouse (**12.07ms**), Authzed PG (**15.20ms**), Authzed CRDB (**28.60ms**), MongoDB (**320.20ms**), Elasticsearch (**937.10ms**). ES enumerates ≈**9,821** resources/iter; Mongo ≈**62**.
- Enumeration regular (view_regular): Scylla (**0.5667ms**), Cockroach (**2.1333ms**), Postgres (**3.7000ms**), ClickHouse (**11.53ms**), Authzed PG (**60.37ms**), Authzed CRDB (**121.50ms**), MongoDB (**160.40ms**), Elasticsearch (**1081.03ms**) with ≈**10k/iter**.

Interpretation: Row‑store engines with indexes/MV (Postgres, Cockroach) and precomputed tables (Scylla) provide consistently low latency for single checks and typical enumeration. The SpiceDB‑CRDB path exhibits higher overhead than SpiceDB‑PG. Elasticsearch figures are in the nanosecond scale and appear to be extremely local measurements; compare against enumeration scenarios (significantly costlier) for true throughput context.

Method note: p95 computed from samples every **100** iterations × **3 runs** (≈**30** samples), so extreme tails may not be fully captured.

## 9. Validity & Limitations
- Sampling: only every **100** iterations for single‑checks; p95 based on ~**50** samples per backend per scenario. Actual p95 from **5,000** checks may differ slightly.
- Unit normalization: all durations normalized to ms; nanoseconds (Elasticsearch) yield extremely small numbers and do not represent network round‑trip.
- Runtimes: single‑node Docker tests, no separate concurrency load; minimal cache priming, resulting in a mixed cold/warm profile.
- MV/closure: refresh/build costs executed serially; in production, consider scheduling or incremental strategies.
- Dataset skew: outliers (user with **875** resources, group with **306** members) affect tails; desirable for robustness testing but makes mean comparisons more sensitive.
- Logging: not all backends emit equally rich metrics; interpretation done as uniformly as possible.

## 10. Recommendations
- Postgres: use segmented or incremental MV refresh to reduce the **15.23s** cost; ensure indexes on ACL relation columns and join keys.
- CockroachDB: avoid full MV refresh (**356.46s**); evaluate alternative designs (index‑only + join) or selective denormalization.
- ScyllaDB: schedule perms/closure builds off‑peak; verify TTL/compaction to keep read tails low.
- ClickHouse: keep streaming MV; add projections/skip indexes for ACL filters; tune `max_threads` to allocated CPUs (e.g., **8**).
- Authzed (PG/CRDB): enable/optimize dispatch caching, use consistent zedtokens, consider batching checks to reduce RPC overhead.
- Elasticsearch: re‑measure with HTTP round‑trip (not in‑process nanoseconds) and constrain result sets via tight filters; target enumerations of **~10k/iter** only when necessary.
- MongoDB: ensure indexes on embedded ACL fields and use pipelines that avoid full scans in heavy enumerations; validate cursor batch sizes for **60–160ms** ranges.
- General: run concurrency sweeps (**1**, **8**, **32** workers) to measure scaling; add per‑scenario tracing to pinpoint hotspots; keep p95 validation on **≥50** samples when possible.

## 11. Risks & Considerations
- Heavy MV in CockroachDB: long refresh times (e.g., **356.46s**) risk post‑ingest SLAs; requires incremental strategies or redesign.
- Eventual consistency: SpiceDB and Elasticsearch paths may exhibit temporal anomalies without consistent zedtokens/versioning; validate with consistent snapshots and **per‑run** tokens.
- Footprint & cost: MV/denormalization increases storage; weigh rebuild costs against latency savings, especially for datasets with **>300k** ACL relations.
- Skew & hotspots: outlier users/groups (e.g., user with **875** resources; group with **306** members) can produce unexpected tails; consider rate limiting or precomputation for outliers.
- Cross‑engine comparability: enumeration results aren’t apples‑to‑apples (e.g., Elasticsearch extracts ≈**10k resources/iter**), so interpret in context of volume differences.

## 12. Conclusions
- Fastest writes: ClickHouse, followed by Elasticsearch. For OLTP + lookups, Postgres offers a strong balance with MV (despite refresh cost).
- Single‑check reads: excluding Elasticsearch’s nanosecond numbers, Postgres/Scylla/Cockroach are the most consistently low; SpiceDB‑PG is faster than SpiceDB‑CRDB on this dataset.
- Enumeration: Postgres/Scylla/Cockroach are significantly stronger; ClickHouse is competent but not as fast as row‑stores for granular lookups.
- Baseline recommendation: Postgres with managed MV for mixed ingest/read; use Scylla for ultra‑low latency; use ClickHouse where ingest + columnar analytics dominate.
- Next steps: measure under concurrency (**1**, **8**, **32** workers), validate p95 with full samples (prefer **≥50**), and deepen observability (profiling/tracing) to confirm specific bottlenecks.