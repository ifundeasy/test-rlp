# Prompt: Benchmark Report Generation & Maintenance (test-rlp)

You are an expert technical writer and tooling engineer maintaining the benchmark documentation for this repository. Produce high‑quality, reproducible updates to the benchmark report based on parsed logs and agreed formatting rules.

## Repository Context
- Project root: `test-rlp`
- Primary report: `BENCHMARK.md`
- Data schema reference: `DATA_SCHEMA.md`
- Read benchmark log: `benchmark/3-3-benchmark.log`
- (Optional) Write/ingest log: `benchmark/3-2-load-data.log`
- Parsers: `benchmark/parse_all.go`, `benchmark/parse_direct_user.go`, `benchmark/parse_monitor.go`

## Objectives
- Parse read benchmark scenarios from `benchmark/3-3-benchmark.log` and ensure sections 6.2.1–6.2.5 in `BENCHMARK.md` are complete and accurate.
- Sort each 6.2.x table by ascending Mean latency (“best first”). Backends with zero/absent samples must be placed last.
- Remove any “Completed” column from 6.2.x tables (do not include it).
- Include Elasticsearch and MongoDB entries for 6.2.1 `check_manage_direct_user` using actual log samples.
- Maintain unit normalization (ns/µs/s → ms) and the agreed p95 calculation on sparse samples.
- Populate narrative sections 7–12 derived from the data (write analysis, read analysis, validity & limitations, recommendations, risks, conclusions).
- Ensure the entire document is in formal technical English. In narrative text, emphasize quantified values using bold (e.g., **5 runs**, **50 samples**). Do not bold numbers in tables.

Monitor Tables (new requirement):
- Parse engine monitor data from `benchmark/2-monitor.log` and add CPU/RAM summary tables under sections 6.1 and 6.2.1–6.2.5.
- Compute per‑engine min, max, and mean for CPU% and RAM% using the log lines emitted by `2-monitor.sh`.
- Normalize CPU% so that **100% equals fully consuming the allocated Docker CPUs** (divide raw Docker CPU% by the allocated CPUs, e.g., **8**).
- Table columns must be: `| Backend | CPU Mean (%) | CPU Min (%) | CPU Max (%) | RAM Mean (%) | RAM Min (%) | RAM Max (%) | Samples |` (no Notes column).
- Rows grouped by engine name (e.g., `authzed_crdb`, `postgres`, `cockroachdb`, etc.). If an engine has zero samples, omit it.

Monitor Parser Usage (when and where):
- Run the monitor parser immediately after a benchmark run and before updating section 6 in `BENCHMARK.md`.
- Command: `go run ./benchmark/parse_monitor.go --cpus <allocated_cpus> benchmark/2-monitor.log` (use your Docker Desktop CPU allocation, e.g., `--cpus 8`).
- Paste the generated markdown table:
  - Under 6.1, directly after the main ingest table, titled “Monitor Summary (CPU/RAM)”.
  - Under each of 6.2.1–6.2.5, directly after the scenario table, titled “Monitor Summary (CPU/RAM)”.
-
Note: Re-run the parser whenever `benchmark/2-monitor.log` changes to keep these tables in sync.

## Scenarios & Sampling
- Streaming scenarios: `check_manage_direct_user`, `check_manage_org_admin`, `check_view_via_group_member`.
- Enumeration scenarios: `lookup_resources_manage_super`, `lookup_resources_view_regular`.
- Sampling cadence for streaming scenarios: every **100** iterations (iter=**0,100,..,900**) across **5** runs (≈**50** samples/backend).
- p95 definition on sampled data: sort samples and take index `floor(0.95*N) - 1`, clamped to bounds.
- Normalize all durations to milliseconds.

## Table Requirements
- 6.2.1 header:
  - `| Backend | Iterations (cfg) | Mean (ms) | p95 (ms) | Min (ms) | Max (ms) | Sample Limit | Resources/User (heavy) | Notes |`
- 6.2.2 header:
  - `| Backend | Iterations (cfg) | Mean (ms) | p95 (ms) | Admin Overhead (ms mean) | Notes |`
- 6.2.3 header:
  - `| Backend | Iterations (cfg) | Mean (ms) | p95 (ms) | Member Discovery (ms mean) | Notes |`
- 6.2.4 header:
  - `| Backend | Iterations | Mean Iter Dur (ms) | p95 Iter Dur (ms) | Total Resources | Mean Resources/Iter | Last Iter Count | Notes |`
- 6.2.5 header:
  - `| Backend | Iterations | Mean Iter Dur (ms) | p95 Iter Dur (ms) | Total Resources | Mean Resources/Iter | Last Iter Count | Notes |`
- Sorting: rows ordered by ascending Mean; in equality ties, preserve original engine order. Entries with no samples/`0` should appear at the bottom.
- Do not re-add a “Completed” column anywhere.

Monitor Table placement:
- Add the CPU/RAM monitor table immediately after the main table in 6.1 and after each scenario table in 6.2.1–6.2.5, titled “Monitor Summary (CPU/RAM)”.

## Special Handling & Notes
- 6.2.1 must include Elasticsearch and MongoDB (they exist near the end of the log). Use actual samples to compute mean/p95/min/max.
- For 6.2.1, if a value like “Resources/User (heavy)” is not applicable, use `—` rather than `0`.
- Treat Elasticsearch nanosecond figures as extremely local measurements (not network round‑trip); keep as parsed but call this out in analysis/notes.
- Keep all durations reported in `ms` in tables and prose (after normalization).

## Narrative Sections (7–12)
- 7 Write Phase Analysis: In formal technical English, summarize per‑backend ingest time, batch p95, and MV/closure build costs. Explicitly call out figures reflected in the tables (e.g., ClickHouse **3.62s** ingest with batch p95 **0.098s**; Elasticsearch **6.61s** with p95 **0.061s**; Postgres **21.5s** ingest and MV refresh **15.23s**; CockroachDB MV refresh **356.46s**; ScyllaDB build **32.59s**; Authzed PG/CRDB streaming p95 **0.73s**/**1.56s**; MongoDB p95 **1.78s**). End with a brief Implications paragraph mirroring BENCHMARK.md (e.g., Postgres+MV and ClickHouse stand out; avoid full MV refresh on CockroachDB).
- 8 Read Phase Analysis: Summarize winners/ordering per scenario to match the 6.2.x tables; note Elasticsearch’s nanosecond‑scale for single‑checks; highlight enumeration counts (e.g., manage_super ES ≈ **9,821** resources/iter; view_regular ES ≈ **10k/iter**). Include a Method note: sampling every **100** iterations × **5** runs (≈**50** samples), p95 via sorted index approximation.
- 9 Validity & Limitations: Cover sampling cadence and p95 approximation, unit normalization to ms, single‑node Docker constraints (no extra concurrency), MV/closure scheduling, dataset skew outliers (user **875** resources; group **306** members), and uneven logging richness across backends.
- 10 Recommendations: Provide concrete tuning per backend using explicit values where useful (e.g., Postgres incremental/segmented MV to reduce **15.23s**; CockroachDB avoid full MV refresh **356.46s**; Scylla TTL/compaction; ClickHouse projections/skip indexes and `max_threads`; Authzed caching + zedtoken consistency and batched checks; Mongo embedded‑field indexes). Add General recommendations: concurrency sweeps (**1**, **8**, **32** workers) and per‑scenario tracing.
- 11 Risks & Considerations: State CockroachDB MV refresh SLA risks, eventual consistency caveats for SpiceDB/Elasticsearch (need consistent zedtokens/versioning), storage vs latency trade‑offs for MV/denormalization, skew/hotspot tail risks, and cross‑engine comparability caveats (ES enumerates ≈**10k resources/iter**; not apples‑to‑apples).
- 12 Conclusions: Deliver practical takeaways aligned with the report: fastest writes (ClickHouse, then Elasticsearch); single‑check leaders (Postgres/Scylla/Cockroach; SpiceDB‑PG faster than CRDB); enumeration strength (Postgres/Scylla/Cockroach). Provide a baseline recommendation (Postgres+MV for mixed workloads; Scylla for ultra‑low latency; ClickHouse for ingest/columnar analytics) and next steps (concurrency measurements, full‑sample p95 validation, deeper tracing).

## Style & Formatting
- Language: formal technical English.
- Emphasis: in narrative sections only, bold any quantified terms (counts, durations, sizes, rates, iterations, runs), e.g., **10k**, **5 runs**, **356.46s**.
- Tables: leave numeric values un‑bolded for readability.
- Units: always in `ms` for latency, after normalization; explicitly note any seconds in narrative when relevant to build/refresh durations.
- Keep section and subsection headings as in `BENCHMARK.md`.

## Inputs & Commands (optional)
- To refresh scenario metrics:
  - `go run ./benchmark/parse_all.go benchmark/3-3-benchmark.log`
  - For direct‑user only: `go run ./benchmark/parse_direct_user.go benchmark/3-3-benchmark.log`
- Capture outputs and update the corresponding tables (6.2.1–6.2.5) sorted by Mean ascending.
- For monitor tables, you may generate via: `go run ./benchmark/parse_monitor.go --cpus 8 benchmark/2-monitor.log` and paste the resulting markdown in the specified sections.

## Acceptance Criteria
- 6.2.x tables are sorted by Mean ascending and contain complete rows for all available backends, including Elasticsearch and MongoDB in 6.2.1.
- No “Completed” column remains in any 6.2.x table.
- Units are normalized to ms; p95 computed with the agreed sampled methodology.
- Sections 7–12 are populated with concise, data‑driven analysis, recommendations, risks, and conclusions.
- Narrative is in formal technical English with quantified terms highlighted in bold.
- The rest of `BENCHMARK.md` structure remains consistent and readable.

## Non‑Goals
- Do not modify parser code unless required to correctly parse new log formats.
- Do not change benchmark methodology or dataset without explicit instruction.

## Deliverable
- An updated `BENCHMARK.md` meeting the Acceptance Criteria above.
