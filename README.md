# test-rlp
> row level permission

Small Go playground for:

- defining schemas
- loading fixture data
- running read benchmarks

across different backends (Authzed, PostgreSQL, MongoDB, Redis, etc.).

The `cmd/` folder contains one-off commands (create schema, drop schema,
load fixtures, benchmark reads), and `infrastructure/` contains the shared
client/connection code for each backend.

---

## Project layout

Rough structure:

```text
.
├── cmd
│   ├── csv/
│   │   └── load_data.go
│   ├── authzed_crdb/
│   │   ├── benchmark_reads.go
│   │   ├── create_schemas.go
│   │   ├── load_data.go
│   │   ├── ...
│   │   └── drop_schemas.go
│   ├── authzed_pgdb/
│   │   └── ...
│   ├── clickhouse/
│   │   └── ...
│   ├── cockroackdb/
│   │   └── ...
│   ├── elasticsearch/
│   │   └── ...
│   ├── mongodb/
│   │   └── ...
│   ├── postgres/
│   │   └── ...
│   ├── scylladb/
│   │   └── ...
│   └── main.go
├── data
│   └── ... (csv data)
├── infrastructure
│   ├── authzed_crdb.go
│   ├── authzed_pgdb.go
│   ├── clickhouse.go
│   ├── cockroachdb.go
│   ├── elasticsearch.go
│   ├── mongodb.go
│   └── scylladb.go
├── go.mod
└── go.sum
````

---

## Requirements

* Go (1.21+ recommended)
* The backing services you want to test (PostgreSQL, MongoDB, Authzed/SpiceDB,
  Redis, etc.) running and reachable
* Any environment variables expected by `infrastructure/*.go`
  (connection strings, credentials, etc.)

---

## Entry point & CLI convention

The central entry point is:

```bash
go run ./cmd/main.go <module> <action>
```

### Actions

The common actions are:

* `drop`          – drop schemas / collections / relations (dangerous)
* `create-schema` – create schemas / tables / collections
* `load-data`   – load fixture data
* `benchmark`     – run read benchmarks

Not every module has to implement every action, but the interface is the same.

---

## Usage

### CSV helpers

Used to generate or prepare CSV data under `data/` that can then be imported
by the various backends.

```bash
# Generate fixture CSV data
go run ./cmd/main.go csv load-data
```

### Authzed

Basic lifecycle:

```bash
# Drop all schemas (dangerous, one-time / cleanup use)
go run ./cmd/main.go authzed_crdb drop

# Create schemas
go run ./cmd/main.go authzed_crdb create-schema

# Load test data
go run ./cmd/main.go authzed_crdb load-data

# Run read benchmarks
go run ./cmd/main.go authzed_crdb benchmark
```

### PostgreSQL (when wired)

```bash
go run ./cmd/main.go postgres drop
go run ./cmd/main.go postgres create-schema
go run ./cmd/main.go postgres load-data
go run ./cmd/main.go postgres benchmark
```

### MongoDB (when wired)

```bash
go run ./cmd/main.go mongodb drop
go run ./cmd/main.go mongodb create-schema
go run ./cmd/main.go mongodb load-data
go run ./cmd/main.go mongodb benchmark
```

You can mirror the same pattern for:

* `authzed_crdb`
* `authzed_pgdb`
* `clickhouse`
* `cockroachdb`
* `elasticsearch`
* `mongodb`
* `postgres`
* `scylladb`

once those commands are implemented.

---

## Data

The `data/` directory is intended for any fixture or sample data used by the
`*_load_data.go` commands. Format and structure is backend-specific:

* CSV for bulk imports
* JSON documents
* raw insert scripts

The `csv` module is the place to centralise data generation logic so that
benchmarks across backends are comparable.

---

## Infrastructure layer

Each file in `infrastructure/` contains the client/connection setup for a
particular backend:

* `authzed_crdb.go` and `authzed_pgdb.go` – Authzed / SpiceDB client and helpers
* `clickhouse.go` – Clickhouse client and helpers
* `cockroachdb.go` – CockroachDB client and helpers
* `elasticsearch.go` – Elasticsearch client and helpers
* `Mongodb.go` – MongoDB client and helpers
* `postgres.go` – PostgreSQL client and helpers
* `scylladb.go` – ScyllaDB client and helpers

Command files under `cmd/*` should:

* focus on orchestration (create schema, load_data, run benchmarks)
* delegate all connectivity and low-level details to the corresponding
  `infrastructure/*.go` helpers

This keeps per-backend logic isolated and makes it easier to swap
implementations or tune clients.

---

## Extending

To add a new backend `foo`:

1. Add an infrastructure client:

   ```text
   infrastructure/foo.go
   ```

2. Add commands under `cmd/foo/`:

   ```text
   cmd/foo/create_schemas.go
   cmd/foo/drop_schemas.go
   cmd/foo/load_data.go
   cmd/foo/benchmark_reads.go
   ```

3. Wire it in `cmd/main.go`:

   * add a new module case `foo`
   * support the standard actions: `drop`, `create-schema`, `load-data`, `benchmark`

Stick to the same naming and action semantics and the CLI stays predictable
as the playground grows.

```

This version:

- matches the `main.go` CLI shape (`<module> <action>`)
- documents `csv` and `authzed` explicitly
- keeps Postgres/Mongo/etc. as clearly “when wired” / future modules
- removes the outdated inline `main.go` example and fixes the Markdown issues (broken code fence, etc.)

From here you can evolve the README just by adding/removing modules and actions as you grow the playground.
```
