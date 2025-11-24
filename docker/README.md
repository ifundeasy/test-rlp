# spicedb/insecure-multi-datastore

## Overview

This setup runs:

- A single-node **CockroachDB** cluster (insecure mode)
- A single-node **PostgreSQL** instance
- **SpiceDB (CockroachDB-backed)** – service `spicedb-crdb`
- **SpiceDB (PostgreSQL-backed)** – service `spicedb-pgdb`
- **Envoy** as a gRPC TCP proxy / load balancer in front of both SpiceDB instances
- Optional **SpiceDB Playground** and other supporting services defined in `docker-compose.yaml`

Envoy exposes two gRPC endpoints on the host:

- `localhost:50051` → `spicedb-crdb` (CockroachDB datastore)
- `localhost:50052` → `spicedb-pgdb` (PostgreSQL datastore)

> ⚠️ This entire setup is for **local development and benchmarking** only:
>
> - CockroachDB runs in `--insecure` mode,
> - gRPC runs without TLS,
> - Secrets are hardcoded in `docker-compose.yaml`.

---

## 1. Start Databases and Enable `rangefeed` on CockroachDB

### 1.1 Start CockroachDB and PostgreSQL

```sh
docker compose up -d cockroachdb postgres mongodb clickhouse elasticsearch scylladb
```

### 1.2 Enable `rangefeed` on CockroachDB

SpiceDB’s Watch API on CockroachDB relies on Cockroach’s `rangefeed` / changefeed support.

1. Open the CockroachDB SQL shell:

   ```sh
   docker compose exec cockroachdb ./cockroach sql --insecure
   ```

2. Enable `rangefeed`:

   ```sql
   CREATE DATABASE rlp;
   CREATE DATABASE rls_spicedb;

   -- optional: show them
   SHOW DATABASES;
   SET CLUSTER SETTING kv.rangefeed.enabled = true;
   ```

3. Exit the SQL shell:

   ```sql
   \q
   ```

At this point:

- CockroachDB is running in insecure mode.
- `kv.rangefeed` is enabled (required if you plan to use the Watch API against `spicedb-crdb`).

---

## 2. Run SpiceDB Database Migrations

You now have **two** separate datastores, so you must run migrations for each SpiceDB instance.

### 2.1 Migrate CockroachDB-backed SpiceDB (`spicedb-crdb`)

```sh
docker compose run --rm spicedb-crdb migrate head
```

This will:

- Start a one-off `spicedb-crdb` container,
- Connect to `cockroachdb`,
- Apply the latest SpiceDB migrations,
- Exit when finished.

### 2.2 Migrate PostgreSQL-backed SpiceDB (`spicedb-pgdb`)

```sh
docker compose run --rm spicedb-pgdb migrate head
```

This will:

- Start a one-off `spicedb-pgdb` container,
- Connect to `postgres`,
- Apply the latest SpiceDB migrations for the PostgreSQL datastore,
- Exit when finished.

Make sure **both** commands succeed before starting the long-running services.

---

## 3. Run SpiceDB + Envoy

### 3.1 Start SpiceDB Instances and Envoy

After migrations:

```sh
docker compose up -d spicedb-crdb spicedb-pgdb spicedb-crdb-envoy spicedb-pgdb-envoy
```

This will:

- Start `spicedb-crdb` (CockroachDB-backed SpiceDB),
- Start `spicedb-pgdb` (PostgreSQL-backed SpiceDB),
- Start `spicedb-envoy`, which listens on:

  - `0.0.0.0:50051` → forwards to `spicedb-crdb:50051`
  - `0.0.0.0:50052` → forwards to `spicedb-pgdb:50051`
  - `0.0.0.0:9901` → Envoy admin interface (optional)

From your host:

- **CockroachDB-backed SpiceDB**

  - Host: `localhost`
  - Port: `50051`

- **PostgreSQL-backed SpiceDB**

  - Host: `localhost`
  - Port: `50052`

Both expect the same pre-shared key in gRPC metadata (e.g. `authorization` header with `spicdbgrpcpwd123`, depending on your client setup).

### 3.2 (Optional) Scale SpiceDB Instances

If you want to benchmark multi-instance SpiceDB while still using a single-node DB:

Scale Cockroach-backed SpiceDB:

```sh
docker compose up -d --scale spicedb-crdb=3 spicedb-crdb spicedb-crdb-envoy
```

Scale PostgreSQL-backed SpiceDB:

```sh
docker compose up -d --scale spicedb-pgdb=3 spicedb-pgdb spicedb-pgdb-envoy
```

Scale ALL-database-backend SpiceDB:

```sh
docker compose up -d --scale spicedb-crdb=3 --scale spicedb-pgdb=3 spicedb-crdb spicedb-pgdb spicedb-crdb-envoy spicedb-pgdb-envoy

```

Because Envoy clusters use `STRICT_DNS` + `ROUND_ROBIN` with service names `spicedb-crdb` and `spicedb-pgdb`, Docker’s internal DNS will expose multiple IPs and Envoy will load balance across the replicas.

---

## 4. (Optional) Running Playground

If `spicedb-playground` is defined in your `docker-compose.yaml`, you can start it like this:

```sh
docker compose up -d spicedb-playground
```

The playground will usually be exposed at:

```text
http://localhost:3000
```

You can configure it to talk to either:

- `spicedb-crdb` (Cockroach datastore, via Envoy `localhost:50051`), or
- `spicedb-pgdb` (Postgres datastore, via Envoy `localhost:50052`),

depending on which backend you want to inspect.

---

## 5. Run / Stop All Services

### 5.1 Start All Services

To start **everything** defined in `docker-compose.yaml` in the background:

```sh
docker compose up -d
```

### 5.2 Stop All Services (Keep Volumes)

```sh
docker compose down
```

This:

- Stops all containers,
- Removes containers,
- **Keeps** named volumes (databases and other persisted data are preserved).

### 5.3 Stop All Services and Remove Volumes

```sh
docker compose down -v
```

This:

- Stops all containers,
- Removes containers,
- Removes all named volumes (database and other persisted data are deleted).

### 5.4 Stop All Services, Remove Volumes and Images

```sh
docker compose down -v --rmi all
```

This:

- Stops and removes containers,
- Removes named volumes (all persisted data),
- Removes all images used by this compose stack.

Use this for a **full reset** of the environment.

---

## 6. Notes & Next Steps

- This setup uses:

  - **Insecure CockroachDB** (`--insecure`),
  - **Plaintext gRPC** between Envoy and SpiceDB,
  - A shared **pre-shared key** for SpiceDB gRPC authentication (`SPICEDB_GRPC_PRESHARED_KEY`).

- For production, you should:

  - Enable TLS and proper authentication for CockroachDB and PostgreSQL,
  - Run SpiceDB with TLS (`--grpc-tls-*` flags),
  - Use a **multi-node** CockroachDB or PostgreSQL cluster (or managed service),
  - Move secrets out of `docker-compose.yaml` (use env vars, secret manager, etc.),
  - Add monitoring, metrics, and proper resource limits.
