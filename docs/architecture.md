# Architecture

This document explains how data moves through the platform and why it is shaped this way. Individual decisions are recorded as ADRs in [adr/](adr/).

## 1. Data flow

```mermaid
sequenceDiagram
  autonumber
  participant App as Simulator
  participant PG as PostgreSQL
  participant DBZ as Debezium
  participant K as Kafka
  participant BR as Spark Bronze
  participant SV as Spark Silver
  participant DBT as dbt (Trino)
  participant BI as Power BI

  App->>PG: business transaction (INSERT/UPDATE/DELETE)
  PG-->>DBZ: WAL via logical replication slot (pgoutput)
  DBZ->>K: unwrapped change event (op, ts_ms, lsn, __deleted)
  K->>BR: micro-batch (every ~30 s)
  BR->>BR: append to Bronze Delta + Kafka coordinates
  BR-->>SV: Bronze Delta read as a stream
  SV->>SV: facts: dedupe by LSN, MERGE current state
  SV->>SV: dims: SCD2 versions ordered by LSN
  DBT->>SV: read Silver via Trino (every DBT_INTERVAL_SECONDS)
  DBT->>DBT: build Gold star schema + run tests
  BI->>DBT: query Gold marts via Trino
```

## 2. Components

| Component | Role | Profile |
|---|---|---|
| PostgreSQL 16 | OLTP source; `wal_level=logical`, publication over the FMCG tables, dedicated replication user | `source` |
| seed (one-shot) | Deterministic master data + ~30 days of historical orders | `source` |
| simulator | Long-running "company applications" producing weighted business events | `sim` |
| Kafka (KRaft) | Durable event log, one topic per source table (`fmcg.public.<table>`) | `kafka` |
| Kafka UI | Topic and consumer inspection | `kafka` |
| Kafka Connect + Debezium | CDC from Postgres into Kafka; fails fast on errors (no source-side DLQ, see ADR-002) | `cdc` |
| MinIO | S3-compatible object storage for Delta tables and Spark checkpoints | `lake` |
| Hive Metastore | Shared table catalog for Spark and Trino | `lake` |
| spark-bronze | Kafka → Bronze Delta (append-only) | `streaming` |
| spark-silver-facts | Bronze → Silver facts (MERGE upsert, soft deletes) | `streaming` |
| spark-silver-dims | Bronze → Silver SCD Type 2 dimensions | `streaming` |
| Trino | SQL engine over Delta via the metastore | `serving` |
| dbt / dbt-scheduler | Gold models and tests; one-shot build and a periodic loop | `serving` |
| Power BI (external) | Report on Gold, connected to Trino; not part of the Compose stack | — |

## 3. Source domain

An FMCG distributor selling to retail outlets:

- **Dimensions (SCD Type 2):** `stores`, `products`, `sales_reps`. These change slowly but their history matters: price changes, tier upgrades, reps moving region.
- **Facts (current state):** `orders`, `order_items`, `inventory`.
- **Reference:** `regions`.

The seed is loaded once into Postgres. Debezium's initial snapshot (`snapshot.mode=initial`) then emits every existing row as `op='r'`, so the seed becomes the initial Bronze load without a separate batch path. After that, live changes stream from the WAL.

## 4. Two kinds of time

| | Business time | Change time |
|---|---|---|
| Example | `orders.order_ts` | Debezium `source.ts_ms`, `source.lsn` |
| Set by | The application (may be backdated) | The database at commit (cannot be faked) |
| Used for | Analytics: sales per day, revenue today vs. yesterday | Event ordering, deduplication, SCD2 `valid_from`/`valid_to` |
| Layer | Gold | Silver |

**Rule:** Silver ordering logic always uses the LSN. Gold analytics always uses business time. Point-in-time joins in Gold connect the two by finding the dimension version whose validity window contains the order's `order_ts`.

## 5. Medallion layers

### Bronze: raw and replayable
- One streaming query subscribes to all `fmcg.public.*` topics. `foreachBatch` routes each micro-batch to `bronze.<table>`, writing the tables in parallel.
- One row per Kafka record:
  - **Payload:** the **raw JSON payload** (verbatim, so no field is ever lost to schema drift), plus `kafka_key`.
  - **CDC metadata:** the parsed `op`, `source_ts_ms`, `source_lsn` and `is_deleted`.
  - **Kafka coordinates and ingest time:** `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp` and `ingested_at`, with the table partitioned by `ingest_date`. Typed parsing is Silver's job.
- Append-only (`delta.appendOnly`). Exactly-once: the checkpoint tracks Kafka offsets, and each Delta write carries `txnAppId`/`txnVersion=batch_id`, so a batch replayed after a crash is skipped. `(kafka_topic, kafka_partition, kafka_offset)` is unique.
- Records that can't be trusted (tombstone, invalid JSON, missing `__op`/`__source_lsn`/`__source_ts_ms`) go to `bronze.quarantine` with a reason. This is the pipeline's dead-letter layer (ADR-002).

### Silver: typed and deduplicated
- Reads **Bronze Delta as a stream**, not Kafka ([ADR-003](adr/ADR-003-medallion-silver-reads-bronze.md)).
- **Facts** (`orders`, `order_items`, `inventory`): one Spark app runs one streaming query per table. Each reads `bronze.<table>` as a Delta stream, casts the raw payload to typed columns (money as `DECIMAL(12,2)`, ISO strings as `TIMESTAMP`), collapses the batch to one row per key, and `MERGE`s it with an **LSN guard** (`s.source_lsn > t.source_lsn`), so replays, re-sends and rebuilds never move a row backwards.
  - **Collapsing a batch:** the LSN, time and delete flag come from the key's newest event. Business values come from the newest **non-delete** event when there is one, so an update followed by a delete in the same batch still lands the update.
  - **Deletes are soft:** `is_deleted = true`. A delete-only batch sets just the flag and never overwrites business values.
- **Dimensions (SCD2)** (`dim_store`, `dim_product`, `dim_sales_rep`): a separate Spark app (`spark-silver-dims`) with one streaming query per dimension. Per batch and per key, events are laid out in LSN order after the stored current version.
  - **Which events count:** events at or below the key's latest LSN are ignored, and events whose `row_hash` (over the tracked attributes) equals the previous state are no-ops.
  - **Versions:** every remaining event becomes a version whose window runs to the next one's commit time. A delete closes the current version and marks it `is_deleted`.
  - **Atomic and deterministic:** one `MERGE` keyed on a deterministic surrogate key (`xxhash64(natural key, LSN)`) closes and inserts in a single commit.
  - **First version:** snapshot versions are valid from `1900-01-01` (ADR-004).
- **Regions:** kept as plain current state alongside the facts, because they never change in this domain.

### Gold: star schema
- `fct_sales` at order-line grain, point-in-time joined to `dim_store`, `dim_product`, `dim_sales_rep` and `dim_date`.
- Aggregates for BI: daily sales by region and brand, product price history, store tier movements.
- dbt tests cover keys, relationships, accepted values, and non-overlapping SCD2 windows.

## 6. Delivery guarantees

| Hop | Guarantee | Mechanism |
|---|---|---|
| Postgres → Kafka | At-least-once | Replication slot + Connect offsets; duplicates possible after a Connect crash |
| Kafka → Bronze | Exactly-once into Delta | Structured Streaming checkpoint + Delta transactional sink |
| Bronze → Silver | Effectively once | Checkpoint + MERGE guarded by LSN, which absorbs upstream duplicates and replays |
| Silver → Gold | Idempotent rebuild | dbt models are deterministic over Silver |

See [ADR-007](adr/ADR-007-idempotency.md).

## 7. Runtime and orchestration

- Docker Compose with one file per domain under `compose/`, included by the root file and gated by profiles ([ADR-008](adr/ADR-008-docker-compose-orchestration.md)).
- Startup ordering uses only `depends_on` conditions (`service_healthy`, `service_completed_successfully`), never sleeps.
- Each Spark streaming job runs in its own container in local mode. A crash means Compose restarts it, and it resumes from its checkpoint on MinIO.
- dbt runs on a simple loop (`dbt-scheduler`) as a lightweight stand-in for Airflow or Dagster.
- Python images are built from a single uv workspace lockfile ([ADR-009](adr/ADR-009-uv-packaging.md)).

## 8. Known limitations

- Debezium cannot see changes made before it started, so SCD2 history begins at the snapshot.
- JSON without embedded schemas: schema evolution is handled by hand in Spark. Avro + Schema Registry is the production path ([ADR-002](adr/ADR-002-debezium-unwrap-json.md)).
- Single-node Kafka and local-mode Spark: this is not highly available, and that is by design for a laptop.
