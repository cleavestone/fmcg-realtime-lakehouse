# Progress

Phase-by-phase build log. Updated at the end of every phase.

| Phase | Status |
|---|---|
| 0. Scaffold and architecture docs | Done |
| 1a. Source database and seed data | Done |
| 1b. Business event simulator | Done |
| 2. Kafka | Done |
| 3. Debezium CDC | Done |
| 4. Lake foundation | Done |
| 5. Bronze ingestion | Done |
| 6. Silver facts | Done |
| 7. Silver dimensions (SCD2) | Implemented, awaiting verification |
| 8. Trino and dbt Gold | — |
| 9. Power BI report | — |
| 10. Hardening and polish | — |

---

## Phase 7: Silver dimensions (SCD Type 2)

**Built**
- `common/schemas.py`: a `DimensionSpec` (natural key, surrogate key, columns, **tracked** attributes) for each dimension:
  - `stores` → `dim_store`: name, channel, region_id, tier, credit_limit
  - `products` → `dim_product`: sku, name, brand, category, pack_size, unit_price, is_active
  - `sales_reps` → `dim_sales_rep`: name, region_id

  It also adds `regions` to `FACTS` as plain current state.
- `silver/scd2.py`: the pure versioning logic.
  - **`build_changes()`:** per key, in LSN order after the stored current version, it ignores replayed events (LSN at or below the latest stored), drops no-ops (same `row_hash`), and turns every remaining event into a version whose window ends at the next boundary's commit time. A delete closes the current version with `is_deleted`. Snapshot first versions are valid from `1900-01-01`.
  - **`merge_changes()`:** one MERGE keyed on the deterministic surrogate key `xxhash64(natural key, LSN)` closes and inserts in a single commit.
- `silver/scd2_dimensions.py`: the `spark-silver-dims` app (UI on :4042), with one streaming query per dimension from `bronze.<table>` and checkpoints at `s3a://checkpoints/silver/dim_*`.
  - **Columns:** surrogate key, business columns, `row_hash`, `valid_from`, `valid_to`, `is_current`, `is_deleted`, `source_lsn`, `source_ts_ms` and `silver_updated_at`.
- `tools/scd2_check.py` and `make scd2-check`: at most one current version per key, contiguous and non-overlapping windows, `valid_from <= valid_to`, and the last version either current and open-ended or closed by a delete. Each dimension is read at **one pinned Delta version**.
- `make silver-dims-rebuild`. `make silver-facts-rebuild` now also resets `regions`. `parse()` now also returns `op`.
- `tests/test_scd2.py`: 13 tests covering the snapshot first version, a live insert, an attribute change, a no-op update, **3 changes in one batch (scrambled arrival order)**, no-ops inside a batch, snapshot plus changes in the first batch, a credit change, a delete, a change plus a delete in one batch, **replay of an old batch and a full replay**, a replayed delete, and independent keys. The Spark total is 29 tests.

**How to run**
```bash
docker compose --profile cdc --profile streaming up -d      # adds spark-silver-dims
make scenario NAME=scd2_price_test
make scenario NAME=scd2_burst_test
# ~1 minute later:
make spark-sql Q="SELECT unit_price, valid_from, valid_to, is_current FROM silver.dim_product WHERE product_id = 1 ORDER BY valid_from"
make spark-sql Q="SELECT tier, valid_from, valid_to, is_current FROM silver.dim_store WHERE store_id = 1 ORDER BY valid_from"
make scd2-check
```

**Decisions (approved)**
- **Snapshot versions are valid from `1900-01-01`.** Otherwise the 30 days of backfilled orders would match no dimension version in Gold's point-in-time join. The real snapshot time stays in `source_ts_ms`.
- **`regions` is plain current state in the facts job,** because it never changes in this domain and Gold's `dim_store` needs region names.
- **Window conventions:** half-open windows, an open end of `9999-12-31` (not NULL), and timestamps from `source_ts_ms` (commit time, ms precision). Ordering always uses `source_lsn`.

**Verified**
- **`scd2_price_test`:** product 1 has **4 versions** (50.00 → 52.50 → 50.93 → 55.00). The first starts at `1900-01-01`, each `valid_to` equals the next `valid_from`, the last is open until `9999-12-31`, and exactly one is current.
- **`scd2_burst_test`:** three tier changes to store 1 within 0.4 s became **3 separate versions** (~200 ms windows). The log shows them in **one micro-batch** (`dim=dim_store batch=1 events=3 rows_merged=4`: 1 close plus 3 inserts).
- **Live no-op update:** `UPDATE stores SET tier = tier WHERE store_id = 3` bumped `updated_at` but created **no** version.
- **SIGKILL of `spark-silver-dims` during live simulation, then restart:** `make scd2-check` gives `SCD2 INVARIANTS HOLD` (dim_store 146 keys / 250 versions, dim_product 200 / 416, dim_sales_rep 10 / 58).
- **Replay determinism:** I fingerprinted every surrogate key, row hash, window and LSN of all three dimensions, ran `make silver-dims-rebuild` (one batch replaying all of Bronze), and got **identical** fingerprints to the history built incrementally over many batches, including the kill.
- **Reconcile** (now including `regions`): exact match (orders 4,685, order_items 19,802, inventory 600, regions 5).
- **Tests:** `make test` gives 31 host tests and 29 Spark tests, all passing.
- **Memory with all three Spark jobs:** about 0.5 / 0.9 / 1.2 GB (2 GB limit each).

**Bug found while verifying (fixed)**
- **`scd2-check` mixed snapshots:** while Silver was still committing, its separate queries read different Delta versions and printed 131 keys next to 146 current rows. Each dimension is now read at one pinned version (`versionAsOf`), and the data itself was always consistent.

**Known issues**
- The Spark test suite now takes about 6–7 minutes (real Delta MERGEs per test on 2 local cores).
- The simulator never issues no-op updates or dimension deletes, so those paths are covered by unit tests and the manual no-op UPDATE above.
- Validity timestamps have millisecond precision (Debezium `source_ts_ms`). Two changes to one key committed within the same millisecond would produce a zero-length window; the invariants still hold.

---

## Phase 6: Silver facts (current state)

**Built**
- `streaming/src/streaming/common/schemas.py`: `TableSpec` (keys and typed columns) for `orders`, `order_items` and `inventory`. Money is `DECIMAL(12,2)`, discount is `DECIMAL(5,2)`, timestamps are `TIMESTAMP`.
- `streaming/src/streaming/silver/facts.py`: the pure, unit-tested logic.
  - **`parse()`:** raw payload to typed columns.
  - **`latest_per_key()`:** one row per key. LSN, time and the delete flag come from the newest event; business values come from the newest non-delete event (`has_values`).
  - **`merge_latest()`:** the LSN-guarded Delta MERGE. A delete-only batch sets only the flag, and a delete for an unseen key is inserted as a deleted row.
- `streaming/src/streaming/silver/upsert_facts.py`: one Spark app (`spark-silver-facts`, UI on :4041) with one streaming query per table.
  - **Reading:** `bronze.<table>` as a Delta stream, with checkpoints at `s3a://checkpoints/silver/<table>` and a 30 s trigger (`SILVER_TRIGGER_SECONDS`).
  - **Writing:** `silver.<table>` (`s3a://lakehouse/silver/<table>`, autoCompact) gets the business columns plus `source_lsn`, `source_ts_ms`, `is_deleted` and `silver_updated_at`.
  - **Startup:** it waits for the Bronze tables to exist.
- Tools (`spark-tools` service, profile `tools`):
  - `make reconcile`: an exact row-by-row, column-by-column comparison of live Silver rows with Postgres, using the read-only `debezium` user, retrying for up to 3 minutes.
  - `make spark-sql Q="…"`.
  - `make silver-facts-rebuild` (`streaming.tools.reset_tables`). `bronze-check` and `spark-shell` now use `spark-tools` too.
- `compose/streaming.yml`: restructured around YAML anchors (`x-spark-build`, `x-spark-env`, `x-spark-job`). Each job sets `SPARK_UI_PORT`.
- `compose/cdc.yml`: `kafka-connect` has `depends_on: postgres: {restart: true}`. `Makefile`: `make connector-restart`.
- 11 new Spark tests (16 in total): typed parsing, latest-by-LSN versus arrival order, insert, newer update, ignored older and replayed events, many changes in one batch, a delete setting only the flag, an update plus a delete in one batch, insert/update/delete in one batch, a delete for an unseen key, and idempotent full replay.

**How to run**
```bash
docker compose --profile cdc --profile streaming up -d
make logs S=spark-silver-facts
make scenario NAME=order_lifecycle_test && make scenario NAME=delete_test
make spark-sql Q="SELECT order_id, status, is_deleted FROM silver.orders ORDER BY order_id DESC LIMIT 5"
make stop-sim && make reconcile        # RECONCILED: silver matches postgres
```

**Bugs found while verifying (fixed)**
- **An update and a delete in the same batch lost the update.**
  - **What happened:** order 3384 was inserted in one Silver batch, then cancelled and hard-deleted inside the next. Latest-per-key kept only the delete, and the delete only set the flag, so Silver showed `placed` instead of `cancelled`.
  - **Fix:** business values now come from the newest non-delete event in the batch, and regression tests were added.
  - **Repair:** Silver was rebuilt from Bronze with `make silver-facts-rebuild`, which is exactly what Bronze exists for. Order 3384 now shows `cancelled` with `is_deleted = true`.
- **The Debezium task stayed FAILED after Postgres was recreated.**
  - **Cause:** changing `.env` made Compose recreate Postgres (it uses `env_file`). The task timed out reconnecting, and Kafka Connect never retries a failed task.
  - **Fix:** `kafka-connect` now restarts with Postgres (`restart: true`). Verified: a `docker compose restart postgres` brings the task back to RUNNING on its own.
- **`make spark-sql` mangled `$` in queries** (make expanded `$.status`), so it now passes `$(value Q)`.

**Verified**
- **Scenarios:** Silver shows order 3383 as `delivered` and order 3384 as `cancelled` with `is_deleted = true`. Its order item 14178 is also `is_deleted = true`.
- **Reconcile after 3 minutes of live simulation:** exact match (orders 3,824, order_items 16,101, inventory 600; 0 missing, extra or differing).
- **SIGKILL of `spark-silver-facts` during a live run, then restart:** reconcile matches again exactly (orders 4,207, order_items 17,768, inventory 600).
- **Tests:** `make test` gives 31 host tests and 16 Spark tests, all passing.
- **Memory:** spark-silver-facts about 1.2 GB, spark-bronze about 1.6 GB (2 GB limit each).

**Known issues**
- Silver lags Postgres by up to two trigger intervals (Bronze plus Silver, about 1 minute), which is within the "about a minute" target. `make reconcile` retries to absorb that.
- Editing `.env` recreates the containers that use `env_file` (Postgres, seed, simulator). Connect now follows Postgres automatically.

---

## Phase 5: Bronze ingestion

**Built**
- `streaming/src/streaming/bronze/ingest_cdc.py`: one Structured Streaming query over `subscribePattern=fmcg\.public\..*`.
  - **Source options:** starts from `earliest`, `maxOffsetsPerTrigger=50000`, `failOnDataLoss=true`, and `kafka.metadata.max.age.ms=30000` (new topics are discovered within 30 s rather than 5 min).
  - **Trigger and checkpoint:** a 30 s trigger (`BRONZE_TRIGGER_SECONDS`), with the checkpoint at `s3a://checkpoints/bronze/ingest_cdc`.
  - **Writes:** `foreachBatch` routes rows to `bronze.<table>` (and `bronze.quarantine`) and writes the tables **in parallel**. Path writes carry `txnAppId`/`txnVersion=batch_id` for exactly-once.
  - **Tables:** Delta, partitioned by `ingest_date`, with `delta.appendOnly` and `delta.autoOptimize.autoCompact`.
- `streaming/src/streaming/bronze/transform.py`: pure parsing and quarantine logic. The **raw JSON payload** is stored verbatim; only the CDC metadata (`op`, `source_lsn`, `source_ts_ms`, `is_deleted`) and the routing table name are parsed.
- `streaming/src/streaming/common/config.py`: job settings from the environment.
- `streaming/src/streaming/tools/bronze_check.py`: rows per table and op, quarantine reasons, and the duplicate `(topic, partition, offset)` check.
- `compose/streaming.yml`:
  - **`spark-bronze`:** runs the job, with the Spark UI on :4040, a healthcheck on the UI's REST API, and `restart: unless-stopped`. It waits for `kafka` and `hive-metastore` to be healthy and `minio-init` to complete. `kafka` joins the `streaming` profile.
  - **`spark-test`:** profile `test`; the same image built with `INSTALL_TEST=true`.
- Spark tests run **inside the container** (`streaming/tests/`, `conftest.py` with a local Delta-enabled session): `make test-streaming`. `make test` runs host tests and container tests, and host pytest now only collects `simulator/tests`.
- `Makefile`: `bronze-check`, `spark-shell`, `test-host`, `test-streaming`.
- `runbook.md`: a Bronze section (crashed job, data loss, re-ingest, quarantine).

**How to run**
```bash
docker compose --profile cdc --profile streaming up -d   # source, kafka, connect, lake, spark-bronze
make logs S=spark-bronze      # one "batch=N rows=… duration_s=…" line per micro-batch
make bronze-check             # rows per table/op + "NO DUPLICATES"
# Spark UI: http://localhost:4040 (Structured Streaming tab)
```

**Decisions**
- **Raw JSON payload in Bronze** (approved deviation from "parsed payload"): a Postgres schema change can never break Bronze or silently drop a field. Silver owns the typed per-table schemas.
- **One query, seven tables:** a single Kafka read, checkpoint and Spark UI, which is lighter on a laptop than seven queries. Exactly-once comes from Delta's idempotent writes (`txnAppId`/`txnVersion`).
- **Spark tests run in the Spark image** (approved): the host has no Java, by design.
- **JARs are downloaded in an early, independent Docker layer** (`/opt/spark-jars`, symlinked into pyspark), so dependency changes don't re-download about 330 MB.
- **Laptop-sized Spark** (measured):
  - **Delta snapshots:** `spark.databricks.delta.snapshotPartitions=2`. Delta's default 50 produced about 57-task jobs per commit for tiny tables.
  - **Cores and shuffle:** `SPARK_MASTER=local[2]` per job, `spark.sql.shuffle.partitions=4`.
  - **Native memory:** `MALLOC_ARENA_MAX=2` capped glibc arena fragmentation.
  - **Result:** batch time went from 40–55 s (falling behind the 30 s trigger) to 12–20 s, and memory from 1.82 GB to about 1.55 GB.

**Verified** (clean start: `make nuke`, then `cdc` + `streaming`, with the simulator running)
- **Snapshot:** batch 0 ingested the whole snapshot (13,227 rows: inventory 600, order_items 10,045, orders 2,317, products 200, regions 5, sales_reps 10, stores 50), all `op='r'`.
- **Live changes:** later batches carried `c`/`u`/`d` events every 30 s, and the counts grew.
- **Kill test:** `docker compose kill spark-bronze` (SIGKILL) 4 s into batch 28, then a restart. Batch 28 re-ran from the checkpoint. With the simulator stopped and Bronze caught up, **every Bronze table's row count equals Kafka's message count** (for example orders 13,263 / 13,263 and order_items 21,728 / 21,728), and `make bronze-check` reports `NO DUPLICATES`.
- **Tests:** `make test` gives 31 host tests and 5 Spark tests (parsing, deletes, snapshot reads, 7 quarantine reasons, column contract), all passing.
- **Idle CPU:** about 0.4% (CPU is only used while a batch runs).

**Known issues**
- **The first batch after a (re)start takes about 50–90 s** (JVM warm-up, table creation, Delta log loading). Later batches take 12–20 s.
- **Memory is tight on this machine.** WSL has 11.6 GB, and containers from other projects were using CPU and RAM during testing. With three Spark jobs (Phase 7) plus Trino (Phase 8), the stack needs about 10 GB. Stop unrelated containers, or raise the WSL memory in `%UserProfile%\.wslconfig` (`memory=14GB`).
- An existing `.env` needs the new `SPARK_MASTER` and Bronze block from `.env.example`.

---

## Phase 4: Lake foundation (MinIO, Hive Metastore, Spark image)

**Built**
- `compose/lake.yml` (profiles `lake` and `streaming`):
  - **`minio`:** `bitnamilegacy/minio:2025.7.23-debian-12-r5`. API on :9000, console on :9001 (`MINIO_BROWSER=on`, because Bitnami defaults the console to off), `minio-data` volume, healthcheck on `/minio/health/live`.
  - **`minio-init`:** a one-shot job (`bitnamilegacy/minio-client`) running `infra/minio/init.sh`. It creates the `lakehouse` and `checkpoints` buckets and a **least-privilege service user** (`LAKE_ACCESS_KEY`, policy `lakehouse-rw` covering just those two buckets). It's idempotent and retries until MinIO accepts connections.
  - **`hms-db`:** Postgres 16.15 for the metastore (`hms-db-data`).
  - **`hive-metastore`:** a thin custom image over `apache/hive:3.1.3` (`infra/hive-metastore/`), thrift on :9083 (internal only), healthcheck by TCP probe. It waits for `hms-db` to be healthy and `minio-init` to complete.
- `streaming/Dockerfile`: Python 3.11-slim-bookworm, OpenJDK 17, uv-installed `pyspark==3.5.9` and `delta-spark==3.3.3`.
  - **JARs:** the JARs listed in `streaming/jars.txt` (Delta 3.3.3, hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262, and the Spark Kafka connector 3.5.9 with its dependencies) are downloaded from Maven Central at build time and **SHA-1 verified**. Nothing is fetched at runtime.
  - **Runtime:** runs as non-root `spark` (uid 10001). Image size is 1.8 GB.
- `streaming/conf/spark-defaults.conf`: Delta extensions and catalog, the Hive catalog at `thrift://hive-metastore:9083`, S3A to MinIO (path style, no SSL, credentials from env), UTC session time zone.
- `streaming/src/streaming/common/spark.py`: the single `build_session()` used by every job (local mode; driver memory from `SPARK_DRIVER_MEMORY`).
- `streaming/src/streaming/smoke_test.py`: creates database `smoke` at `s3a://lakehouse/smoke`, writes `smoke.numbers` (10 rows) with `saveAsTable`, reads it back by name, and checks the row count, the sum, the format and the metastore listing.
- `compose/streaming.yml`: `spark-bronze` (the shared image, one-shot for now). Its ingestion command arrives in Phase 5.
- `Makefile`: `make spark-smoke`.

**How to run**
```bash
make up P=lake     # minio, minio-init, hms-db, hive-metastore
make spark-smoke   # ends with "SMOKE TEST PASSED"
# MinIO console: http://localhost:9001  (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD from .env)
```

**Decisions**
- **MinIO:** the community images are gone from Docker Hub ("object not found"). The build uses Bitnami's last legacy MinIO build (frozen July 2025, no further security updates; acceptable for a local stack). The alternatives considered were building MinIO from source and RustFS.
- **Delta 3.3.3** instead of 3.2.x: it's the newest Delta release that supports Spark 3.5, with the same API.
- **Hive Metastore 3.1.3**, with image fixes (ADR-005):
  - The bundled 2016 Postgres JDBC driver can't authenticate against Postgres 16, so it's replaced with 42.7.13 (SHA-256 pinned).
  - The S3A JARs are linked onto Hive's classpath.
  - An entrypoint wrapper makes schema init idempotent; the stock image re-runs `initSchema` on every start and would crash-loop.
  - The hardcoded `-Xmx1G` is overridden to 512 MB.
- **No secrets in config files:**
  - HMS gets its JDBC settings through `SERVICE_OPTS` system properties.
  - Spark and HMS get S3 credentials through `AWS_*` env vars (`EnvironmentVariableCredentialsProvider`).
  - Only `minio-init` sees the MinIO root credentials.
- **Lake services also carry the `streaming` profile,** so `spark-bronze` can declare `depends_on` on them (the same pattern as `postgres` with `sim` and `cdc`).

**Verified**
- **Lake services:** all healthy, and `minio-init` exits 0, reporting both buckets and the `lakehouse-rw` policy on the service user.
- **Metastore:** the first start initialises the schema. On restart it logs "schema present, skipping init" and becomes healthy with 0 restarts.
- **Smoke test:** `make spark-smoke` reports `format=delta location=s3a://lakehouse/smoke/numbers rows=10 sum(square)=385` and `SMOKE TEST PASSED`. A second run passes with `delta_versions=2`.
- **MinIO contents:** `_delta_log/00…0.json` and `00…1.json`, plus the Parquet files from both versions (old files stay until `VACUUM`, which is what makes time travel possible).
- **Least privilege:** the service user gets `Access Denied` when it tries to create a bucket.
- **Idle memory:** minio about 350 MB (limit raised to 768 MB), HMS about 345 MB of 768 MB, hms-db about 75 MB of 256 MB.

**Known issues and notes**
- Harmless Spark warnings: the native-hadoop library is missing (it uses built-in Java classes), the S3A metrics config is missing, and "Couldn't find corresponding Hive SerDe for data source provider delta". The last one is expected: Hive itself can't read Delta, but Spark and Trino read the table through its Delta metadata.
- The MinIO console in community builds from mid-2025 is an object browser only (MinIO removed the admin screens upstream). Users and policies are managed by `minio-init` with `mc`.
- Building the Spark image downloads about 650 MB (pyspark plus JARs); later builds are cached.
- `.env` values containing spaces (`*_HEAP_OPTS`) are now quoted so `.env` can also be sourced by a shell. An existing `.env` needs the new lake block from `.env.example` added to it.

---

## Phase 3: Debezium CDC

**Built**
- `compose/cdc.yml` (profile `cdc`):
  - **`kafka-connect`:** `quay.io/debezium/connect:2.7.4.Final` (Kafka 3.7 clients), REST API on :8083.
    - **Setup:** JSON converters with schemas off; internal topics `connect-configs`, `connect-offsets` and `connect-status` with replication factor 1; `EnvVarConfigProvider` for secrets.
    - **Startup and health:** waits for `kafka` and `postgres` to be healthy; healthcheck on `GET /connectors`; 1 GB limit; log rotation.
  - **`connector-register`:** a one-shot job (`curlimages/curl:8.22.0`) that runs `infra/debezium/register.sh`. It does an idempotent `PUT /connectors/fmcg-postgres/config`, then waits until the connector *and* its task are `RUNNING` (exit 0) or `FAILED`/timeout (exit 1). It waits for `kafka-connect` to be healthy and `seed` to complete, so the snapshot always includes the seed.
- `infra/debezium/fmcg-postgres.json`:
  - **Source:** `pgoutput`, slot `fmcg_debezium`, the pre-created `fmcg_publication` (`publication.autocreate.mode=disabled`), and the 7 tables.
  - **Topics:** `topic.prefix=fmcg`, giving `fmcg.public.<table>`.
  - **Snapshot and heartbeat:** `snapshot.mode=initial`, with a heartbeat every 10 s (`__debezium-heartbeat.fmcg`).
  - **Message shape:** decimals as strings; unwrap SMT with `add.fields=op,source.ts_ms,source.lsn` and `delete.tombstone.handling.mode=rewrite`.
  - **Topic creation:** `topic.creation.default.*` gives 1 partition, replication factor 1, 7-day retention and lz4.
  - **Errors:** `errors.tolerance=none`, with full error logging.
- Postgres:
  - **`REPLICA IDENTITY FULL`** on all 7 tables (`01_schema.sql`).
  - **`max_slot_wal_keep_size=2GB`.**
  - **Profiles:** `postgres` and `seed` join the `cdc` profile, and so do `kafka` and `kafka-ui` (which now also shows the Connect cluster).
- `Makefile`: `register-connector`, `connector-status`, `cdc-counts` (`infra/debezium/cdc-counts.sh`) and `cdc-tail T=<table> N=<n>` (`infra/debezium/cdc-tail.sh`).
- Docs: ADR-002 revised (fail-fast instead of DLQ, replica identity, secrets provider); README diagram and `architecture.md` no longer show a Connect DLQ; `runbook.md` has a CDC section.

**How to run**
```bash
make up P=cdc               # postgres, seed, kafka, kafka-ui, kafka-connect, connector-register
make connector-status       # connector + task RUNNING
make cdc-counts             # after the snapshot: kafka_msgs == pg_rows for every table
make cdc-tail T=orders N=5  # latest events with __op / __source_lsn
make simulate               # live changes flow within ~1 s
```

**Example event** (`fmcg.public.products`, key `{"product_id":1}`):
```json
{"product_id":1,"sku":"BEV-TAM-001-EA","name":"Tamu Cola 330ml","unit_price":"52.50","is_active":true,
 "updated_at":"2026-10-03T14:01:36.662488Z","__deleted":"false","__op":"u",
 "__source_ts_ms":1791036096666,"__source_lsn":31677120}
```

**Decisions**
- **No Connect DLQ:** Kafka Connect DLQs exist only for sink connectors, and `errors.tolerance=all` on a source silently drops changes. CDC fails fast instead; the slot retains WAL, so a fix plus restart loses nothing. The dead-letter layer moves to the Bronze quarantine table in Phase 5 (ADR-002).
- **`REPLICA IDENTITY FULL`:** testing showed that with the default identity, delete events carried placeholders (`store_id: 0`, `order_ts: 1970-01-01`, `status: placed`). With FULL they carry the real last row.
- **WAL safety cap:** `max_slot_wal_keep_size=2GB`. A long Connect outage invalidates the slot instead of filling the Postgres disk, and recovery is a re-snapshot (see the runbook).
- **No `env_file` on Connect:** the worker receives only `POSTGRES_DB` and the Debezium user's credentials, not the Postgres superuser password.
- **Version:** Debezium 2.7.4.Final (latest 2.7), aligned with the Kafka 3.x broker. Debezium 3.x is a future upgrade.

**Verified**
- **Clean start:** `make nuke && make up P=cdc` brings every service up healthy, and `connector-register` exits 0 with the connector and task RUNNING.
- **Snapshot:** 7 `fmcg.public.*` topics plus the heartbeat topic. `cdc-counts` matches Postgres exactly: 5 / 50 / 200 / 10 / 2,426 / 10,576 / 600.
- **Streaming:**
  - `scd2_price_test` produced three `__op:"u"` events for product 1 with increasing LSNs.
  - `delete_test` produced `__op:"d"`, `__deleted:"true"` events carrying the real last state, with the item deletes' LSNs before the order's.
- **Latency:** from Postgres commit to the Kafka append, 450–830 ms with the simulator running.
- **Resilience:**
  - Re-running `make register-connector` is idempotent.
  - After stopping Connect for 20 s while the simulator kept writing, rebuilding the latest state per `order_id` (highest LSN) from the topic matched Postgres exactly: 2,543 live orders, 0 missing, 0 extra, 0 status mismatches.
- **Slot:** `fmcg_debezium` is active, `wal_status=reserved`, and `max_slot_wal_keep_size=2GB`.

**Known issues**
- Snapshot events (`__op:"r"`) share one LSN. That's fine for Silver, because each key appears once per snapshot.
- A targeted re-snapshot that keeps downstream data isn't documented yet; the runbook uses a full reset until Phase 10.
- An existing `.env` needs the new CDC block from `.env.example`, and the schema change (`REPLICA IDENTITY FULL`) needs `make nuke`, because init scripts only run on an empty volume.

---

## Phase 2: Kafka

**Built**
- `compose/kafka.yml` (profile `kafka`):
  - **`kafka`:** `apache/kafka:3.9.2` in KRaft combined mode (broker + controller, node 1, with a fixed `KAFKA_CLUSTER_ID` from `.env`).
    - **Listeners:** `PLAINTEXT://kafka:9092` for containers and `CONTROLLER://:9093`. There is no host port; use Kafka UI.
    - **Settings:** `auto.create.topics.enable=false`, 7-day retention, and replication factor 1 everywhere (single broker).
    - **Resources:** 512 MB heap, 1 GB memory limit, `kafka-data` volume, log rotation.
    - **Healthcheck:** `kafka-broker-api-versions.sh`.
  - **`kafka-ui`:** `kafbat/kafka-ui:v1.5.0` on http://localhost:8080. It waits for `kafka` to be healthy and has a healthcheck on `/actuator/health`, a 512 MB limit and log rotation.
- `infra/kafka/smoke-test.sh`: creates a topic, produces 5 messages, consumes them with offsets, confirms that producing to a missing topic does **not** create it, then deletes the topic.
- `Makefile`: `make kafka-topics`, `make kafka-smoke`.
- `.env.example`: Kafka and Kafka UI image tags, cluster ID, retention, heap and memory limits.

**How to run**
```bash
make up P=kafka
make kafka-smoke        # ends with "SMOKE TEST PASSED"
make kafka-topics
# Kafka UI: http://localhost:8080
```

**Decisions**
- **No `env_file` on Kafka services:** the `apache/kafka` image turns every `KAFKA_*` environment variable into a broker property, so only the explicit `environment:` block is passed (Kafka needs no secrets).
- **Kafka 3.9.2 rather than 4.x:** keeps the broker on the same major version as the Kafka client inside the Debezium 2.7 Connect image (ADR-001).
- **Kafka UI is the maintained fork** `kafbat/kafka-ui` (`provectuslabs/kafka-ui` is unmaintained).
- **No data-volume fix needed:** the image ships `/var/lib/kafka/data` owned by `appuser`, so a new named volume inherits writable ownership.

**Verified**
- Both services are healthy. The smoke test passes. Kafka UI's API reports cluster `fmcg` ONLINE with 1 broker, version 3.9.
- Topics survive `down`/`up` (named volume plus fixed cluster ID).
- The effective broker config shows `auto.create.topics.enable=false` and `log.retention.hours=168`.
- Idle memory: kafka about 390 MB of 1 GB, kafka-ui about 190 MB of 512 MB.

**Known issues**
- `docker compose --profile kafka down` while other profiles are running prints "Network fmcg-net Resource is still in use". That's harmless; use `make down` to stop everything.
- An existing `.env` needs the new Kafka block from `.env.example` added to it (`make env` only creates `.env` when it is missing).

---

## Phase 1b: Business event simulator

**Built**
- `simulator/src/simulator/`:
  - `rules.py`: pure business rules shared by the seed and the simulator. It covers the order state machine (forward only; delivered and cancelled are terminal; only placed or confirmed can be cancelled), stock allocation (never negative), price changes (cents, floor of 1.00), one-step tier moves, credit-limit rounding, weighted picks and line quantities.
  - `events.py`: one function per event, covering all 10 events from the spec. Each runs in the caller's transaction and raises `Skip` (with a reason) when there is no valid target.
    - **New orders:** take current prices under `FOR SHARE`, lock stock rows in `product_id` order, and shrink or drop lines that are out of stock.
    - **Cancellations:** return stock to the store's regional warehouse.
    - **Hard deletes:** remove the order items first, then the order.
  - `runner.py`: continuous mode.
    - **Timing:** inter-arrival times are drawn from an exponential distribution at `--rate`, with an optional time-of-day multiplier. Product popularity is relearned from order history every 5 minutes.
    - **Logging:** one `event=… status=ok|skipped|failed entity_id=… latency_ms=…` line per event, plus a `summary` line every 30 s (events/s by type).
    - **Shutdown:** SIGTERM and SIGINT set a flag that is checked between events, so in-flight transactions always finish.
  - `scenarios.py`: `scd2_price_test`, `scd2_burst_test`, `order_lifecycle_test` and `delete_test`.
    - **Commits:** every step commits separately, so each change gets its own WAL LSN.
    - **Output:** each scenario prints the IDs it touched and the expected Silver/Gold result, then checks Postgres and exits non-zero on a mismatch.
  - `config.yaml` is restructured into four sections:
    - `business` (shared)
    - `seed`
    - `simulator`, which holds event weights, dispatch batch, price and credit ranges, restock thresholds, the active-product floor, hard-delete age, reserved fixtures and the time-of-day curve
    - `scenarios`
  - CLI: `python -m simulator run [--rate] [--duration] [--seed]` (`SIM_RATE` and `SIM_SEED` env vars work too) and `python -m simulator scenario <name>`.
- `compose/source.yml`: a `simulator` service (profile `sim`) on the same image, with `restart: unless-stopped` and `stop_grace_period: 15s`. It waits for `postgres` to be healthy and `seed` to complete. `postgres` and `seed` also carry the `sim` profile.
- `Makefile`: `make simulate` (start and follow logs), `make stop-sim` and `make scenario NAME=…`.
- `simulator/tests/test_rules.py`: 20 tests for the state machine, allocation, pricing, tiers, credit limits, distinct picks, and that the config weights match the event registry and sample in proportion. 31 tests in total.

**How to run**
```bash
make simulate                              # live business; Ctrl+C stops following logs only
make stop-sim                              # graceful stop (SIGTERM)
make scenario NAME=scd2_price_test         # also: scd2_burst_test, order_lifecycle_test, delete_test
docker compose --profile sim run --rm simulator run --rate 20 --duration 60   # ad-hoc run
```

**Decisions**
- **Status advances are dispatch batches:** each advance event moves 3–7 of the oldest open orders one step. With ~50% new orders and ~30% advances, single-order advances would leave the open-order backlog growing forever. Batching keeps it flat (measured: it stays below ~50 open orders at 50 events/s).
- **Reserved scenario fixtures** (`simulator.reserved`: stores 1 and 3, product 1):
  - **Why:** the live simulator never selects these stores or their orders, and never changes these products, so scenarios give the same result while the simulator runs. Without this, the simulator advanced the lifecycle test's order mid-scenario.
  - **How:** YAML anchors keep the reserved IDs and the scenario config in sync.
- **`delete_test`** deletes a cancelled order of reserved store 3, creating and cancelling one first if needed, so it never collides with the simulator's own `hard_delete`.
- **Product popularity is learned from `order_items`**, so live orders keep the seed's 80/20 skew without duplicating the ranking logic.
- **Time-of-day rhythm is off by default** (`simulator.time_of_day.enabled`), so evening demos aren't nearly idle.
- **Shared business settings:** `config.yaml` sections `seed` and `simulator` are each merged over `business` (`config.section()`).

**Verified**
- At `--rate 5`, the summary lines show 4.9–5.1 events/s with the configured mix. A 2-minute run at 50 events/s gave 0 failures, a stable backlog, stock recovering through restocks, and 0 orders without lines.
- All 4 scenarios pass twice in a row with the live simulator running.
- `docker compose stop simulator` logs "received SIGTERM … stopped cleanly" and exits 0 within a second.

**Known issues**
- At high `--rate` the achieved rate falls short (about 37/s at `--rate 50`) because event time isn't subtracted from the wait. At the default 5/s, the difference is negligible.
- With the spec weights, a long run grows the master data quickly. At 5 events/s that's about 360 new stores per hour, and discontinuations hit the `min_active_products: 150` floor within about an hour, after which they are skipped. Lower `new_store` / `product_discontinued` in `config.yaml` for all-day runs.
- Cancelling a *seeded* historical order returns stock that the seed never deducted (the seed sets current inventory directly). This is harmless for the demo.

---

## Phase 1a: Source database and seed data

**Built**
- `compose/source.yml` (profile `source`):
  - **`postgres`:** pinned `postgres:16.15`, started with `wal_level=logical`, `max_replication_slots=4` and `max_wal_senders=4`. It uses the `pg-data` volume, port 5432, a memory limit and log rotation.
  - **Healthcheck:** `pg_isready -h 127.0.0.1`. It checks over TCP so the service isn't reported healthy while the init scripts are still running.
  - **`seed`:** a one-shot job built from the simulator image that waits for `postgres` to be healthy and exits 0.
- `infra/postgres/init/` (runs only on an empty volume):
  - `01_schema.sql`: 7 tables with identity primary keys, foreign keys, check constraints (statuses, channels, tiers, categories, non-negative stock, positive prices, discount 0–100), `UNIQUE (order_id, product_id)` on `order_items`, and indexes on the foreign keys and on `orders(status)` and `orders(order_ts)`.
  - `02_triggers.sql`: a `set_updated_at()` trigger on every table. `regions` and `order_items` also got an `updated_at` column so every table has one.
  - `03_replication.sh`: the `debezium` role (LOGIN REPLICATION, plus CONNECT, USAGE and SELECT only) and the `fmcg_publication` publication over the 7 tables. It's a shell script so the password comes from `.env`.
- `simulator` package:
  - `catalog.py`: 29 fictional Kenyan brands across 5 categories. Each pack size is sold as a single unit and as a case, giving 200 SKUs priced in KES.
  - `seed.py`:
    - `generate(cfg, now)` is pure: it takes the config and the clock and returns rows, so it can be tested without a database. `run_seed()` loads everything in a single transaction under a table lock and skips if data already exists.
    - Explicit IDs are inserted with `OVERRIDING SYSTEM VALUE`, then the identity sequences are moved past them.
  - `config.yaml` (`seed:` section): counts, regions with weights and warehouses, the channel mix and order shapes, tier discounts, the Zipf popularity exponent, status by order age, and inventory ranges.
  - `db.py` connects with psycopg 3 using env settings; `config.py` loads the config (`SIM_CONFIG` overrides the path).
  - `__main__.py` is a Typer CLI with `python -m simulator seed`.
- `simulator/Dockerfile`: follows the uv workspace pattern (dependency layer first, then the project, installed non-editable), uses a BuildKit cache and runs as non-root (uid 10001).
- `simulator/tests/test_seed.py`: 11 tests covering determinism, counts, references, price copying, the rep and store region match, non-negative stock, status by order age, and clock independence of master data.
- `Makefile`: `make seed` and `make psql`.

**Seed result** (`random_seed: 42`): 5 regions, 50 stores, 200 products, 10 reps, 600 inventory rows, and about 2,400 orders with about 10,500 order items spanning about 30 days. Most orders are delivered, about 6% cancelled, and recent ones are still placed, confirmed or shipped. The top 20% of SKUs account for about 70% of order lines.

**How to run**
```bash
make up P=source   # postgres + seed
make psql          # interactive psql
make seed          # re-run the seed (prints "seed skipped" when populated)
make nuke          # wipe volumes for a fresh seed
```

**Decisions**
- Company setting: a Kenyan distributor, priced in KES, with regions Nairobi, Central, Coast, Rift Valley and Western, warehouses `nairobi_dc`, `mombasa_dc` and `kisumu_dc`, and EAT business hours. Brand names are fictional.
- Determinism: master data, popularity and inventory are byte-identical across clean starts, because each section has its own random stream. Order timestamps are anchored to the moment of seeding so history always ends "today"; two seeds on the same day produce identical orders except for the ones placed later in the day.
- The seed sets current inventory directly rather than replaying stock movements for the historical orders. About 10% of SKU/warehouse pairs start low so that restock events have targets.
- Debezium gets a pre-created publication (to be used with `publication.autocreate.mode=disabled`) and no ownership or write privileges.

**Known issues**
- On WSL, Docker Engine prints "Your kernel does not support swap limit capabilities". This is harmless: memory limits still apply, without swap accounting.

---

## Phase 0: Scaffold and architecture docs

**Built**
- Repository structure (empty dirs hold a `.gitkeep`).
- README with the project pitch and a Mermaid architecture diagram; `docs/architecture.md`; ADR-001 to ADR-010 in `docs/adr/`.
- uv workspace: a virtual root `pyproject.toml` with an exactly pinned `dev` group (ruff, sqlfluff, pytest, pre-commit, detect-secrets), members `simulator`, `streaming` and `dbt` (the last is `package = false`), `.python-version` = 3.11, and `uv.lock`.
- Compose skeleton: the root `docker-compose.yml` (project `fmcg`, network `fmcg-net`, four named volumes, `include:` of `compose/*.yml`). Each domain file is `services: {}` for now.
- `Makefile` with every §5.4 target. Lifecycle, `test` and `lint` work now; later-phase targets exit with "not implemented until Phase N".
- `.pre-commit-config.yaml`: pre-commit-hooks v6.0.0 (whitespace, EOF, YAML/TOML/JSON, large files, merge conflicts, LF line endings), plus ruff, sqlfluff and detect-secrets run through `uv run` so they match the lockfile. `.secrets.baseline` is committed.
- BI is Power BI, outside the Compose stack, connected to Trino (ADR-010). No Superset service, `bi` profile or `superset-data` volume.
- `.gitignore`, `.dockerignore`, `.gitattributes` (LF everywhere), `.env.example`, `.sqlfluff` (postgres dialect).

**How to run**
```bash
uv sync
uv run pre-commit install
make lint
docker compose config -q
```

**Decisions**
- The repo lives in the WSL filesystem (`~/projects/fmcg-realtime-lakehouse`), not under `/mnt/c`, for bind-mount and I/O performance.
- Kafka UI: `provectuslabs/kafka-ui` is unmaintained, so the build uses the maintained fork `ghcr.io/kafbat/kafka-ui` (Phase 2).
- MinIO: community Docker images are no longer published. Phase 4 will either pin the last published `RELEASE.*` tag or pick an alternative S3-compatible store.
- Debezium: images come from `quay.io/debezium/connect`, which is no longer on Docker Hub (Phase 3).
- Kafka pinned to the 3.9.x line (ADR-001). Hive Metastore to be pinned at 3.1.3 and validated in Phase 4 (ADR-005).
- The base Python image is `python:3.11.x-slim-bookworm`, not trixie, because Debian trixie no longer ships OpenJDK 17, which Spark 3.5 needs.
- `make test` treats pytest exit code 5 (no tests collected) as success until tests exist.

**Known issues**
- None.
