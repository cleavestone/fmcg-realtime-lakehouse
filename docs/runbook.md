# Runbook

Operational procedures. Filled in as each component lands; completed in Phase 10.

## Start / stop

| Action | Command |
|---|---|
| Start one profile | `make up P=<profile>` |
| Start everything | `make demo` |
| Stop, keep data | `make down` |
| Stop and wipe all volumes | `make nuke` |
| Status | `make ps` |
| Logs | `make logs S=<service>` |

## CDC (Debezium)

| Check | Command |
|---|---|
| Connector and task state | `make connector-status` (both should be `RUNNING`) |
| Topic message counts vs Postgres rows | `make cdc-counts` |
| Latest events for a table | `make cdc-tail T=orders N=5` |
| Re-apply the connector config | `make register-connector` (idempotent `PUT`) |
| Replication slot health | `make psql`, then `SELECT slot_name, active, wal_status FROM pg_replication_slots;` |

### Connector FAILED
1. Find the cause: `make logs S=kafka-connect` (errors are logged with the offending record).
2. Fix it (config, permissions, schema).
3. Restart the task: `curl -X POST "localhost:8083/connectors/fmcg-postgres/restart?includeTasks=true&onlyFailed=true"`.
   Because the replication slot kept the WAL, the connector resumes at its last committed LSN and nothing is lost.

### Task FAILED after a Postgres restart
If Postgres restarts outside Compose (or before this fix existed), the Debezium task can fail to reconnect and stay `FAILED`; Kafka Connect never retries it. Run `make connector-restart`; it resumes from the replication slot and nothing is lost. When Compose restarts or recreates Postgres, `kafka-connect` is restarted with it automatically (`depends_on … restart: true`). Note that editing `.env` makes Compose recreate every service that uses `env_file`, including Postgres.

### Connect was down for a while
Nothing to do. On start it resumes from its offset in `connect-offsets`, and the slot kept the WAL. Verified: a 20-second outage with the simulator running lost no changes.

### Slot invalidated (`wal_status = lost`) or re-snapshot needed
This happens when Connect was down long enough for retained WAL to exceed `max_slot_wal_keep_size` (2GB). The slot can't resume and Debezium must take a fresh snapshot. For this local stack, the verified way is a full reset: `make nuke && make up P=cdc` (or `make demo`). A targeted re-snapshot that keeps Bronze and Silver (drop the slot and reset the connector's offsets) will be written and tested in Phase 10.

## Bronze ingestion (Spark)

| Check | Command |
|---|---|
| Job running, recent batches | `make logs S=spark-bronze` (one `batch=N rows=… orders=…` line per micro-batch) |
| Spark UI (Structured Streaming tab) | http://localhost:4040 |
| Rows per table/op, quarantine, duplicate offsets | `make bronze-check` (must print `NO DUPLICATES`) |
| Explore tables interactively | `make spark-shell`, then `spark.table("bronze.orders").show()` |

### Job crashed or was killed
Nothing to do: Compose restarts it (`restart: unless-stopped`) and it resumes from its checkpoint at `s3a://checkpoints/bronze/ingest_cdc`. A batch that was written but not checkpointed is replayed and skipped by Delta's idempotent-write check (`txnAppId`/`txnVersion`), so no duplicates. Verify with `make bronze-check`.

### Job stops with "Some data may have been lost" (failOnDataLoss)
Kafka deleted records before Bronze read them (Bronze was down longer than the 7-day retention). The records are unrecoverable from Kafka. Re-snapshot with Debezium (see CDC above) so the current state is captured again, then reset Bronze's Kafka position by removing its checkpoint (below).

### Re-ingest Bronze from Kafka (rebuild)
Only possible for data still inside Kafka's retention window.
```bash
docker compose stop spark-bronze
make spark-shell    # then: spark.sql("DROP DATABASE bronze CASCADE")  (data files stay on MinIO)
# delete s3a://lakehouse/bronze/ and s3a://checkpoints/bronze/ in the MinIO console (http://localhost:9001)
docker compose --profile streaming up -d spark-bronze
```

### Quarantined records
`make bronze-check` lists the reasons. Inspect with `make spark-shell`:
`spark.table("bronze.quarantine").select("kafka_topic", "kafka_offset", "quarantine_reason", "payload").show(truncate=False)`.

## Silver facts (Spark)

| Check | Command |
|---|---|
| Job running, per-table batches | `make logs S=spark-silver-facts` (`table=orders batch=N keys=… duration_s=…`) |
| Spark UI | http://localhost:4041 |
| Silver equals Postgres | `make stop-sim`, then `make reconcile` (retries up to 3 min while Silver catches up; must print `RECONCILED`) |
| Query anything | `make spark-sql Q="SELECT * FROM silver.orders WHERE order_id = 2427"` |

### Silver job crashed or was killed
Nothing to do: Compose restarts it and each table's query resumes from `s3a://checkpoints/silver/<table>`. A replayed batch is a no-op thanks to the LSN guard. Verified with a SIGKILL during a live run, followed by `make reconcile`.

### Rebuild Silver facts from Bronze
After a bug fix in Silver logic, or to recover from bad Silver data: `make silver-facts-rebuild`. It stops the job, drops `silver.orders`, `silver.order_items` and `silver.inventory`, deletes their data and checkpoints on MinIO, and restarts the job, which replays the whole Bronze history (`batch=0` holds everything). Kafka isn't involved.

## Silver dimensions (SCD2)

| Check | Command |
|---|---|
| Job running, per-dimension batches | `make logs S=spark-silver-dims` (`dim=dim_store batch=N events=… rows_merged=… open_versions=…`) |
| Spark UI | http://localhost:4042 |
| Invariants (one current per key, contiguous, no overlaps) | `make scd2-check` (must print `SCD2 INVARIANTS HOLD`) |
| History of one entity | `make spark-sql Q="SELECT * FROM silver.dim_product WHERE product_id = 1 ORDER BY valid_from"` |

### Dimensions job crashed or was killed
Nothing to do: it resumes from `s3a://checkpoints/silver/dim_*`. Replayed events are ignored, because their LSN is at or below the key's latest version and surrogate keys are deterministic. Verified with a SIGKILL during a live run, followed by `make scd2-check`.

### Rebuild dimensions from Bronze
`make silver-dims-rebuild` drops the three `dim_*` tables with their data and checkpoints and replays Bronze. The result is identical to the incrementally built history: verified by fingerprinting every surrogate key, hash and window before and after a rebuild.

## Procedures (to be written)

- Gold rebuilds (Phase 8)
