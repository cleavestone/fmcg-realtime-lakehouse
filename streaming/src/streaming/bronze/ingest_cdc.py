"""Bronze: Kafka CDC topics -> one append-only Delta table per source table.

One streaming query subscribes to every fmcg.public.* topic. Each micro-batch is split by
topic in foreachBatch and appended to bronze.<table>; untrustworthy records go to
bronze.quarantine. Exactly-once: the checkpoint tracks Kafka offsets, and every Delta write
carries (txnAppId, txnVersion=batch_id), so a batch replayed after a crash is skipped by
Delta instead of being appended twice (ADR-007).

    python -m streaming.bronze.ingest_cdc
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.bronze.transform import BRONZE_COLUMNS, QUARANTINE_COLUMNS, parse_cdc
from streaming.common.config import Settings, load_settings
from streaming.common.spark import build_session

log = logging.getLogger("bronze")

DATABASE = "bronze"
QUARANTINE_TABLE = "quarantine"
TXN_APP_ID = "bronze_ingest_cdc"
# appendOnly: Bronze is an immutable log. autoCompact: 30 s micro-batches would otherwise
# pile up small files.
TABLE_PROPERTIES = "'delta.appendOnly' = 'true', 'delta.autoOptimize.autoCompact' = 'true'"

BRONZE_DDL = """
    kafka_key STRING, payload STRING, op STRING, source_ts_ms BIGINT, source_lsn BIGINT,
    is_deleted BOOLEAN, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP, ingested_at TIMESTAMP, ingest_date DATE
"""
QUARANTINE_DDL = """
    kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, kafka_timestamp TIMESTAMP,
    kafka_key STRING, payload STRING, quarantine_reason STRING, ingested_at TIMESTAMP,
    ingest_date DATE
"""


class BronzeWriter:
    """foreachBatch handler; remembers which tables already exist."""

    def __init__(self, spark: SparkSession, settings: Settings) -> None:
        self.spark = spark
        self.base = f"{settings.lakehouse_uri}/{DATABASE}"
        self.known_tables: set[str] = set()

    def _ensure_table(self, table: str, ddl: str) -> None:
        if table in self.known_tables:
            return
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table} ({ddl}) USING DELTA "
            f"PARTITIONED BY (ingest_date) LOCATION '{self.base}/{table}' "
            f"TBLPROPERTIES ({TABLE_PROPERTIES})"
        )
        self.known_tables.add(table)

    def _append(self, df: DataFrame, table: str, batch_id: int) -> None:
        # Path write: the table is already registered, so skip a metastore round trip.
        (
            df.write.format("delta")
            .mode("append")
            .option("txnAppId", TXN_APP_ID)
            .option("txnVersion", batch_id)
            .save(f"{self.base}/{table}")
        )

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        parsed = parse_cdc(batch_df).persist()
        try:
            counts = {
                (r["table"], r["bad"]): r["n"]
                for r in parsed.groupBy(
                    "table", F.col("quarantine_reason").isNotNull().alias("bad")
                )
                .agg(F.count("*").alias("n"))
                .collect()
            }
            good = parsed.filter(F.col("quarantine_reason").isNull())
            writes = {
                table: good.filter(F.col("table") == table).select(*BRONZE_COLUMNS)
                for table in sorted({t for t, bad in counts if not bad})
            }
            quarantined = sum(n for (_, bad), n in counts.items() if bad)
            if quarantined:
                writes[QUARANTINE_TABLE] = parsed.filter(
                    F.col("quarantine_reason").isNotNull()
                ).select(*QUARANTINE_COLUMNS)
                log.warning("batch=%s quarantined=%s", batch_id, quarantined)

            for table in writes:
                self._ensure_table(
                    table, QUARANTINE_DDL if table == QUARANTINE_TABLE else BRONZE_DDL
                )
            # Each table has its own Delta log, so commits are independent: write them in
            # parallel (each S3 commit is mostly waiting). Idempotency stays per table.
            with ThreadPoolExecutor(max_workers=max(len(writes), 1)) as pool:
                for future in [
                    pool.submit(self._append, df, table, batch_id) for table, df in writes.items()
                ]:
                    future.result()

            written = " ".join(f"{t}={n}" for (t, bad), n in sorted(counts.items()) if not bad)
            log.info(
                "batch=%s rows=%s duration_s=%.1f %s",
                batch_id,
                sum(counts.values()),
                time.monotonic() - started,
                written or "(empty)",
            )
        finally:
            parsed.unpersist()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s level=%(levelname)s logger=%(name)s %(message)s"
    )
    # py4j (the Python<->JVM bridge) logs every callback connection at INFO.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    settings = load_settings()
    spark = build_session("bronze-ingest-cdc")
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DATABASE} LOCATION '{settings.lakehouse_uri}/{DATABASE}'"
    )

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribePattern", settings.cdc_topic_pattern)
        # Re-check the pattern for new topics every 30 s (Kafka's default is 5 min), so Bronze
        # picks up Debezium's topics quickly even if it started before they were created.
        .option("kafka.metadata.max.age.ms", "30000")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", settings.bronze_max_offsets_per_trigger)
        # Kafka deleted data Bronze never read (retention): stop loudly, never skip silently.
        .option("failOnDataLoss", "true")
        .load()
    )
    query = (
        source.writeStream.queryName("bronze_ingest_cdc")
        .foreachBatch(BronzeWriter(spark, settings))
        .option("checkpointLocation", f"{settings.checkpoint_uri}/bronze/ingest_cdc")
        .trigger(processingTime=f"{settings.bronze_trigger_seconds} seconds")
        .start()
    )
    log.info(
        "started: pattern=%s trigger=%ss checkpoint=%s/bronze/ingest_cdc",
        settings.cdc_topic_pattern,
        settings.bronze_trigger_seconds,
        settings.checkpoint_uri,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
