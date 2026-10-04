"""Silver facts: Bronze change log -> current-state orders, order_items, inventory.

One streaming query per table, all in this Spark app. Each reads bronze.<table> as a Delta
stream (ADR-003), keeps the newest event per key by LSN and MERGEs it into silver.<table>
with an LSN guard, so restarts, replays and re-sent events never move a row backwards
(ADR-007).

    python -m streaming.silver.upsert_facts
"""

import logging
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from streaming.common.config import Settings, load_settings
from streaming.common.schemas import FACTS, TableSpec
from streaming.common.spark import build_session
from streaming.silver.facts import latest_per_key, merge_latest, parse, silver_ddl

log = logging.getLogger("silver.facts")

DATABASE = "silver"
BRONZE_POLL_SECONDS = 10


def wait_for_bronze(spark: SparkSession, tables: list[str]) -> None:
    """On a fresh start Bronze creates its tables with its first batch; wait for them."""
    while missing := [t for t in tables if not spark.catalog.tableExists(f"bronze.{t}")]:
        log.info("waiting for bronze tables: %s", ", ".join(missing))
        time.sleep(BRONZE_POLL_SECONDS)


def ensure_table(spark: SparkSession, spec: TableSpec, settings: Settings) -> DeltaTable:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {DATABASE}.{spec.name} ({silver_ddl(spec)}) USING DELTA "
        f"LOCATION '{settings.lakehouse_uri}/{DATABASE}/{spec.name}' "
        "TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')"
    )
    return DeltaTable.forName(spark, f"{DATABASE}.{spec.name}")


def upsert_batch(spec: TableSpec, target: DeltaTable):
    def handle(batch_df: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        latest = latest_per_key(parse(batch_df, spec), spec).persist()
        try:
            keys = latest.count()
            if keys:
                merge_latest(target, latest, spec)
            log.info(
                "table=%s batch=%s keys=%s duration_s=%.1f",
                spec.name,
                batch_id,
                keys,
                time.monotonic() - started,
            )
        finally:
            latest.unpersist()

    return handle


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s level=%(levelname)s logger=%(name)s %(message)s"
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    settings = load_settings()
    spark = build_session("silver-upsert-facts")
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DATABASE} LOCATION '{settings.lakehouse_uri}/{DATABASE}'"
    )
    wait_for_bronze(spark, list(FACTS))

    for spec in FACTS.values():
        target = ensure_table(spark, spec, settings)
        (
            spark.readStream.format("delta")
            .table(f"bronze.{spec.name}")
            .writeStream.queryName(f"silver_{spec.name}")
            .foreachBatch(upsert_batch(spec, target))
            .option("checkpointLocation", f"{settings.checkpoint_uri}/silver/{spec.name}")
            .trigger(processingTime=f"{settings.silver_trigger_seconds} seconds")
            .start()
        )
        log.info("started silver_%s", spec.name)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
