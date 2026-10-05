"""Silver dimensions: Bronze change log -> SCD Type 2 dim_store, dim_product, dim_sales_rep.

One streaming query per dimension, all in this Spark app (ADR-004). Each micro-batch is
turned into "close these versions / insert these versions" by streaming.silver.scd2 and
applied with one MERGE, so every change in the batch becomes its own version, even
several changes to one key within one batch.

    python -m streaming.silver.scd2_dimensions
"""

import logging
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.common.config import Settings, load_settings
from streaming.common.schemas import DIMENSIONS, DimensionSpec
from streaming.common.spark import build_session
from streaming.silver.facts import parse
from streaming.silver.scd2 import build_changes, dimension_ddl, merge_changes
from streaming.silver.upsert_facts import DATABASE, wait_for_bronze

log = logging.getLogger("silver.dims")


def ensure_table(spark: SparkSession, spec: DimensionSpec, settings: Settings) -> DeltaTable:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {DATABASE}.{spec.name} ({dimension_ddl(spec)}) USING DELTA "
        f"LOCATION '{settings.lakehouse_uri}/{DATABASE}/{spec.name}' "
        "TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')"
    )
    return DeltaTable.forName(spark, f"{DATABASE}.{spec.name}")


def version_batch(spec: DimensionSpec, target: DeltaTable):
    def handle(batch_df: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        events = parse(batch_df, spec).persist()
        try:
            keys = events.select(spec.key).distinct()
            # Only the stored versions of keys in this batch take part.
            existing = target.toDF().join(F.broadcast(keys), spec.key, "left_semi")
            changes = build_changes(events, existing, spec).persist()
            try:
                rows, still_open = changes.agg(
                    F.count("*"), F.sum(F.col("is_current").cast("int"))
                ).first()
                if rows:
                    merge_changes(target, changes, spec)
                log.info(
                    "dim=%s batch=%s events=%s rows_merged=%s open_versions=%s duration_s=%.1f",
                    spec.name,
                    batch_id,
                    events.count(),
                    rows,
                    still_open or 0,
                    time.monotonic() - started,
                )
            finally:
                changes.unpersist()
        finally:
            events.unpersist()

    return handle


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s level=%(levelname)s logger=%(name)s %(message)s"
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    settings = load_settings()
    spark = build_session("silver-scd2-dimensions")
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DATABASE} LOCATION '{settings.lakehouse_uri}/{DATABASE}'"
    )
    wait_for_bronze(spark, [d.source for d in DIMENSIONS.values()])

    for spec in DIMENSIONS.values():
        target = ensure_table(spark, spec, settings)
        (
            spark.readStream.format("delta")
            .table(f"bronze.{spec.source}")
            .writeStream.queryName(f"silver_{spec.name}")
            .foreachBatch(version_batch(spec, target))
            .option("checkpointLocation", f"{settings.checkpoint_uri}/{DATABASE}/{spec.name}")
            .trigger(processingTime=f"{settings.silver_trigger_seconds} seconds")
            .start()
        )
        log.info("started silver_%s", spec.name)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
