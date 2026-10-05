"""SCD Type 2 versioning for one micro-batch (pure logic, unit-tested).

For each natural key, the batch's events are laid out after the version Silver already
holds (the "anchor"), in LSN order:

    anchor v1 (current)  ->  event A  ->  event B (no-op)  ->  event C  ->  delete
         close v1             version       dropped           version      closes C

- Events at or below the key's latest stored LSN are ignored (replays, re-sends).
- An event whose tracked attributes hash the same as the previous state is a no-op.
- Each remaining event becomes a version. Its window is [own commit time, next boundary's
  commit time); the last one stays open until 9999-12-31 and is_current.
- A delete creates no version: it closes the version before it and marks it is_deleted.
- The very first version of an entity that came from the Debezium snapshot (op = 'r') is
  valid from 1900-01-01: history before CDC started is unknown, and the backfilled orders
  must still find a version in Gold's point-in-time join.

The output is one row per surrogate key, either an existing version to close or a new
version to insert, applied by a single MERGE, so a batch lands atomically. Surrogate keys
are deterministic (hash of natural key + version LSN), so a replayed batch inserts nothing.
"""

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from streaming.common.schemas import BIGINT, BOOLEAN, STRING, TIMESTAMP, DimensionSpec

BEGINNING_OF_TIME = datetime(1900, 1, 1)
END_OF_TIME = datetime(9999, 12, 31)

METADATA_COLUMNS = (
    ("row_hash", STRING),
    ("valid_from", TIMESTAMP),
    ("valid_to", TIMESTAMP),
    ("is_current", BOOLEAN),
    ("is_deleted", BOOLEAN),
    ("source_lsn", BIGINT),
    ("source_ts_ms", BIGINT),
    ("silver_updated_at", TIMESTAMP),
)


def dimension_ddl(spec: DimensionSpec) -> str:
    cols = [(spec.surrogate, BIGINT), *spec.columns, *METADATA_COLUMNS]
    return ", ".join(f"{c} {t}" for c, t in cols)


def all_columns(spec: DimensionSpec) -> list[str]:
    return [spec.surrogate, *spec.column_names, *(c for c, _ in METADATA_COLUMNS)]


def row_hash(spec: DimensionSpec) -> Column:
    """SHA-256 over the tracked attributes (null-safe, unambiguous separator)."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("␀")) for c in spec.tracked]
    return F.sha2(F.concat_ws("\u0001", *parts), 256)


def surrogate_key(spec: DimensionSpec) -> Column:
    return F.xxhash64(F.col(spec.key), F.col("source_lsn"))


def build_changes(events: DataFrame, existing: DataFrame, spec: DimensionSpec) -> DataFrame:
    """Rows to MERGE for this batch.

    `events`: parsed Bronze rows (typed columns + op, source_lsn, source_ts_ms, is_deleted,
    kafka_offset). `existing`: the dimension's stored rows (any subset that includes every
    version of the batch's keys).
    """
    key = spec.key
    by_lsn = [F.col("source_lsn"), F.col("kafka_offset")]

    # The key's latest stored version (current or, after a delete, the closed one).
    latest = (
        existing.withColumn(
            "_rn",
            F.row_number().over(Window.partitionBy(key).orderBy(F.col("source_lsn").desc())),
        )
        .filter("_rn = 1")
        .drop("_rn")
    )
    known = latest.select(key, F.col("source_lsn").alias("_latest_lsn"))

    # 1. Drop events the dimension has already absorbed.
    fresh = (
        events.withColumn("row_hash", row_hash(spec))
        .join(known, key, "left")
        .filter(F.col("_latest_lsn").isNull() | (F.col("source_lsn") > F.col("_latest_lsn")))
        .withColumn("_has_history", F.col("_latest_lsn").isNotNull())
        .drop("_latest_lsn")
    )

    # 2. Change detection along each key's chain: anchor (if current) + non-delete events.
    anchor_chain = latest.filter("is_current").select(
        key,
        "row_hash",
        "source_lsn",
        F.lit(-1).cast("bigint").alias("kafka_offset"),
        F.lit(True).alias("_is_anchor"),
    )
    event_chain = fresh.filter(~F.col("is_deleted")).select(
        key, "row_hash", "source_lsn", "kafka_offset", F.lit(False).alias("_is_anchor")
    )
    chain = anchor_chain.unionByName(event_chain).withColumn(
        "_prev_hash", F.lag("row_hash").over(Window.partitionBy(key).orderBy(*by_lsn))
    )
    changed = chain.filter(
        ~F.col("_is_anchor")
        & (F.col("_prev_hash").isNull() | (F.col("row_hash") != F.col("_prev_hash")))
    ).select(key, "source_lsn", "kafka_offset")
    versions = fresh.join(changed, [key, "source_lsn", "kafka_offset"], "inner")

    # 3. Boundaries in LSN order: open anchor, new versions, deletes. Each element's window
    #    ends where the next one starts.
    ts = F.timestamp_millis(F.col("source_ts_ms"))
    boundaries = (
        latest.filter("is_current")
        .select(
            key, "source_lsn", F.col("valid_from").alias("_start"), F.lit("anchor").alias("_kind")
        )
        .unionByName(
            versions.select(key, "source_lsn", ts.alias("_start"), F.lit("version").alias("_kind"))
        )
        .unionByName(
            fresh.filter("is_deleted").select(
                key, "source_lsn", ts.alias("_start"), F.lit("delete").alias("_kind")
            )
        )
    )
    w = Window.partitionBy(key).orderBy("source_lsn")
    boundaries = boundaries.select(
        key,
        "source_lsn",
        "_kind",
        F.lead("_start").over(w).alias("_next_start"),
        F.lead("_kind").over(w).alias("_next_kind"),
        (F.row_number().over(w) == 1).alias("_first"),
    )

    now = F.current_timestamp()

    # 4a. Existing current versions that something in this batch closes.
    closes = (
        latest.filter("is_current")
        .join(
            boundaries.filter("_kind = 'anchor' AND _next_start IS NOT NULL"),
            [key, "source_lsn"],
        )
        .withColumn("valid_to", F.col("_next_start"))
        .withColumn("is_current", F.lit(False))
        .withColumn("is_deleted", F.col("_next_kind") == "delete")
        .withColumn("silver_updated_at", now)
        .select(*all_columns(spec))
    )

    # 4b. New versions.
    first_ever = F.col("_first") & ~F.col("_has_history") & (F.col("op") == "r")
    inserts = (
        versions.join(boundaries.filter("_kind = 'version'"), [key, "source_lsn"])
        .withColumn(spec.surrogate, surrogate_key(spec))
        .withColumn("valid_from", F.when(first_ever, F.lit(BEGINNING_OF_TIME)).otherwise(ts))
        .withColumn("valid_to", F.coalesce(F.col("_next_start"), F.lit(END_OF_TIME)))
        .withColumn("is_current", F.col("_next_start").isNull())
        .withColumn("is_deleted", F.coalesce(F.col("_next_kind") == "delete", F.lit(False)))
        .withColumn("silver_updated_at", now)
        .select(*all_columns(spec))
    )
    return closes.unionByName(inserts)


def merge_changes(target: DeltaTable, changes: DataFrame, spec: DimensionSpec) -> None:
    """One atomic MERGE: close existing versions, insert new ones (keyed by surrogate)."""
    (
        target.alias("t")
        .merge(changes.alias("s"), f"t.{spec.surrogate} = s.{spec.surrogate}")
        .whenMatchedUpdate(
            set={
                "valid_to": "s.valid_to",
                "is_current": "s.is_current",
                "is_deleted": "s.is_deleted",
                "silver_updated_at": "s.silver_updated_at",
            }
        )
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in all_columns(spec)})
        .execute()
    )
