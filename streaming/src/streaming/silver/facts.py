"""Pure Silver-facts logic: typed parsing, latest-event-per-key, LSN-guarded MERGE.

Ordering always uses source_lsn (commit order), never arrival order; the Kafka offset only
breaks ties, which can't happen for one key outside a single transaction.
"""

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from streaming.common.schemas import BIGINT, BOOLEAN, STRING, TIMESTAMP, TableSpec

# Columns every Silver fact table carries after the business columns.
METADATA_COLUMNS = (
    ("source_lsn", BIGINT),
    ("source_ts_ms", BIGINT),
    ("is_deleted", BOOLEAN),
    ("silver_updated_at", TIMESTAMP),
)


def silver_ddl(spec: TableSpec) -> str:
    return ", ".join(f"{c} {t}" for c, t in (*spec.columns, *METADATA_COLUMNS))


def _typed(column: str, sql_type: str) -> Column:
    # Every payload field is extracted as a string, then cast: robust to numbers sent as
    # strings (decimals) and to ISO-8601 timestamps with a trailing Z.
    raw = F.get_json_object(F.col("payload"), f"$.{column}")
    if sql_type == BOOLEAN:
        return (raw == "true").alias(column)
    if sql_type == STRING:
        return raw.alias(column)
    return raw.cast(sql_type).alias(column)


def parse(bronze_df: DataFrame, spec) -> DataFrame:
    """Bronze rows (raw payload + CDC metadata) -> typed business columns + metadata.

    `spec` is a TableSpec or DimensionSpec (anything with `.columns`).
    """
    return bronze_df.select(
        *[_typed(c, t) for c, t in spec.columns],
        "op",
        "source_lsn",
        "source_ts_ms",
        "is_deleted",
        "kafka_offset",
    )


def latest_per_key(df: DataFrame, spec: TableSpec) -> DataFrame:
    """Collapse a batch to one row per key.

    LSN/time/is_deleted come from the key's newest event. Business values come from the
    newest *non-delete* event when the batch has one (`has_values`): an update followed by a
    delete in the same batch must still land the update's values. A batch whose only event
    for a key is a delete keeps the delete's payload with has_values = false, so the MERGE
    sets just the flag on an existing row.
    """
    keys = list(spec.keys)
    by_lsn = [F.col("source_lsn").desc(), F.col("kafka_offset").desc()]
    newest = (
        df.withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy(*by_lsn)))
        .filter("_rn = 1")
        .drop("_rn", "kafka_offset")
    )
    live = (
        df.filter(~F.col("is_deleted"))
        .withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy(*by_lsn)))
        .filter("_rn = 1")
        .select(*keys, *[F.col(c).alias(f"_live_{c}") for c in spec.column_names if c not in keys])
        .withColumn("_live", F.lit(True))
    )
    has_live = F.col("_live").isNotNull()
    return newest.join(live, keys, "left").select(
        *[
            F.col(c)
            if c in keys
            else F.when(has_live, F.col(f"_live_{c}")).otherwise(F.col(c)).alias(c)
            for c in spec.column_names
        ],
        "source_lsn",
        "source_ts_ms",
        "is_deleted",
        (has_live | ~F.col("is_deleted")).alias("has_values"),
        F.current_timestamp().alias("silver_updated_at"),
    )


def merge_latest(target: DeltaTable, latest: DataFrame, spec: TableSpec) -> None:
    """Apply a batch: newer LSN wins, older/equal LSN is ignored (replays, re-sends).

    A delete without a newer non-delete event in the batch only flips is_deleted (plus its
    LSN/time) and never overwrites business values. A delete for a key Silver has never seen
    is inserted as a deleted row, keeping the record.
    """
    on = " AND ".join(f"t.{k} = s.{k}" for k in spec.keys)
    newer = "s.source_lsn > t.source_lsn"
    all_columns = [*spec.column_names, *(c for c, _ in METADATA_COLUMNS)]
    (
        target.alias("t")
        .merge(latest.alias("s"), on)
        .whenMatchedUpdate(
            condition=f"{newer} AND s.is_deleted AND NOT s.has_values",
            set={
                "is_deleted": "true",
                "source_lsn": "s.source_lsn",
                "source_ts_ms": "s.source_ts_ms",
                "silver_updated_at": "s.silver_updated_at",
            },
        )
        .whenMatchedUpdate(condition=newer, set={c: f"s.{c}" for c in all_columns})
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in all_columns})
        .execute()
    )
