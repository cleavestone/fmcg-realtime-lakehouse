"""Pure Bronze logic: Kafka records in, Bronze rows out (no I/O, unit-tested).

The payload stays the raw JSON string Debezium produced, so Bronze never loses a field when
the source schema changes; Silver owns the typed per-table schemas. Only the CDC metadata
needed for routing, ordering and deletes is parsed here. A record that can't be trusted
gets a quarantine_reason instead of being dropped.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

VALID_OPS = ("c", "u", "d", "r")
TABLE_NAME_PATTERN = r"^[a-z_][a-z0-9_]*$"

# Column order of every bronze.<table>.
BRONZE_COLUMNS = [
    "kafka_key",
    "payload",
    "op",
    "source_ts_ms",
    "source_lsn",
    "is_deleted",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "ingest_date",
]

QUARANTINE_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_key",
    "payload",
    "quarantine_reason",
    "ingested_at",
    "ingest_date",
]


def _json(field: str) -> Column:
    path = "$" if field == "$" else f"$.{field}"
    return F.get_json_object(F.col("payload"), path)


def parse_cdc(kafka_df: DataFrame) -> DataFrame:
    """Add routing (`table`), CDC metadata and `quarantine_reason` (null = valid)."""
    df = kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    df = df.select(
        "*",
        F.element_at(F.split("kafka_topic", r"\."), -1).alias("table"),
        _json("__op").alias("op"),
        _json("__source_ts_ms").cast("bigint").alias("source_ts_ms"),
        _json("__source_lsn").cast("bigint").alias("source_lsn"),
        (F.coalesce(_json("__deleted"), F.lit("false")) == "true").alias("is_deleted"),
        F.current_timestamp().alias("ingested_at"),
    ).withColumn("ingest_date", F.to_date("ingested_at"))

    reason = (
        F.when(F.col("payload").isNull(), "null value (tombstone)")
        .when(_json("$").isNull() & F.col("payload").isNotNull(), "invalid json")
        .when(~F.col("table").rlike(TABLE_NAME_PATTERN), "invalid table name")
        .when(F.col("op").isNull() | ~F.col("op").isin(*VALID_OPS), "missing or unknown __op")
        .when(F.col("source_lsn").isNull(), "missing __source_lsn")
        .when(F.col("source_ts_ms").isNull(), "missing __source_ts_ms")
    )
    return df.withColumn("quarantine_reason", reason)
