"""Lake foundation smoke test: write a Delta table to MinIO, register it in the Hive
Metastore, read it back. Idempotent (overwrites). Exit code 0 = everything works.

    docker compose run --rm spark-bronze python -m streaming.smoke_test
"""

import sys

from pyspark.sql import functions as F

from streaming.common.spark import build_session

DATABASE = "smoke"
TABLE = f"{DATABASE}.numbers"
LOCATION = "s3a://lakehouse/smoke"


def main() -> int:
    spark = build_session("smoke-test")
    print(f"spark {spark.version}, catalog: {spark.conf.get('spark.sql.catalogImplementation')}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE} LOCATION '{LOCATION}'")
    df = (
        spark.range(1, 11)
        .withColumn("square", F.col("id") * F.col("id"))
        .withColumn("written_at", F.current_timestamp())
    )
    df.write.format("delta").mode("overwrite").saveAsTable(TABLE)
    print(f"wrote 10 rows to {TABLE}")

    # Read back through the metastore (by name, not by path).
    back = spark.table(TABLE)
    rows, squares = back.agg(F.count("*"), F.sum("square")).first()
    detail = spark.sql(f"DESCRIBE DETAIL {TABLE}").first()
    versions = spark.sql(f"DESCRIBE HISTORY {TABLE}").count()
    registered = [t.name for t in spark.catalog.listTables(DATABASE)]

    print(f"metastore tables in '{DATABASE}': {registered}")
    print(f"format={detail['format']} location={detail['location']} files={detail['numFiles']}")
    print(f"rows={rows} sum(square)={squares} delta_versions={versions}")

    ok = rows == 10 and squares == 385 and detail["format"] == "delta" and "numbers" in registered
    print("SMOKE TEST PASSED" if ok else "SMOKE TEST FAILED")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
