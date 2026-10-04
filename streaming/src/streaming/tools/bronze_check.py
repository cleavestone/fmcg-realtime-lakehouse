"""Bronze health check: rows per table and op, quarantine count, and the exactly-once
check that no Kafka record (topic, partition, offset) was written twice.

    docker compose run --rm spark-bronze python -m streaming.tools.bronze_check
"""

import sys

from pyspark.sql import functions as F

from streaming.common.spark import build_session


def main() -> int:
    spark = build_session("bronze-check")
    tables = sorted(t.name for t in spark.catalog.listTables("bronze"))
    if not tables:
        print("no bronze tables yet")
        return 1

    ok = True
    print(f"{'table':<12} {'rows':>8} {'r':>7} {'c':>7} {'u':>7} {'d':>6} {'dup_offsets':>11}")
    for table in tables:
        df = spark.table(f"bronze.{table}")
        if table == "quarantine":
            reasons = [r[0] for r in df.select("quarantine_reason").distinct().collect()]
            print(f"{table:<12} {df.count():>8}  (reasons: {reasons})")
            continue
        by_op = dict(df.groupBy("op").count().collect())
        dups = (
            df.groupBy("kafka_topic", "kafka_partition", "kafka_offset")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        total = sum(by_op.values())
        ok &= dups == 0
        print(
            f"{table:<12} {total:>8} {by_op.get('r', 0):>7} {by_op.get('c', 0):>7} "
            f"{by_op.get('u', 0):>7} {by_op.get('d', 0):>6} {dups:>11}"
        )
    print("NO DUPLICATES" if ok else "DUPLICATE OFFSETS FOUND")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
