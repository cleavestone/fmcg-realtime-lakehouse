"""Check the SCD2 invariants on every Silver dimension.

Per natural key: at most one current version; windows contiguous (each valid_to equals the
next valid_from) and never overlapping; valid_from <= valid_to; the last version is either
current and open-ended or closed by a delete. Exit code 0 = all invariants hold.

    docker compose run --rm spark-tools python -m streaming.tools.scd2_check
"""

import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from streaming.common.schemas import DIMENSIONS
from streaming.common.spark import build_session
from streaming.silver.scd2 import END_OF_TIME


def main() -> int:
    spark = build_session("scd2-check")
    ok = True
    print(
        f"{'dimension':<14} {'version':>8} {'keys':>6} {'versions':>9} {'max/key':>8} "
        f"{'current':>8} {'deleted':>8} {'violations':>11}"
    )
    for spec in DIMENSIONS.values():
        # Pin one Delta version so every number below comes from the same snapshot, even
        # while the streaming job keeps committing.
        version = spark.sql(f"DESCRIBE HISTORY silver.{spec.name} LIMIT 1").first()["version"]
        df = spark.read.option("versionAsOf", version).table(f"silver.{spec.name}")
        w = Window.partitionBy(spec.key).orderBy("valid_from", "source_lsn")
        checked = df.select(
            spec.key,
            "valid_from",
            "valid_to",
            "is_current",
            "is_deleted",
            F.lead("valid_from").over(w).alias("next_from"),
            F.sum(F.col("is_current").cast("int"))
            .over(Window.partitionBy(spec.key))
            .alias("n_current"),
        )
        bad = checked.filter(
            (F.col("n_current") > 1)
            | (F.col("valid_from") > F.col("valid_to"))
            | (F.col("next_from").isNotNull() & (F.col("valid_to") != F.col("next_from")))
            | (F.col("next_from").isNotNull() & F.col("is_current"))
            | (
                F.col("next_from").isNull()
                & ~(
                    (F.col("is_current") & (F.col("valid_to") == F.lit(END_OF_TIME)))
                    | (~F.col("is_current") & F.col("is_deleted"))
                )
            )
        )
        stats = (
            df.groupBy(spec.key)
            .agg(F.count("*").alias("n"))
            .agg(F.count("*"), F.sum("n"), F.max("n"))
            .first()
        )
        current = df.filter("is_current").count()
        deleted = df.filter("is_deleted").count()
        violations = bad.count()
        ok &= violations == 0
        print(
            f"{spec.name:<14} {version:>8} {stats[0]:>6} {stats[1]:>9} {stats[2]:>8} "
            f"{current:>8} {deleted:>8} {violations:>11}"
        )
        if violations:
            bad.orderBy(spec.key, "valid_from").show(10, truncate=False)
    print("SCD2 INVARIANTS HOLD" if ok else "SCD2 INVARIANT VIOLATIONS FOUND")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
