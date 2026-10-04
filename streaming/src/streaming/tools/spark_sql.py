"""Run one SQL statement against the lake and print the result.

docker compose run --rm spark-tools python -m streaming.tools.spark_sql "SELECT 1"
"""

import sys

from streaming.common.spark import build_session


def main() -> int:
    if len(sys.argv) != 2:
        print('usage: python -m streaming.tools.spark_sql "<sql>"', file=sys.stderr)
        return 2
    spark = build_session("spark-sql")
    spark.sql(sys.argv[1]).show(n=100, truncate=False)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
