"""Drop lake tables and their streaming checkpoints so a job rebuilds them from scratch.

Used to rebuild Silver from Bronze (Bronze is the replay point, ADR-003). Stop the job
first. Data files and checkpoints are deleted from MinIO, not just unregistered.

    python -m streaming.tools.reset_tables silver orders order_items inventory
"""

import sys

from streaming.common.config import load_settings
from streaming.common.spark import build_session


def main() -> int:
    if len(sys.argv) < 3:
        print(
            "usage: python -m streaming.tools.reset_tables <database> <table>...", file=sys.stderr
        )
        return 2
    database, tables = sys.argv[1], sys.argv[2:]
    settings = load_settings()
    spark = build_session("reset-tables")
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    def delete(uri: str) -> bool:
        path = jvm.org.apache.hadoop.fs.Path(uri)
        return path.getFileSystem(hadoop_conf).delete(path, True)

    for table in tables:
        location = f"{settings.lakehouse_uri}/{database}/{table}"
        checkpoint = f"{settings.checkpoint_uri}/{database}/{table}"
        spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
        print(
            f"{database}.{table}: dropped; data deleted={delete(location)}; "
            f"checkpoint deleted={delete(checkpoint)}"
        )
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
