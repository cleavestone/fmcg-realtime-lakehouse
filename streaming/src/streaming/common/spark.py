"""The one SparkSession builder every job uses.

All engine settings (Delta, Hive Metastore, S3A/MinIO) live in conf/spark-defaults.conf,
baked into the image; S3 credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.
"""

import os

from pyspark.sql import SparkSession


def build_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # Must be set before the JVM starts, which happens in getOrCreate().
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
