"""Local SparkSession for unit tests (runs inside the Spark image: make test-streaming).

Ignores the image's spark-defaults.conf (Hive Metastore, MinIO) so tests are self-contained;
Delta is enabled because its JARs are already on the image's classpath.
"""

import os
import tempfile

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ["SPARK_CONF_DIR"] = tempfile.mkdtemp(prefix="spark-conf-")
    warehouse = tempfile.mkdtemp(prefix="spark-warehouse-")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("streaming-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
