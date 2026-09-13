import pytest
from pyspark.sql import SparkSession

from src.utils.spark_helpers import pin_pyspark_worker_python

pin_pyspark_worker_python()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("bmsdp-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
