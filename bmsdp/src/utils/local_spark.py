"""SparkSession factory for running the pipeline logic locally against real
Delta Lake tables on disk. Only used by local scripts/tests - the actual
Databricks DLT pipeline gets its Delta-enabled SparkSession from the
runtime and never imports this module.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_local_delta_spark(app_name: str = "bmsdp-local") -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
