"""Shared PySpark transformation helpers used across domain pipelines."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def add_ingestion_metadata(df: DataFrame, source_system: str) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_system", F.lit(source_system)
    )


def dedup_latest_by_key(df: DataFrame, key_cols: list, order_col: str) -> DataFrame:
    """Keep only the latest CDC record per business key (last-write-wins)."""
    window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
