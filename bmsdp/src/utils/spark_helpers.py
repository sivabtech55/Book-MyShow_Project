"""Shared PySpark transformation helpers used across domain pipelines."""

import os
import sys

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def pin_pyspark_worker_python() -> None:
    """Force PySpark's worker subprocesses to use the same interpreter as
    the driver. Without this, PySpark falls back to whatever `python3`
    resolves to on PATH, which only happens to match when a venv has been
    "activated" in a shell - launching the interpreter by absolute path
    (as an IDE's test runner or debugger does) skips that PATH change and
    triggers a PYTHON_VERSION_MISMATCH error against the system Python.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


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
