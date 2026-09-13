"""Pure Bronze -> Silver -> Gold transformations for the Bookings domain.

These functions take/return DataFrames only (no DLT/Databricks-only APIs),
so they can be unit tested with a local SparkSession and reused inside the
DLT pipeline defined in dlt_pipeline.py. Keeping business logic out of the
`@dlt.table` decorators is what makes it testable outside a Databricks
cluster.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.spark_helpers import dedup_latest_by_key

VALID_BOOKING_STATUSES = {"CONFIRMED", "CANCELLED", "PENDING"}


# ---------------------------------------------------------------------------
# Silver: dedup, cleanse, quarantine bad records
# ---------------------------------------------------------------------------
def split_valid_invalid_bookings(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Data-quality gate for bookings: required keys present, sane amounts,
    known status. Returns (valid, quarantined) DataFrames.
    """
    is_valid = (
        F.col("booking_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("show_id").isNotNull()
        & F.col("num_seats").isNotNull()
        & (F.col("num_seats") > 0)
        & F.col("total_amount").isNotNull()
        & (F.col("total_amount") >= 0)
        & F.col("booking_status").isin(*VALID_BOOKING_STATUSES)
    )
    valid = df.filter(is_valid)
    quarantined = df.filter(~is_valid)
    return valid, quarantined


def clean_bookings_silver(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze bookings -> (silver valid bookings, quarantined bookings)."""
    deduped = dedup_latest_by_key(bronze_df, ["booking_id"], "updated_at")
    deduped = deduped.filter(
        (F.col("is_deleted").isNull()) | (~F.col("is_deleted"))
    )
    valid, quarantined = split_valid_invalid_bookings(deduped)
    valid = valid.withColumn("booking_status", F.upper(F.trim(F.col("booking_status"))))
    return valid, quarantined


def clean_payments_silver(bronze_df: DataFrame) -> DataFrame:
    deduped = dedup_latest_by_key(bronze_df, ["payment_id"], "updated_at")
    return deduped.filter(
        F.col("booking_id").isNotNull() & F.col("amount").isNotNull() & (F.col("amount") >= 0)
    ).withColumn("payment_status", F.upper(F.trim(F.col("payment_status"))))


def clean_dimension_silver(bronze_df: DataFrame, key_col: str) -> DataFrame:
    """Generic cleanse+dedup for reference/dimension sources (users, venues,
    movies, shows): drop null-key rows, keep latest version per key.
    """
    filtered = bronze_df.filter(F.col(key_col).isNotNull())
    return dedup_latest_by_key(filtered, [key_col], "updated_at")


# ---------------------------------------------------------------------------
# Silver -> Gold: SCD Type 2 dimension build
# ---------------------------------------------------------------------------
def build_scd2(
    current_dim: DataFrame,
    new_source: DataFrame,
    key_col: str,
    tracked_cols: list,
    as_of: str | None = None,
) -> DataFrame:
    """Apply an SCD Type 2 merge of `new_source` onto `current_dim`.

    `current_dim` is expected to already have SCD2 columns
    (effective_start, effective_end, is_current); pass an empty DataFrame
    with that shape to bootstrap. Rows whose tracked columns changed get
    their old version closed out and a new version opened.
    """
    as_of_ts = F.lit(as_of).cast("timestamp") if as_of else F.current_timestamp()

    open_rows = current_dim.filter(F.col("is_current"))
    closed_rows = current_dim.filter(~F.col("is_current"))

    joined = open_rows.alias("cur").join(
        new_source.alias("new"), on=key_col, how="full_outer"
    )

    changed_cond = F.lit(False)
    for c in tracked_cols:
        changed_cond = changed_cond | (
            ~F.col(f"cur.{c}").eqNullSafe(F.col(f"new.{c}"))
        )

    unchanged = joined.filter(
        F.col("cur." + key_col).isNotNull()
        & F.col("new." + key_col).isNotNull()
        & ~changed_cond
    ).select("cur.*")

    to_close = joined.filter(
        F.col("cur." + key_col).isNotNull()
        & F.col("new." + key_col).isNotNull()
        & changed_cond
    ).select(
        *[F.col(f"cur.{c}").alias(c) for c in current_dim.columns if c != "effective_end" and c != "is_current"],
        F.col("cur.effective_end").alias("_unused_end"),
    )
    to_close = to_close.withColumn("effective_end", as_of_ts).withColumn(
        "is_current", F.lit(False)
    ).drop("_unused_end")

    new_versions = joined.filter(
        F.col("new." + key_col).isNotNull()
        & (F.col("cur." + key_col).isNull() | changed_cond)
    ).select("new.*")
    new_versions = (
        new_versions.withColumn("effective_start", as_of_ts)
        .withColumn("effective_end", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    ordered_cols = current_dim.columns
    result = (
        unchanged.select(*ordered_cols)
        .unionByName(to_close.select(*ordered_cols))
        .unionByName(new_versions.select(*ordered_cols))
        .unionByName(closed_rows.select(*ordered_cols))
    )
    return result


def bootstrap_scd2(source: DataFrame, as_of: str | None = None) -> DataFrame:
    """Build the initial SCD2 dimension from a first full load."""
    as_of_ts = F.lit(as_of).cast("timestamp") if as_of else F.current_timestamp()
    return (
        source.withColumn("effective_start", as_of_ts)
        .withColumn("effective_end", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )


# ---------------------------------------------------------------------------
# Gold: fact tables
# ---------------------------------------------------------------------------
def build_fact_bookings(
    silver_bookings: DataFrame,
    dim_show: DataFrame,
    dim_venue: DataFrame,
) -> DataFrame:
    """fact_bookings at booking grain, enriched with venue/show keys and a
    computed revenue column (0 for cancelled bookings).
    """
    show_venue = dim_show.filter(F.col("is_current")).select(
        "show_id", "movie_id", "venue_id", "show_time", "base_price"
    )
    enriched = silver_bookings.join(show_venue, on="show_id", how="left")
    return enriched.withColumn(
        "revenue",
        F.when(F.col("booking_status") == "CONFIRMED", F.col("total_amount")).otherwise(
            F.lit(0.0)
        ),
    ).withColumn("booking_date", F.to_date("booking_time"))


def build_fact_payments(silver_payments: DataFrame, fact_bookings: DataFrame) -> DataFrame:
    """fact_payments at payment grain, enriched with venue/movie context from
    fact_bookings so payment analysis doesn't need a runtime join to bookings.
    """
    booking_context = fact_bookings.select(
        "booking_id", "venue_id", "movie_id", "booking_status"
    ).withColumnRenamed("booking_status", "booking_status_at_fact_time")
    return silver_payments.join(booking_context, on="booking_id", how="left")


def build_daily_revenue_agg(fact_bookings: DataFrame) -> DataFrame:
    return fact_bookings.groupBy("booking_date", "venue_id").agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum(
            F.when(F.col("booking_status") == "CANCELLED", 1).otherwise(0)
        ).alias("cancelled_count"),
        F.count("booking_id").alias("total_bookings"),
        F.sum("num_seats").alias("total_seats_booked"),
    )


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()
