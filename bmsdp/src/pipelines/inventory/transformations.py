"""Bronze -> Silver -> Gold transformations for the Inventory & Seating domain.

Same design as the Bookings domain: pure DataFrame-in/DataFrame-out
functions so they're unit testable locally, wired to DLT in
dlt_pipeline.py for the real Databricks deployment.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.spark_helpers import dedup_latest_by_key

VALID_HOLD_STATUSES = {"HELD", "BOOKED", "RELEASED", "EXPIRED"}
OCCUPIED_STATUSES = {"HELD", "BOOKED"}


def clean_seat_holds_silver(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze seat-hold events -> (silver latest-state-per-seat, quarantined).

    A seat can flip states multiple times per show (HELD -> BOOKED, or
    HELD -> EXPIRED); only the latest state per (show_id, seat_no) matters
    for current occupancy.
    """
    is_valid = (
        F.col("show_id").isNotNull()
        & F.col("seat_no").isNotNull()
        & F.col("hold_status").isin(*VALID_HOLD_STATUSES)
    )
    valid = bronze_df.filter(is_valid)
    quarantined = bronze_df.filter(~is_valid)

    deduped = dedup_latest_by_key(valid, ["show_id", "seat_no"], "updated_at")
    return deduped, quarantined


def build_fact_seat_occupancy(silver_seat_holds: DataFrame, dim_venue: DataFrame, dim_show: DataFrame) -> DataFrame:
    """Per-show occupancy: capacity (from the venue), occupied seat count
    (held or booked), and occupancy percentage.
    """
    show_venue = dim_show.filter(F.col("is_current")).select(
        F.col("show_id"), F.col("venue_id"), F.col("show_time")
    )
    venue_capacity = dim_venue.filter(F.col("is_current")).select(
        F.col("venue_id"), F.col("capacity")
    )

    occupied_per_show = (
        silver_seat_holds.filter(F.col("hold_status").isin(*OCCUPIED_STATUSES))
        .groupBy("show_id")
        .agg(F.countDistinct("seat_no").alias("occupied_seats"))
    )

    return (
        show_venue.join(venue_capacity, on="venue_id", how="left")
        .join(occupied_per_show, on="show_id", how="left")
        .withColumn("occupied_seats", F.coalesce(F.col("occupied_seats"), F.lit(0)))
        .withColumn(
            "occupancy_pct",
            F.when(
                F.col("capacity").isNotNull() & (F.col("capacity") > 0),
                F.round(F.col("occupied_seats") / F.col("capacity") * 100, 2),
            ).otherwise(F.lit(None)),
        )
    )
