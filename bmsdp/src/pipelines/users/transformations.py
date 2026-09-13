"""Gold-layer Users 360 domain: per-user booking behavior summary built on
top of the Bookings domain's fact_bookings and dim_user, for personalization
and recommendation use cases (per the platform objective).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_user_booking_summary(fact_bookings: DataFrame, dim_user: DataFrame) -> DataFrame:
    """One row per user: booking counts/spend/cancellation behavior plus
    their favorite genre-proxy (most-visited venue) and recency, the kind of
    feature set a recommendation or churn model would start from.
    """
    per_user = fact_bookings.groupBy("user_id").agg(
        F.count("booking_id").alias("total_bookings"),
        F.sum("revenue").alias("lifetime_spend"),
        F.sum(F.when(F.col("booking_status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_bookings"),
        F.min("booking_date").alias("first_booking_date"),
        F.max("booking_date").alias("last_booking_date"),
        F.avg("num_seats").alias("avg_seats_per_booking"),
    )
    cancellation_rate = F.round(F.col("cancelled_bookings") / F.col("total_bookings"), 4)
    per_user = per_user.withColumn(
        "cancellation_rate",
        F.when(F.col("total_bookings") > 0, cancellation_rate).otherwise(F.lit(0.0)),
    )

    current_users = dim_user.filter(F.col("is_current")).select("user_id", "city", "signup_date")
    return current_users.join(per_user, on="user_id", how="left").fillna(
        {"total_bookings": 0, "lifetime_spend": 0.0, "cancelled_bookings": 0, "cancellation_rate": 0.0}
    )
