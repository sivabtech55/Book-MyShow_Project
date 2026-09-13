"""Bronze -> Silver -> Gold transformations for the Pricing & Promotions domain."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.spark_helpers import dedup_latest_by_key

VALID_DISCOUNT_TYPES = {"PERCENT", "FLAT"}
VALID_PROMOTION_STATUSES = {"ACTIVE", "EXPIRED", "DISABLED"}


def clean_promotions_silver(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    is_valid = (
        F.col("promotion_id").isNotNull()
        & F.col("discount_type").isin(*VALID_DISCOUNT_TYPES)
        & F.col("discount_value").isNotNull()
        & (F.col("discount_value") > 0)
        & F.col("status").isin(*VALID_PROMOTION_STATUSES)
    )
    valid = bronze_df.filter(is_valid)
    quarantined = bronze_df.filter(~is_valid)
    deduped = dedup_latest_by_key(valid, ["promotion_id"], "updated_at")
    return deduped, quarantined


def clean_redemptions_silver(bronze_df: DataFrame) -> DataFrame:
    deduped = dedup_latest_by_key(bronze_df, ["redemption_id"], "updated_at")
    return deduped.filter(
        F.col("promotion_id").isNotNull()
        & F.col("booking_id").isNotNull()
        & F.col("discount_amount").isNotNull()
        & (F.col("discount_amount") >= 0)
    )


def build_fact_promotion_redemptions(
    silver_redemptions: DataFrame,
    silver_promotions: DataFrame,
    fact_bookings: DataFrame,
) -> DataFrame:
    """Redemption-grain fact: discount applied, resulting net revenue for
    that booking, and the promotion's code for reporting.
    """
    promo_lookup = silver_promotions.select("promotion_id", "code", "discount_type")
    booking_context = fact_bookings.select(
        "booking_id", "venue_id", "movie_id", "revenue"
    ).withColumnRenamed("revenue", "gross_revenue")

    return (
        silver_redemptions.join(promo_lookup, on="promotion_id", how="left")
        .join(booking_context, on="booking_id", how="left")
        .withColumn(
            "net_revenue",
            F.greatest(F.col("gross_revenue") - F.col("discount_amount"), F.lit(0.0)),
        )
    )


def build_promotion_performance_agg(fact_promotion_redemptions: DataFrame) -> DataFrame:
    return fact_promotion_redemptions.groupBy("promotion_id", "code").agg(
        F.count("redemption_id").alias("total_redemptions"),
        F.sum("discount_amount").alias("total_discount_given"),
        F.sum("net_revenue").alias("total_net_revenue"),
    )
