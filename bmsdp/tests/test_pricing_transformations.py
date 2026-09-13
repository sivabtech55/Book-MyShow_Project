from datetime import datetime

from src.pipelines.pricing import transformations as tr


def _ts(s):
    return datetime.fromisoformat(s)


def test_clean_promotions_silver_filters_bad_records(spark):
    rows = [
        ("P1", "PERCENT", 10.0, "ACTIVE", _ts("2026-01-01T00:00:00")),
        ("P2", "MYSTERY", 10.0, "ACTIVE", _ts("2026-01-01T00:00:00")),  # bad discount type
        ("P3", "FLAT", 0.0, "ACTIVE", _ts("2026-01-01T00:00:00")),  # zero discount
        ("P4", "FLAT", 50.0, "UNKNOWN", _ts("2026-01-01T00:00:00")),  # bad status
    ]
    df = spark.createDataFrame(rows, ["promotion_id", "discount_type", "discount_value", "status", "updated_at"])

    valid, quarantined = tr.clean_promotions_silver(df)

    assert valid.count() == 1
    assert valid.collect()[0].promotion_id == "P1"
    assert quarantined.count() == 3


def test_clean_redemptions_silver_filters_missing_refs(spark):
    rows = [
        ("R1", "P1", "B1", 50.0, _ts("2026-01-01T00:00:00")),
        ("R2", None, "B2", 20.0, _ts("2026-01-01T00:00:00")),
        ("R3", "P1", "B3", -10.0, _ts("2026-01-01T00:00:00")),
    ]
    df = spark.createDataFrame(rows, ["redemption_id", "promotion_id", "booking_id", "discount_amount", "updated_at"])

    result = tr.clean_redemptions_silver(df)

    assert result.count() == 1
    assert result.collect()[0].redemption_id == "R1"


def test_build_fact_promotion_redemptions_computes_net_revenue(spark):
    silver_redemptions = spark.createDataFrame(
        [("R1", "P1", "B1", 50.0)], ["redemption_id", "promotion_id", "booking_id", "discount_amount"]
    )
    silver_promotions = spark.createDataFrame([("P1", "SAVE50", "FLAT")], ["promotion_id", "code", "discount_type"])
    fact_bookings = spark.createDataFrame(
        [("B1", "V1", "M1", 300.0)], ["booking_id", "venue_id", "movie_id", "revenue"]
    )

    result = tr.build_fact_promotion_redemptions(silver_redemptions, silver_promotions, fact_bookings).collect()[0]

    assert result.net_revenue == 250.0
    assert result.code == "SAVE50"


def test_build_fact_promotion_redemptions_floors_net_revenue_at_zero(spark):
    silver_redemptions = spark.createDataFrame(
        [("R1", "P1", "B1", 500.0)], ["redemption_id", "promotion_id", "booking_id", "discount_amount"]
    )
    silver_promotions = spark.createDataFrame([("P1", "BIGSAVE", "FLAT")], ["promotion_id", "code", "discount_type"])
    fact_bookings = spark.createDataFrame(
        [("B1", "V1", "M1", 300.0)], ["booking_id", "venue_id", "movie_id", "revenue"]
    )

    result = tr.build_fact_promotion_redemptions(silver_redemptions, silver_promotions, fact_bookings).collect()[0]

    assert result.net_revenue == 0.0
