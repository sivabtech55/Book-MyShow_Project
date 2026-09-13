from datetime import datetime

from pyspark.sql import functions as F

from src.pipelines.bookings import transformations as tr


def _ts(s):
    return datetime.fromisoformat(s)


def test_dedup_latest_by_key_keeps_newest(spark):
    rows = [
        ("B1", "CONFIRMED", _ts("2026-01-01T00:00:00")),
        ("B1", "CANCELLED", _ts("2026-01-02T00:00:00")),
        ("B2", "CONFIRMED", _ts("2026-01-01T00:00:00")),
    ]
    df = spark.createDataFrame(rows, ["booking_id", "booking_status", "updated_at"])

    result = tr.dedup_latest_by_key(df, ["booking_id"], "updated_at").collect()
    result_by_id = {r.booking_id: r.booking_status for r in result}

    assert len(result) == 2
    assert result_by_id["B1"] == "CANCELLED"
    assert result_by_id["B2"] == "CONFIRMED"


def test_split_valid_invalid_bookings_filters_bad_rows(spark):
    rows = [
        ("B1", "U1", "S1", 2, 300.0, "CONFIRMED"),
        ("B2", "U2", "S1", 1, -50.0, "CONFIRMED"),  # negative amount
        ("B3", None, "S1", 1, 100.0, "CONFIRMED"),  # missing user
        ("B4", "U3", "S1", 1, 100.0, "UNKNOWN"),  # bad status
        ("B5", "U4", "S1", 0, 100.0, "CONFIRMED"),  # zero seats
    ]
    df = spark.createDataFrame(
        rows, ["booking_id", "user_id", "show_id", "num_seats", "total_amount", "booking_status"]
    )

    valid, quarantined = tr.split_valid_invalid_bookings(df)

    assert valid.count() == 1
    assert valid.collect()[0].booking_id == "B1"
    assert quarantined.count() == 4


def test_clean_bookings_silver_dedups_and_drops_deleted(spark):
    rows = [
        ("B1", "U1", "S1", 2, 300.0, "CONFIRMED", _ts("2026-01-01T00:00:00"), _ts("2026-01-01T00:00:00"), False),
        ("B1", "U1", "S1", 2, 300.0, "CANCELLED", _ts("2026-01-01T00:00:00"), _ts("2026-01-02T00:00:00"), False),
        ("B2", "U2", "S1", 1, 150.0, "CONFIRMED", _ts("2026-01-01T00:00:00"), _ts("2026-01-01T00:00:00"), True),
    ]
    cols = [
        "booking_id", "user_id", "show_id", "num_seats", "total_amount",
        "booking_status", "booking_time", "updated_at", "is_deleted",
    ]
    df = spark.createDataFrame(rows, cols)

    valid, _ = tr.clean_bookings_silver(df)
    result = valid.collect()

    assert len(result) == 1
    assert result[0].booking_id == "B1"
    assert result[0].booking_status == "CANCELLED"


def test_build_fact_bookings_computes_revenue_and_zeroes_cancelled(spark):
    bookings = spark.createDataFrame(
        [
            ("B1", "U1", "S1", 2, 300.0, "CONFIRMED", _ts("2026-01-05T10:00:00")),
            ("B2", "U2", "S1", 1, 150.0, "CANCELLED", _ts("2026-01-05T11:00:00")),
        ],
        ["booking_id", "user_id", "show_id", "num_seats", "total_amount", "booking_status", "booking_time"],
    )
    dim_show = spark.createDataFrame(
        [("S1", "M1", "V1", _ts("2026-01-06T18:00:00"), 150.0, True)],
        ["show_id", "movie_id", "venue_id", "show_time", "base_price", "is_current"],
    )
    dim_venue = spark.createDataFrame([("V1", True)], ["venue_id", "is_current"])

    fact = tr.build_fact_bookings(bookings, dim_show, dim_venue)
    rows = {r.booking_id: r for r in fact.collect()}

    assert rows["B1"].revenue == 300.0
    assert rows["B2"].revenue == 0.0
    assert rows["B1"].venue_id == "V1"


def test_build_daily_revenue_agg(spark):
    fact = spark.createDataFrame(
        [
            ("B1", "V1", _ts("2026-01-05T00:00:00").date(), "CONFIRMED", 300.0, 2),
            ("B2", "V1", _ts("2026-01-05T00:00:00").date(), "CANCELLED", 0.0, 1),
            ("B3", "V2", _ts("2026-01-05T00:00:00").date(), "CONFIRMED", 500.0, 3),
        ],
        ["booking_id", "venue_id", "booking_date", "booking_status", "revenue", "num_seats"],
    )

    agg = tr.build_daily_revenue_agg(fact).collect()
    by_venue = {r.venue_id: r for r in agg}

    assert by_venue["V1"].total_revenue == 300.0
    assert by_venue["V1"].cancelled_count == 1
    assert by_venue["V1"].total_bookings == 2
    assert by_venue["V2"].total_revenue == 500.0


def test_bootstrap_scd2_sets_current_flag(spark):
    source = spark.createDataFrame([("U1", "Alice")], ["user_id", "name"])
    result = tr.bootstrap_scd2(source, as_of="2026-01-01T00:00:00").collect()[0]

    assert result.is_current is True
    assert result.effective_end is None


def test_build_scd2_closes_old_version_on_change_and_leaves_unchanged_alone(spark):
    current = (
        spark.createDataFrame(
            [
                ("V1", "Cineplex 1", 100, "2026-01-01T00:00:00", True),
                ("V2", "Cineplex 2", 200, "2026-01-01T00:00:00", True),
            ],
            ["venue_id", "venue_name", "capacity", "effective_start", "is_current"],
        )
        .withColumn("effective_start", F.col("effective_start").cast("timestamp"))
        .withColumn("effective_end", F.lit(None).cast("timestamp"))
    )
    new_source = spark.createDataFrame(
        [("V1", "Cineplex 1", 160), ("V2", "Cineplex 2", 200)],
        ["venue_id", "venue_name", "capacity"],
    )

    result = tr.build_scd2(
        current, new_source, key_col="venue_id", tracked_cols=["capacity"], as_of="2026-02-01T00:00:00"
    ).collect()
    by_key = {}
    for r in result:
        by_key.setdefault(r.venue_id, []).append(r)

    assert len(by_key["V2"]) == 1
    assert by_key["V2"][0].is_current is True

    v1_versions = sorted(by_key["V1"], key=lambda r: r.effective_start)
    assert len(v1_versions) == 2
    assert v1_versions[0].capacity == 100
    assert v1_versions[0].is_current is False
    assert v1_versions[0].effective_end is not None
    assert v1_versions[1].capacity == 160
    assert v1_versions[1].is_current is True
    assert v1_versions[1].effective_end is None


def test_build_fact_payments_enriches_with_booking_context(spark):
    silver_payments = spark.createDataFrame(
        [("P1", "B1", 300.0, "SUCCESS"), ("P2", "B2", 150.0, "REFUNDED")],
        ["payment_id", "booking_id", "amount", "payment_status"],
    )
    fact_bookings = spark.createDataFrame(
        [("B1", "V1", "M1", "CONFIRMED"), ("B2", "V2", "M2", "CANCELLED")],
        ["booking_id", "venue_id", "movie_id", "booking_status"],
    )

    result = {r.payment_id: r for r in tr.build_fact_payments(silver_payments, fact_bookings).collect()}

    assert result["P1"].venue_id == "V1"
    assert result["P1"].movie_id == "M1"
    assert result["P2"].booking_status_at_fact_time == "CANCELLED"
