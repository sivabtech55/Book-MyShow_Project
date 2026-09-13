from datetime import date, datetime

from src.pipelines.users import transformations as tr


def _ts(s):
    return datetime.fromisoformat(s)


def test_build_user_booking_summary_aggregates_correctly(spark):
    fact_bookings = spark.createDataFrame(
        [
            ("U1", "B1", "CONFIRMED", 300.0, date(2026, 1, 1), 2),
            ("U1", "B2", "CANCELLED", 0.0, date(2026, 1, 5), 1),
            ("U1", "B3", "CONFIRMED", 200.0, date(2026, 1, 10), 3),
            ("U2", "B4", "CONFIRMED", 500.0, date(2026, 1, 1), 4),
        ],
        ["user_id", "booking_id", "booking_status", "revenue", "booking_date", "num_seats"],
    )
    dim_user = spark.createDataFrame(
        [
            ("U1", "Mumbai", _ts("2025-01-01T00:00:00"), True),
            ("U2", "Delhi", _ts("2025-01-01T00:00:00"), True),
            ("U3", "Pune", _ts("2025-01-01T00:00:00"), True),  # never booked
        ],
        ["user_id", "city", "signup_date", "is_current"],
    )

    result = {r.user_id: r for r in tr.build_user_booking_summary(fact_bookings, dim_user).collect()}

    assert result["U1"].total_bookings == 3
    assert result["U1"].lifetime_spend == 500.0
    assert result["U1"].cancelled_bookings == 1
    assert result["U1"].cancellation_rate == round(1 / 3, 4)
    assert result["U1"].first_booking_date == date(2026, 1, 1)
    assert result["U1"].last_booking_date == date(2026, 1, 10)

    # user with no bookings at all should still appear, with zeroed-out metrics
    assert result["U3"].total_bookings == 0
    assert result["U3"].lifetime_spend == 0.0
    assert result["U3"].cancellation_rate == 0.0
