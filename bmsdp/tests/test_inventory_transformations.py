from datetime import datetime

from src.pipelines.inventory import transformations as tr


def _ts(s):
    return datetime.fromisoformat(s)


def test_clean_seat_holds_silver_dedups_and_quarantines(spark):
    rows = [
        ("H1", "S1", "A1", "HELD", _ts("2026-01-01T00:00:00")),
        ("H2", "S1", "A1", "BOOKED", _ts("2026-01-01T01:00:00")),  # later state wins
        ("H3", None, "A2", "HELD", _ts("2026-01-01T00:00:00")),  # missing show_id
        ("H4", "S1", "A3", "UNKNOWN", _ts("2026-01-01T00:00:00")),  # bad status
    ]
    df = spark.createDataFrame(rows, ["hold_id", "show_id", "seat_no", "hold_status", "updated_at"])

    valid, quarantined = tr.clean_seat_holds_silver(df)
    result = {r.seat_no: r.hold_status for r in valid.collect()}

    assert result == {"A1": "BOOKED"}
    assert quarantined.count() == 2


def test_build_fact_seat_occupancy_computes_percentage(spark):
    silver_seat_holds = spark.createDataFrame(
        [
            ("S1", "A1", "BOOKED"),
            ("S1", "A2", "HELD"),
            ("S1", "A3", "RELEASED"),  # not occupied
            ("S2", "A1", "BOOKED"),
        ],
        ["show_id", "seat_no", "hold_status"],
    )
    dim_show = spark.createDataFrame(
        [("S1", "V1", _ts("2026-01-05T18:00:00"), True), ("S2", "V1", _ts("2026-01-06T18:00:00"), True)],
        ["show_id", "venue_id", "show_time", "is_current"],
    )
    dim_venue = spark.createDataFrame([("V1", 10, True)], ["venue_id", "capacity", "is_current"])

    result = {r.show_id: r for r in tr.build_fact_seat_occupancy(silver_seat_holds, dim_venue, dim_show).collect()}

    assert result["S1"].occupied_seats == 2
    assert result["S1"].occupancy_pct == 20.0
    assert result["S2"].occupied_seats == 1
    assert result["S2"].occupancy_pct == 10.0
