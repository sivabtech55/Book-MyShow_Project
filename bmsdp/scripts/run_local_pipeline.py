"""Runs the Bookings bronze->silver->gold pipeline against real Delta Lake
tables on local disk, incrementally: each run reads whatever landing-zone
batch files exist (batch1, batch2, ...), rebuilds bronze/silver from all of
them, and SCD2-merges the gold dimensions against whatever was persisted by
the previous run. This is the closest local stand-in for what the DLT
pipeline does continuously in Databricks.

Usage:
    python data/sample/generate_sample_data.py          # batch 1 (first run only)
    python scripts/run_local_pipeline.py                # run 1: bootstrap
    python data/sample/generate_incremental_batch.py     # batch 2
    python scripts/run_local_pipeline.py                # run 2: incremental merge
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipelines.bookings import transformations as tr
from src.pipelines.inventory import transformations as inv_tr
from src.pipelines.pricing import transformations as price_tr
from src.pipelines.users import transformations as user_tr
from src.utils import schemas as sc
from src.utils.local_spark import get_local_delta_spark
from src.utils.spark_helpers import add_ingestion_metadata

ROOT = Path(__file__).parent.parent
LANDING_DIR = ROOT / "data" / "sample" / "output"
LAKEHOUSE_DIR = ROOT / "data" / "local_lakehouse"


def read_all_batches(spark, entity, schema):
    path = str(LANDING_DIR / entity / "*.json")
    return spark.read.schema(schema).json(path)


def delta_path(layer, table):
    return str(LAKEHOUSE_DIR / layer / table)


def write_delta(df, layer, table):
    df.write.format("delta").mode("overwrite").save(delta_path(layer, table))


def read_delta_if_exists(spark, layer, table):
    path = Path(delta_path(layer, table))
    if not (path / "_delta_log").exists():
        return None
    return spark.read.format("delta").load(str(path))


def upsert_scd2_dim(spark, table_name, silver_df, key_col, tracked_cols):
    current = read_delta_if_exists(spark, "gold", table_name)
    if current is None:
        print(f"[{table_name}] no prior state found -> bootstrapping")
        result = tr.bootstrap_scd2(silver_df)
    else:
        print(f"[{table_name}] prior state found ({current.count()} rows) -> SCD2 merge")
        result = tr.build_scd2(current, silver_df, key_col, tracked_cols)
    write_delta(result, "gold", table_name)
    return result


def main():
    spark = get_local_delta_spark("bmsdp-local-pipeline")

    # ---- Bronze: reread all landed batches, add ingestion metadata ----
    bronze = {
        "users": add_ingestion_metadata(read_all_batches(spark, "users", sc.USERS_SCHEMA), "user_service"),
        "venues": add_ingestion_metadata(read_all_batches(spark, "venues", sc.VENUES_SCHEMA), "venue_service"),
        "movies": add_ingestion_metadata(read_all_batches(spark, "movies", sc.MOVIES_SCHEMA), "catalog_service"),
        "shows": add_ingestion_metadata(read_all_batches(spark, "shows", sc.SHOWS_SCHEMA), "show_service"),
        "bookings": add_ingestion_metadata(
            read_all_batches(spark, "bookings", sc.BOOKINGS_SCHEMA), "booking_oltp_cdc"
        ),
        "payments": add_ingestion_metadata(
            read_all_batches(spark, "payments", sc.PAYMENTS_SCHEMA), "payment_gateway"
        ),
        "seat_holds": add_ingestion_metadata(
            read_all_batches(spark, "seat_holds", sc.SEAT_HOLDS_SCHEMA), "seat_lock_service"
        ),
        "promotions": add_ingestion_metadata(
            read_all_batches(spark, "promotions", sc.PROMOTIONS_SCHEMA), "promotions_engine"
        ),
        "promotion_redemptions": add_ingestion_metadata(
            read_all_batches(spark, "promotion_redemptions", sc.PROMOTION_REDEMPTIONS_SCHEMA),
            "promotions_engine",
        ),
    }
    for name, df in bronze.items():
        write_delta(df, "bronze", name)
    print("bronze counts:", {k: v.count() for k, v in bronze.items()})

    # ---- Silver: cleanse/dedup ----
    silver_users = tr.clean_dimension_silver(bronze["users"], "user_id")
    silver_venues = tr.clean_dimension_silver(bronze["venues"], "venue_id")
    silver_shows = tr.clean_dimension_silver(bronze["shows"], "show_id")
    silver_bookings, quarantined_bookings = tr.clean_bookings_silver(bronze["bookings"])
    silver_payments = tr.clean_payments_silver(bronze["payments"])
    silver_seat_holds, quarantined_seat_holds = inv_tr.clean_seat_holds_silver(bronze["seat_holds"])
    silver_promotions, quarantined_promotions = price_tr.clean_promotions_silver(bronze["promotions"])
    silver_redemptions = price_tr.clean_redemptions_silver(bronze["promotion_redemptions"])

    write_delta(silver_users, "silver", "users")
    write_delta(silver_venues, "silver", "venues")
    write_delta(silver_shows, "silver", "shows")
    write_delta(silver_bookings, "silver", "bookings")
    write_delta(silver_payments, "silver", "payments")
    write_delta(quarantined_bookings, "silver", "quarantine_bookings")
    write_delta(silver_seat_holds, "silver", "seat_holds")
    write_delta(quarantined_seat_holds, "silver", "quarantine_seat_holds")
    write_delta(silver_promotions, "silver", "promotions")
    write_delta(quarantined_promotions, "silver", "quarantine_promotions")
    write_delta(silver_redemptions, "silver", "promotion_redemptions")

    print(
        "silver counts:",
        {
            "users": silver_users.count(),
            "venues": silver_venues.count(),
            "shows": silver_shows.count(),
            "bookings": silver_bookings.count(),
            "payments": silver_payments.count(),
            "quarantined_bookings": quarantined_bookings.count(),
            "seat_holds": silver_seat_holds.count(),
            "quarantined_seat_holds": quarantined_seat_holds.count(),
            "promotions": silver_promotions.count(),
            "quarantined_promotions": quarantined_promotions.count(),
            "promotion_redemptions": silver_redemptions.count(),
        },
    )

    # ---- Gold: SCD2 dimensions, merged against prior persisted state ----
    dim_venue = upsert_scd2_dim(spark, "dim_venue", silver_venues, "venue_id", ["capacity", "venue_name"])
    dim_show = upsert_scd2_dim(spark, "dim_show", silver_shows, "show_id", ["base_price", "screen_no"])
    dim_user = upsert_scd2_dim(spark, "dim_user", silver_users, "user_id", ["city", "name"])

    fact_bookings = tr.build_fact_bookings(silver_bookings, dim_show, dim_venue)
    fact_payments = tr.build_fact_payments(silver_payments, fact_bookings)
    gold_daily_revenue = tr.build_daily_revenue_agg(fact_bookings)

    fact_seat_occupancy = inv_tr.build_fact_seat_occupancy(silver_seat_holds, dim_venue, dim_show)
    fact_promotion_redemptions = price_tr.build_fact_promotion_redemptions(
        silver_redemptions, silver_promotions, fact_bookings
    )
    gold_promotion_performance = price_tr.build_promotion_performance_agg(fact_promotion_redemptions)
    gold_user_booking_summary = user_tr.build_user_booking_summary(fact_bookings, dim_user)

    write_delta(fact_bookings, "gold", "fact_bookings")
    write_delta(fact_payments, "gold", "fact_payments")
    write_delta(gold_daily_revenue, "gold", "gold_daily_revenue")
    write_delta(fact_seat_occupancy, "gold", "fact_seat_occupancy")
    write_delta(fact_promotion_redemptions, "gold", "fact_promotion_redemptions")
    write_delta(gold_promotion_performance, "gold", "gold_promotion_performance")
    write_delta(gold_user_booking_summary, "gold", "gold_user_booking_summary")

    print(
        "gold counts:",
        {
            "fact_bookings": fact_bookings.count(),
            "fact_payments": fact_payments.count(),
            "gold_daily_revenue_rows": gold_daily_revenue.count(),
            "fact_seat_occupancy": fact_seat_occupancy.count(),
            "fact_promotion_redemptions": fact_promotion_redemptions.count(),
            "gold_promotion_performance_rows": gold_promotion_performance.count(),
            "gold_user_booking_summary": gold_user_booking_summary.count(),
        },
    )

    print("\ndim_venue history for the venue that changed (if any batch2 has run):")
    dim_venue.orderBy("venue_id", "effective_start").show(20, truncate=False)

    print("\nsample fact_seat_occupancy:")
    fact_seat_occupancy.orderBy(fact_seat_occupancy.occupancy_pct.desc()).show(10, truncate=False)

    print("\ntop promotions by net revenue:")
    gold_promotion_performance.orderBy(gold_promotion_performance.total_net_revenue.desc()).show(
        10, truncate=False
    )

    print("\nsample gold_user_booking_summary:")
    gold_user_booking_summary.orderBy(gold_user_booking_summary.lifetime_spend.desc()).show(
        10, truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()
