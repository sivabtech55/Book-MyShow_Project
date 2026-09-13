"""Local end-to-end smoke run of the Bookings bronze->silver->gold logic
against the generated synthetic data, using batch reads (no Auto Loader/DLT,
which only exist inside Databricks). Same transformation functions the DLT
pipeline calls in production, so a pass here is a strong signal the pipeline
logic itself is correct before deploying to Databricks.

Usage: python scripts/run_local_e2e.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession

from src.pipelines.bookings import transformations as tr
from src.utils import schemas as sc
from src.utils.spark_helpers import add_ingestion_metadata, pin_pyspark_worker_python

DATA_DIR = Path(__file__).parent.parent / "data" / "sample" / "output"


def read_json(spark, entity, schema):
    path = str(DATA_DIR / entity / "*.json")
    return spark.read.schema(schema).json(path)


def main():
    pin_pyspark_worker_python()
    spark = SparkSession.builder.master("local[2]").appName("bmsdp-e2e").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bronze_users = add_ingestion_metadata(read_json(spark, "users", sc.USERS_SCHEMA), "user_service")
    bronze_venues = add_ingestion_metadata(read_json(spark, "venues", sc.VENUES_SCHEMA), "venue_service")
    bronze_movies = add_ingestion_metadata(read_json(spark, "movies", sc.MOVIES_SCHEMA), "catalog_service")
    bronze_shows = add_ingestion_metadata(read_json(spark, "shows", sc.SHOWS_SCHEMA), "show_service")
    bronze_bookings = add_ingestion_metadata(read_json(spark, "bookings", sc.BOOKINGS_SCHEMA), "booking_oltp_cdc")
    bronze_payments = add_ingestion_metadata(read_json(spark, "payments", sc.PAYMENTS_SCHEMA), "payment_gateway")

    print(f"bronze counts: users={bronze_users.count()} venues={bronze_venues.count()} "
          f"movies={bronze_movies.count()} shows={bronze_shows.count()} "
          f"bookings={bronze_bookings.count()} payments={bronze_payments.count()}")

    silver_users = tr.clean_dimension_silver(bronze_users, "user_id")
    silver_venues = tr.clean_dimension_silver(bronze_venues, "venue_id")
    silver_shows = tr.clean_dimension_silver(bronze_shows, "show_id")
    silver_bookings, quarantined_bookings = tr.clean_bookings_silver(bronze_bookings)
    silver_payments = tr.clean_payments_silver(bronze_payments)

    print(f"silver counts: users={silver_users.count()} venues={silver_venues.count()} "
          f"shows={silver_shows.count()} bookings={silver_bookings.count()} "
          f"payments={silver_payments.count()} quarantined_bookings={quarantined_bookings.count()}")

    print("sample quarantined bookings (should show negative amount / bad status / missing user):")
    quarantined_bookings.select(
        "booking_id", "user_id", "total_amount", "booking_status"
    ).show(10, truncate=False)

    dim_show = tr.bootstrap_scd2(silver_shows)
    dim_venue = tr.bootstrap_scd2(silver_venues)
    dim_user = tr.bootstrap_scd2(silver_users)

    fact_bookings = tr.build_fact_bookings(silver_bookings, dim_show, dim_venue)
    gold_daily_revenue = tr.build_daily_revenue_agg(fact_bookings)

    print(f"gold counts: fact_bookings={fact_bookings.count()} "
          f"daily_revenue_rows={gold_daily_revenue.count()} "
          f"dim_user={dim_user.count()} dim_venue={dim_venue.count()} dim_show={dim_show.count()}")

    print("sample fact_bookings:")
    fact_bookings.select(
        "booking_id", "venue_id", "movie_id", "booking_status", "num_seats", "revenue"
    ).show(10, truncate=False)

    print("top venues by revenue:")
    gold_daily_revenue.orderBy(gold_daily_revenue.total_revenue.desc()).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
