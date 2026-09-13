"""Delta Live Tables pipeline definition for the Bookings domain.

This module only runs inside a Databricks DLT pipeline (the `dlt` module is
injected by the DLT runtime) - it is intentionally a thin wrapper: all real
logic lives in transformations.py so it can be unit tested locally without
a Databricks cluster. Wired up as a pipeline via
resources/pipelines/bookings_pipeline.yml.

Bronze sources are read with Auto Loader from the landing zone paths passed
in as pipeline configuration (see the YAML resource) so dev/stage/prod can
point at different storage locations without code changes.
"""

import dlt

from src.pipelines.bookings import transformations as tr
from src.utils import schemas as sc
from src.utils.spark_helpers import add_ingestion_metadata

landing_root = spark.conf.get("bmsdp.landing_root")  # noqa: F821


def _autoload(path: str, schema):
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .schema(schema)
        .load(path)
    )


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------


@dlt.table(name="bronze_bookings", comment="Raw booking CDC events, as landed.")
def bronze_bookings():
    raw = _autoload(f"{landing_root}/bookings", sc.BOOKINGS_SCHEMA)
    return add_ingestion_metadata(raw, "booking_oltp_cdc")


@dlt.table(name="bronze_payments", comment="Raw payment gateway events, as landed.")
def bronze_payments():
    raw = _autoload(f"{landing_root}/payments", sc.PAYMENTS_SCHEMA)
    return add_ingestion_metadata(raw, "payment_gateway")


@dlt.table(name="bronze_users")
def bronze_users():
    raw = _autoload(f"{landing_root}/users", sc.USERS_SCHEMA)
    return add_ingestion_metadata(raw, "user_service")


@dlt.table(name="bronze_venues")
def bronze_venues():
    raw = _autoload(f"{landing_root}/venues", sc.VENUES_SCHEMA)
    return add_ingestion_metadata(raw, "venue_service")


@dlt.table(name="bronze_movies")
def bronze_movies():
    raw = _autoload(f"{landing_root}/movies", sc.MOVIES_SCHEMA)
    return add_ingestion_metadata(raw, "catalog_service")


@dlt.table(name="bronze_shows")
def bronze_shows():
    raw = _autoload(f"{landing_root}/shows", sc.SHOWS_SCHEMA)
    return add_ingestion_metadata(raw, "show_scheduling_service")


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------
@dlt.table(name="silver_bookings")
@dlt.expect_all_or_drop(
    {
        "valid_booking_id": "booking_id IS NOT NULL",
        "valid_amount": "total_amount >= 0",
    }
)
def silver_bookings():
    bronze = dlt.read("bronze_bookings")
    valid, _quarantined = tr.clean_bookings_silver(bronze)
    return valid


@dlt.table(name="quarantine_bookings", comment="Bookings failing DQ rules, for triage.")
def quarantine_bookings():
    bronze = dlt.read("bronze_bookings")
    _valid, quarantined = tr.clean_bookings_silver(bronze)
    return quarantined


@dlt.table(name="silver_payments")
@dlt.expect_or_drop("valid_booking_ref", "booking_id IS NOT NULL")
def silver_payments():
    return tr.clean_payments_silver(dlt.read("bronze_payments"))


@dlt.table(name="silver_users")
def silver_users():
    return tr.clean_dimension_silver(dlt.read("bronze_users"), "user_id")


@dlt.table(name="silver_venues")
def silver_venues():
    return tr.clean_dimension_silver(dlt.read("bronze_venues"), "venue_id")


@dlt.table(name="silver_movies")
def silver_movies():
    return tr.clean_dimension_silver(dlt.read("bronze_movies"), "movie_id")


@dlt.table(name="silver_shows")
def silver_shows():
    return tr.clean_dimension_silver(dlt.read("bronze_shows"), "show_id")


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------
@dlt.table(name="dim_show", comment="SCD2 dimension for shows (price/schedule changes tracked).")
def dim_show():
    return tr.bootstrap_scd2(dlt.read("silver_shows"))


@dlt.table(name="dim_venue")
def dim_venue():
    return tr.bootstrap_scd2(dlt.read("silver_venues"))


@dlt.table(name="dim_user")
def dim_user():
    return tr.bootstrap_scd2(dlt.read("silver_users"))


@dlt.table(name="fact_bookings", comment="Booking-grain fact table for revenue/occupancy reporting.")
def fact_bookings():
    return tr.build_fact_bookings(
        dlt.read("silver_bookings"), dlt.read("dim_show"), dlt.read("dim_venue")
    )


@dlt.table(name="fact_payments", comment="Payment-grain fact table enriched with venue/movie context.")
def fact_payments():
    return tr.build_fact_payments(dlt.read("silver_payments"), dlt.read("fact_bookings"))


@dlt.table(name="gold_daily_revenue")
def gold_daily_revenue():
    return tr.build_daily_revenue_agg(dlt.read("fact_bookings"))
