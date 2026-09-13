"""DLT pipeline definition for the Pricing & Promotions domain. Thin wrapper
only - real logic lives in transformations.py. Depends on fact_bookings from
the bookings pipeline for net-revenue calculations.
"""

import dlt

from src.pipelines.pricing import transformations as tr
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


@dlt.table(name="bronze_promotions")
def bronze_promotions():
    raw = _autoload(f"{landing_root}/promotions", sc.PROMOTIONS_SCHEMA)
    return add_ingestion_metadata(raw, "promotions_engine")


@dlt.table(name="bronze_promotion_redemptions")
def bronze_promotion_redemptions():
    raw = _autoload(f"{landing_root}/promotion_redemptions", sc.PROMOTION_REDEMPTIONS_SCHEMA)
    return add_ingestion_metadata(raw, "promotions_engine")


@dlt.table(name="silver_promotions")
def silver_promotions():
    valid, _quarantined = tr.clean_promotions_silver(dlt.read("bronze_promotions"))
    return valid


@dlt.table(name="quarantine_promotions")
def quarantine_promotions():
    _valid, quarantined = tr.clean_promotions_silver(dlt.read("bronze_promotions"))
    return quarantined


@dlt.table(name="silver_promotion_redemptions")
@dlt.expect_or_drop("valid_refs", "promotion_id IS NOT NULL AND booking_id IS NOT NULL")
def silver_promotion_redemptions():
    return tr.clean_redemptions_silver(dlt.read("bronze_promotion_redemptions"))


@dlt.table(name="fact_promotion_redemptions")
def fact_promotion_redemptions():
    fact_bookings = dlt.read("bmsdp_bookings.fact_bookings")
    return tr.build_fact_promotion_redemptions(
        dlt.read("silver_promotion_redemptions"), dlt.read("silver_promotions"), fact_bookings
    )


@dlt.table(name="gold_promotion_performance")
def gold_promotion_performance():
    return tr.build_promotion_performance_agg(dlt.read("fact_promotion_redemptions"))
