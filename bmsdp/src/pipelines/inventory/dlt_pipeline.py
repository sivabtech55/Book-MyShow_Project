"""DLT pipeline definition for the Inventory & Seating domain. Thin wrapper
only - real logic lives in transformations.py (see bookings/dlt_pipeline.py
for why). Depends on dim_show/dim_venue published by the bookings pipeline,
so this pipeline's resource config should list bookings_pipeline as a
dependency when both are deployed.
"""

import dlt

from src.pipelines.inventory import transformations as tr
from src.utils import schemas as sc
from src.utils.spark_helpers import add_ingestion_metadata

landing_root = spark.conf.get("bmsdp.landing_root")  # noqa: F821


@dlt.table(name="bronze_seat_holds", comment="Raw seat-lock/hold events, as landed.")
def bronze_seat_holds():
    raw = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .schema(sc.SEAT_HOLDS_SCHEMA)
        .load(f"{landing_root}/seat_holds")
    )
    return add_ingestion_metadata(raw, "seat_lock_service")


@dlt.table(name="silver_seat_holds")
@dlt.expect_or_drop("valid_show_and_seat", "show_id IS NOT NULL AND seat_no IS NOT NULL")
def silver_seat_holds():
    valid, _quarantined = tr.clean_seat_holds_silver(dlt.read("bronze_seat_holds"))
    return valid


@dlt.table(name="quarantine_seat_holds")
def quarantine_seat_holds():
    _valid, quarantined = tr.clean_seat_holds_silver(dlt.read("bronze_seat_holds"))
    return quarantined


@dlt.table(name="fact_seat_occupancy", comment="Per-show seat occupancy for real-time inventory reporting.")
def fact_seat_occupancy():
    dim_venue = dlt.read("bmsdp_bookings.dim_venue")
    dim_show = dlt.read("bmsdp_bookings.dim_show")
    return tr.build_fact_seat_occupancy(dlt.read("silver_seat_holds"), dim_venue, dim_show)
