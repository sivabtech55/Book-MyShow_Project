"""DLT pipeline definition for the Users 360 gold domain. Pure gold-layer
aggregation on top of the bookings pipeline's fact_bookings/dim_user - no
bronze/silver of its own.
"""

import dlt

from src.pipelines.users import transformations as tr


@dlt.table(name="gold_user_booking_summary", comment="Per-user booking behavior for personalization/churn models.")
def gold_user_booking_summary():
    fact_bookings = dlt.read("bmsdp_bookings.fact_bookings")
    dim_user = dlt.read("bmsdp_bookings.dim_user")
    return tr.build_user_booking_summary(fact_bookings, dim_user)
