"""Explicit schemas for the Bookings domain (bronze source contracts).

Kept centralized so Auto Loader / Structured Streaming readers and unit
tests share one definition instead of relying on schema inference.
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

USERS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("signup_date", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

VENUES_SCHEMA = StructType(
    [
        StructField("venue_id", StringType(), nullable=False),
        StructField("venue_name", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("capacity", IntegerType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

MOVIES_SCHEMA = StructType(
    [
        StructField("movie_id", StringType(), nullable=False),
        StructField("title", StringType(), nullable=True),
        StructField("genre", StringType(), nullable=True),
        StructField("language", StringType(), nullable=True),
        StructField("duration_min", IntegerType(), nullable=True),
        StructField("release_date", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

SHOWS_SCHEMA = StructType(
    [
        StructField("show_id", StringType(), nullable=False),
        StructField("movie_id", StringType(), nullable=False),
        StructField("venue_id", StringType(), nullable=False),
        StructField("screen_no", IntegerType(), nullable=True),
        StructField("show_time", TimestampType(), nullable=False),
        StructField("base_price", DoubleType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

BOOKINGS_SCHEMA = StructType(
    [
        StructField("booking_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("show_id", StringType(), nullable=False),
        StructField("num_seats", IntegerType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("booking_status", StringType(), nullable=True),
        StructField("booking_time", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
        StructField("is_deleted", BooleanType(), nullable=True),
    ]
)

PAYMENTS_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), nullable=False),
        StructField("booking_id", StringType(), nullable=False),
        StructField("amount", DoubleType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("payment_status", StringType(), nullable=True),
        StructField("payment_time", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

# Seat-lock/hold service events (Inventory & Seating domain). One row per
# (show_id, seat_no) state change - HELD when a user starts checkout,
# BOOKED on payment success, RELEASED/EXPIRED otherwise.
SEAT_HOLDS_SCHEMA = StructType(
    [
        StructField("hold_id", StringType(), nullable=False),
        StructField("show_id", StringType(), nullable=False),
        StructField("seat_no", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=True),
        StructField("hold_status", StringType(), nullable=True),
        StructField("held_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

# Pricing & Promotions domain: coupon/offer definitions from the promotions engine.
PROMOTIONS_SCHEMA = StructType(
    [
        StructField("promotion_id", StringType(), nullable=False),
        StructField("code", StringType(), nullable=True),
        StructField("discount_type", StringType(), nullable=True),
        StructField("discount_value", DoubleType(), nullable=True),
        StructField("valid_from", TimestampType(), nullable=True),
        StructField("valid_to", TimestampType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

# One row per booking that redeemed a promotion.
PROMOTION_REDEMPTIONS_SCHEMA = StructType(
    [
        StructField("redemption_id", StringType(), nullable=False),
        StructField("promotion_id", StringType(), nullable=False),
        StructField("booking_id", StringType(), nullable=False),
        StructField("discount_amount", DoubleType(), nullable=True),
        StructField("redeemed_at", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)
