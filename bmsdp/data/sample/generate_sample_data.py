"""Generates synthetic landing-zone JSON files for the Bookings domain.

Run locally to produce files under data/sample/output/{entity}/*.json that
mimic what Auto Loader would see in the bronze landing zone. Intentionally
includes a few dirty/duplicate/late CDC records so the silver DQ rules and
dedup logic have something real to catch.

Usage: python data/sample/generate_sample_data.py
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(__file__).parent / "output"

CITIES = ["Mumbai", "Bengaluru", "Delhi", "Hyderabad", "Chennai", "Pune"]
GENRES = ["Action", "Drama", "Comedy", "Thriller", "Romance"]
LANGS = ["Hindi", "English", "Telugu", "Tamil", "Kannada"]
PAYMENT_METHODS = ["UPI", "CARD", "NETBANKING", "WALLET"]


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def write(entity: str, records: list):
    entity_dir = OUT / entity
    entity_dir.mkdir(parents=True, exist_ok=True)
    path = entity_dir / f"{entity}_batch1.json"
    with open(path, "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in records)
    print(f"wrote {len(records)} records -> {path}")


def gen_users(n=200):
    now = datetime.now()
    users = []
    for i in range(n):
        uid = f"U{i:05d}"
        signup = now - timedelta(days=random.randint(30, 900))
        users.append(
            {
                "user_id": uid,
                "name": f"User {i}",
                "email": f"user{i}@example.com",
                "phone": f"9{random.randint(100000000, 999999999)}",
                "city": random.choice(CITIES),
                "signup_date": iso(signup),
                "updated_at": iso(now - timedelta(days=random.randint(0, 5))),
            }
        )
    # a couple of null-key dirty records to exercise DQ filtering
    users.append(
        {
            "user_id": None,
            "name": "Bad Record",
            "email": None,
            "phone": None,
            "city": None,
            "signup_date": None,
            "updated_at": iso(now),
        }
    )
    return users


def gen_venues(n=15):
    now = datetime.now()
    venues = []
    for i in range(n):
        venues.append(
            {
                "venue_id": f"V{i:03d}",
                "venue_name": f"Cineplex {i}",
                "city": random.choice(CITIES),
                "state": "NA",
                "capacity": random.choice([120, 180, 240, 300]),
                "updated_at": iso(now),
            }
        )
    return venues


def gen_movies(n=10):
    now = datetime.now()
    movies = []
    for i in range(n):
        movies.append(
            {
                "movie_id": f"M{i:03d}",
                "title": f"Movie Title {i}",
                "genre": random.choice(GENRES),
                "language": random.choice(LANGS),
                "duration_min": random.randint(100, 170),
                "release_date": iso(now - timedelta(days=random.randint(1, 60))),
                "updated_at": iso(now),
            }
        )
    return movies


def gen_shows(movies, venues, n=60):
    now = datetime.now()
    shows = []
    for i in range(n):
        movie = random.choice(movies)
        venue = random.choice(venues)
        show_time = now + timedelta(days=random.randint(0, 14), hours=random.randint(0, 23))
        shows.append(
            {
                "show_id": f"S{i:05d}",
                "movie_id": movie["movie_id"],
                "venue_id": venue["venue_id"],
                "screen_no": random.randint(1, 8),
                "show_time": iso(show_time),
                "base_price": float(random.choice([150, 200, 250, 350, 500])),
                "updated_at": iso(now),
            }
        )
    return shows


def gen_bookings(users, shows, n=1000):
    now = datetime.now()
    bookings = []
    for i in range(n):
        user = random.choice(users)
        if user["user_id"] is None:
            continue
        show = random.choice(shows)
        num_seats = random.randint(1, 6)
        total = round(num_seats * show["base_price"], 2)
        status = random.choices(
            ["CONFIRMED", "CANCELLED", "PENDING"], weights=[0.8, 0.15, 0.05]
        )[0]
        booking_time = now - timedelta(hours=random.randint(0, 720))
        bookings.append(
            {
                "booking_id": f"B{i:06d}",
                "user_id": user["user_id"],
                "show_id": show["show_id"],
                "num_seats": num_seats,
                "total_amount": total,
                "booking_status": status,
                "booking_time": iso(booking_time),
                "updated_at": iso(booking_time),
                "is_deleted": False,
            }
        )

    # duplicate CDC updates for a few bookings (later updated_at should win)
    for b in random.sample(bookings, 20):
        updated = dict(b)
        updated["booking_status"] = "CANCELLED"
        updated["updated_at"] = iso(datetime.now())
        bookings.append(updated)

    # dirty records: bad amount, missing keys, unknown status
    bookings.append(
        {
            "booking_id": "B999901",
            "user_id": users[0]["user_id"],
            "show_id": shows[0]["show_id"],
            "num_seats": 2,
            "total_amount": -50.0,
            "booking_status": "CONFIRMED",
            "booking_time": iso(now),
            "updated_at": iso(now),
            "is_deleted": False,
        }
    )
    bookings.append(
        {
            "booking_id": "B999902",
            "user_id": None,
            "show_id": shows[0]["show_id"],
            "num_seats": 1,
            "total_amount": 100.0,
            "booking_status": "UNKNOWN_STATUS",
            "booking_time": iso(now),
            "updated_at": iso(now),
            "is_deleted": False,
        }
    )
    return bookings


def gen_payments(bookings):
    payments = []
    confirmed = [b for b in bookings if b["booking_status"] in ("CONFIRMED", "CANCELLED")]
    for i, b in enumerate(confirmed):
        payments.append(
            {
                "payment_id": f"P{i:06d}",
                "booking_id": b["booking_id"],
                "amount": b["total_amount"],
                "payment_method": random.choice(PAYMENT_METHODS),
                "payment_status": "SUCCESS" if b["booking_status"] == "CONFIRMED" else "REFUNDED",
                "payment_time": b["booking_time"],
                "updated_at": b["updated_at"],
            }
        )
    return payments


def gen_seat_holds(shows, venues):
    """One state row per occupied/attempted seat per show (not every empty
    seat - those simply have no row, and count as available by omission).
    """
    now = datetime.now()
    venue_capacity = {v["venue_id"]: v["capacity"] for v in venues}
    holds = []
    hold_i = 0
    for show in shows:
        capacity = venue_capacity.get(show["venue_id"], 120)
        activity_count = random.randint(0, min(capacity, 250))
        seat_numbers = random.sample(range(1, capacity + 1), activity_count)
        for seat_no in seat_numbers:
            status = random.choices(
                ["BOOKED", "HELD", "RELEASED", "EXPIRED"], weights=[0.5, 0.05, 0.15, 0.3]
            )[0]
            held_at = now - timedelta(hours=random.randint(0, 48))
            holds.append(
                {
                    "hold_id": f"H{hold_i:07d}",
                    "show_id": show["show_id"],
                    "seat_no": f"S{seat_no}",
                    "user_id": None,
                    "hold_status": status,
                    "held_at": iso(held_at),
                    "updated_at": iso(held_at),
                }
            )
            hold_i += 1
    return holds


def gen_promotions(n=8):
    now = datetime.now()
    promos = []
    for i in range(n):
        discount_type = random.choice(["PERCENT", "FLAT"])
        discount_value = float(random.choice([10, 15, 20, 25])) if discount_type == "PERCENT" else float(
            random.choice([50, 100, 150])
        )
        promos.append(
            {
                "promotion_id": f"PROMO{i:03d}",
                "code": f"SAVE{i:03d}",
                "discount_type": discount_type,
                "discount_value": discount_value,
                "valid_from": iso(now - timedelta(days=30)),
                "valid_to": iso(now + timedelta(days=30)),
                "status": "ACTIVE",
                "updated_at": iso(now),
            }
        )
    # one dirty record: invalid discount type
    promos.append(
        {
            "promotion_id": "PROMOBAD",
            "code": "BADCODE",
            "discount_type": "MYSTERY",
            "discount_value": 10.0,
            "valid_from": iso(now),
            "valid_to": iso(now),
            "status": "ACTIVE",
            "updated_at": iso(now),
        }
    )
    return promos


def gen_promotion_redemptions(bookings, promotions, fraction=0.15):
    now = datetime.now()
    confirmed = [b for b in bookings if b["booking_status"] == "CONFIRMED"]
    sample_size = int(len(confirmed) * fraction)
    redeemed_bookings = random.sample(confirmed, min(sample_size, len(confirmed)))
    active_promos = [p for p in promotions if p["status"] == "ACTIVE" and p["discount_type"] in ("PERCENT", "FLAT")]

    redemptions = []
    for i, b in enumerate(redeemed_bookings):
        promo = random.choice(active_promos)
        if promo["discount_type"] == "PERCENT":
            discount = round(b["total_amount"] * promo["discount_value"] / 100, 2)
        else:
            discount = min(promo["discount_value"], b["total_amount"])
        redemptions.append(
            {
                "redemption_id": f"R{i:06d}",
                "promotion_id": promo["promotion_id"],
                "booking_id": b["booking_id"],
                "discount_amount": discount,
                "redeemed_at": b["booking_time"],
                "updated_at": iso(now),
            }
        )
    return redemptions


def main():
    users = gen_users()
    venues = gen_venues()
    movies = gen_movies()
    shows = gen_shows(movies, venues)
    bookings = gen_bookings(users, shows)
    payments = gen_payments(bookings)
    seat_holds = gen_seat_holds(shows, venues)
    promotions = gen_promotions()
    promotion_redemptions = gen_promotion_redemptions(bookings, promotions)

    write("users", users)
    write("venues", venues)
    write("movies", movies)
    write("shows", shows)
    write("bookings", bookings)
    write("payments", payments)
    write("seat_holds", seat_holds)
    write("promotions", promotions)
    write("promotion_redemptions", promotion_redemptions)


if __name__ == "__main__":
    main()
