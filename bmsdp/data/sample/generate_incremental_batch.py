"""Generates a second landing-zone batch to simulate "day 2" CDC activity:
new bookings, a status change on existing bookings (cancellation), and a
venue capacity change (renovation). Used to exercise dedup-across-batches
and the SCD2 dimension merge (build_scd2), which a single-batch run can't
test.

Requires batch1 to already exist (run generate_sample_data.py first).

Usage: python data/sample/generate_incremental_batch.py
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(99)
OUT = Path(__file__).parent / "output"


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def read_batch1(entity: str) -> list:
    path = OUT / entity / f"{entity}_batch1.json"
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def write_batch2(entity: str, records: list):
    entity_dir = OUT / entity
    entity_dir.mkdir(parents=True, exist_ok=True)
    path = entity_dir / f"{entity}_batch2.json"
    with open(path, "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in records)
    print(f"wrote {len(records)} records -> {path}")


def _parse(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")


def main():
    users = [u for u in read_batch1("users") if u["user_id"] is not None]
    shows = read_batch1("shows")
    venues = read_batch1("venues")
    bookings_b1 = read_batch1("bookings")

    # Derive "now" from batch1's own latest timestamp (+1 day) instead of the
    # wall clock, so batch2 is guaranteed to be newer than batch1 regardless
    # of any clock drift between when each script happens to run.
    latest_seen = max(
        _parse(r["updated_at"])
        for r in bookings_b1 + venues + users
        if r.get("updated_at")
    )
    now = latest_seen + timedelta(days=1)

    # 1. Venue renovation: capacity change on one venue (drives SCD2 new version)
    changed_venue = dict(venues[0])
    old_capacity = changed_venue["capacity"]
    changed_venue["capacity"] = old_capacity + 60
    changed_venue["updated_at"] = iso(now)
    print(f"venue {changed_venue['venue_id']} capacity: {old_capacity} -> {changed_venue['capacity']}")
    write_batch2("venues", [changed_venue])

    # 2. Status change: cancel a few previously-confirmed bookings (later updated_at wins)
    confirmed = [b for b in bookings_b1 if b["booking_status"] == "CONFIRMED"]
    to_cancel = random.sample(confirmed, min(10, len(confirmed)))
    status_updates = []
    for b in to_cancel:
        updated = dict(b)
        updated["booking_status"] = "CANCELLED"
        updated["updated_at"] = iso(now)
        status_updates.append(updated)

    # 3. New bookings placed since batch1
    new_bookings = []
    for i in range(150):
        user = random.choice(users)
        show = random.choice(shows)
        num_seats = random.randint(1, 6)
        total = round(num_seats * show["base_price"], 2)
        status = random.choices(["CONFIRMED", "CANCELLED", "PENDING"], weights=[0.8, 0.15, 0.05])[0]
        booking_time = now - timedelta(hours=random.randint(0, 24))
        new_bookings.append(
            {
                "booking_id": f"B2{i:06d}",
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

    write_batch2("bookings", status_updates + new_bookings)

    new_payments = []
    for i, b in enumerate(new_bookings):
        if b["booking_status"] not in ("CONFIRMED", "CANCELLED"):
            continue
        new_payments.append(
            {
                "payment_id": f"P2{i:06d}",
                "booking_id": b["booking_id"],
                "amount": b["total_amount"],
                "payment_method": random.choice(["UPI", "CARD", "NETBANKING", "WALLET"]),
                "payment_status": "SUCCESS" if b["booking_status"] == "CONFIRMED" else "REFUNDED",
                "payment_time": b["booking_time"],
                "updated_at": b["updated_at"],
            }
        )
    write_batch2("payments", new_payments)


if __name__ == "__main__":
    main()
