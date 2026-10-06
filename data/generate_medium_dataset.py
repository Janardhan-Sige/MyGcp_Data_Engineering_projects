"""Generate the deterministic medium e-commerce workload.

Creates:
- 10,000 order-line records
- 500 customers
- 120 products
- 5,000 streaming events

The fixed formulas make the dataset reproducible for testing, demos and interviews.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent / "medium"
ROOT.mkdir(parents=True, exist_ok=True)

CITIES = ["Hyderabad", "Bengaluru", "Chennai", "Pune", "Mumbai", "Delhi"]
CHANNELS = ["WEB", "MOBILE_APP", "MARKETPLACE", "STORE"]
PAYMENTS = ["UPI", "CARD", "COD", "WALLET", "NETBANKING"]
STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED", "REFUNDED"]


def build_orders() -> None:
    """Create 10,000 order-line records across four files."""
    fields = [
        "order_id", "order_line_id", "customer_id", "order_ts", "product_id",
        "quantity", "unit_price", "discount_amount", "tax_amount",
        "gross_amount", "net_amount", "status", "payment_method", "channel",
        "city", "updated_at", "source_system",
    ]

    for part in range(4):
        path = ROOT / f"orders_part_{part + 1:02d}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(fields)

            for offset in range(2500):
                n = part * 2500 + offset + 1
                quantity = n % 4 + 1
                unit_price = round(19 + (n * 23 % 481) + 0.49, 2)
                gross = round(quantity * unit_price, 2)
                discount = round(gross * (0.08 if n % 5 == 0 else 0.03), 2)
                tax = round((gross - discount) * 0.18, 2)
                net = round(gross - discount + tax, 2)
                order_time = datetime(2026, 7, 1) + timedelta(
                    days=n % 90,
                    minutes=(n * 7) % 1440,
                )

                writer.writerow([
                    f"ORD{n:08d}",
                    f"{n}-{n * 3 % 4 + 1}",
                    f"C{n * 17 % 500 + 1:05d}",
                    order_time.strftime("%Y-%m-%d %H:%M:%S"),
                    f"P{n * 13 % 120 + 1:04d}",
                    quantity, unit_price, discount, tax, gross, net,
                    STATUSES[n % len(STATUSES)],
                    PAYMENTS[n % len(PAYMENTS)],
                    CHANNELS[n % len(CHANNELS)],
                    CITIES[n % len(CITIES)],
                    order_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "ECOMMERCE_OMS",
                ])


def build_stream_events() -> None:
    """Create 5,000 JSONL events for Pub/Sub replay."""
    event_types = [
        "ORDER_CREATED", "PAYMENT_CAPTURED", "ORDER_SHIPPED",
        "ORDER_DELIVERED", "ORDER_CANCELLED",
    ]

    for part in range(2):
        path = ROOT / f"stream_events_{part + 1:02d}.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for offset in range(2500):
                n = part * 2500 + offset + 1
                event_ts = datetime(2026, 10, 1) + timedelta(seconds=n * 37)
                event = {
                    "event_id": f"EVT{n + 30000:09d}",
                    "order_id": f"ORD{(n - 1) % 10000 + 1:08d}",
                    "customer_id": f"C{n * 17 % 500 + 1:05d}",
                    "product_id": f"P{n * 13 % 120 + 1:04d}",
                    "event_type": event_types[n % len(event_types)],
                    "event_ts": event_ts.isoformat() + "Z",
                    "quantity": n % 4 + 1,
                    "unit_price": round(19 + (n * 23 % 481) + 0.49, 2),
                    "city": CITIES[n % len(CITIES)],
                    "source": "WEB_APP",
                    "schema_version": "v2",
                }
                handle.write(json.dumps(event) + "\n")


if __name__ == "__main__":
    build_orders()
    build_stream_events()
    print("Generated medium e-commerce workload.")
