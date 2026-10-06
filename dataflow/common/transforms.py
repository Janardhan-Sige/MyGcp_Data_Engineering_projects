"""Reusable Apache Beam transformations.

The functions in this file are deliberately small so each transformation can
be understood and unit-tested independently during interview preparation.
"""

from datetime import datetime


def parse_csv_order(line: str) -> dict:
    """Convert one CSV line into a typed order dictionary.

    The header is handled by the batch pipeline before this function is called.
    """
    parts = [value.strip() for value in line.split(",")]

    return {
        "order_id": parts[0],
        "customer_id": parts[1],
        "order_ts": parts[2],
        "product_id": parts[3],
        "quantity": int(parts[4]),
        "unit_price": float(parts[5]),
        "status": parts[6],
        "city": parts[7],
    }


def clean_order(order: dict) -> dict:
    """Apply simple business/data-quality rules."""

    # Calculate the order amount once so downstream SQL can reuse it.
    order["order_amount"] = round(order["quantity"] * order["unit_price"], 2)

    # Normalize status and city values.
    order["status"] = order["status"].upper()
    order["city"] = order["city"].title()

    return order


def parse_event(message: bytes) -> dict:
    """Decode a Pub/Sub JSON message into a Python dictionary."""

    import json

    event = json.loads(message.decode("utf-8"))

    # Add a calculated metric to demonstrate streaming transformation.
    event["order_amount"] = round(
        float(event["quantity"]) * float(event["unit_price"]), 2
    )

    return event
