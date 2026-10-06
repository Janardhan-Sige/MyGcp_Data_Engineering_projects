"""Unit tests for reusable Python transformations.

These tests run locally and do not require a live GCP project.
"""

from dataflow.common.transforms import clean_order


def test_clean_order_calculates_amount():
    # Arrange: create a small representative order.
    order = {
        "order_id": "1",
        "customer_id": "C1",
        "order_ts": "2026-10-01 10:00:00",
        "product_id": "P1",
        "quantity": 2,
        "unit_price": 25.5,
        "status": "completed",
        "city": "hyderabad",
    }

    # Act: apply the production transformation.
    result = clean_order(order)

    # Assert: validate both the metric and normalization.
    assert result["order_amount"] == 51.0
    assert result["status"] == "COMPLETED"
    assert result["city"] == "Hyderabad"
