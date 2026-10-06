"""Focused tests for production parsing and validation contracts."""

from dataflow.batch.batch_orders_pipeline import ParseValidateOrder


def test_valid_order_is_typed():
    row = (
        "ORD00000001,1-1,C00001,2026-07-01 10:00:00,P0001,2,99.99,"
        "3.00,17.46,199.98,214.44,COMPLETED,UPI,WEB,Hyderabad,"
        "2026-07-01 10:00:00,ECOMMERCE_OMS"
    )
    result = list(ParseValidateOrder().process(row))
    assert result[0]["quantity"] == 2
    assert result[0]["unit_price"] == 99.99


def test_invalid_quantity_goes_to_dlq():
    row = (
        "ORD00000001,1-1,C00001,2026-07-01 10:00:00,P0001,0,99.99,"
        "3.00,17.46,199.98,214.44,COMPLETED,UPI,WEB,Hyderabad,"
        "2026-07-01 10:00:00,ECOMMERCE_OMS"
    )
    outputs = list(ParseValidateOrder().process(row))
    assert outputs[0]["error_message"] == "Invalid quantity or unit_price"
