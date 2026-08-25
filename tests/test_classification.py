import pytest

from quality_rules import clean_order


def _base(**overrides):
    row = {
        "order_id": "O-1",
        "customer_id": "C-1",
        "customer_email": "user@example.com",
        "phone": "777123456",
        "order_date": "2025-01-31",
        "items": '[{"price": 2, "quantity": 1}]',
        "price": "2",
        "quantity": "1",
        "shipping_cost": "0",
        "total": "2",
        "status": "confirmed",
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"order_id": ""}, "MISSING_ORDER_ID"),
        ({"customer_id": ""}, "MISSING_CUSTOMER_ID"),
        ({"order_date": "2025-02-30"}, "INVALID_IMPOSSIBLE_DATE"),
        ({"items": "not-json"}, "CORRUPTED_ITEMS_JSON"),
        ({"items": "[]"}, "EMPTY_ITEMS"),
        ({"price": "unknown"}, "UNKNOWN_PRICE"),
        ({"price": "-5"}, "AMBIGUOUS_NEGATIVE_VALUE"),
    ],
)
def test_official_quarantine_codes(overrides, expected):
    result = clean_order(_base(**overrides))
    assert result["quality_status"] == "quarantined"
    assert expected in result["error_codes"]
    assert result["error_details"]


def test_multiple_conflicting_errors_are_collapsed_to_official_code():
    result = clean_order(_base(order_id="", order_date="2025-02-30", items="bad", price="unknown"))
    assert result["quality_status"] == "quarantined"
    assert result["error_codes"] == ["MULTIPLE_CONFLICTING_ERRORS"]
