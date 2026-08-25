from quality_rules import clean_order


def test_clean_order_records_audit_trail():
    result = clean_order(
        {
            "order_id": " O-1 ",
            "customer_id": "C-1",
            "customer_email": "u@@mail..com",
            "phone": "٧٧٧ ١٢٣",
            "order_date": "31/01/2025",
            "items": '[{"price": 2, "quantity": 1}]',
            "price": "2 ريال",
            "quantity": "١",
            "shipping_cost": "0",
            "total": "2",
            "status": "مؤكد",
        }
    )
    assert result["quality_status"] == "corrected"
    assert len(result["corrections"]) >= 5
    assert all(
        set(item) == {"field", "original_value", "corrected_value", "rule_code"}
        for item in result["corrections"]
    )


def test_uncorrectable_order_is_quarantined():
    result = clean_order(
        {
            "order_id": "",
            "customer_id": "C-1",
            "order_date": "2025-02-30",
            "items": "broken",
            "price": "unknown",
        }
    )
    assert result["quality_status"] == "quarantined"
    assert "MULTIPLE_CONFLICTING_ERRORS" in result["error_codes"]
