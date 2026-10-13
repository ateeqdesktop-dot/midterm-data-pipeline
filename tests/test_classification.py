import json
from src.validation.quarantine_classifier import classify_quarantine_errors
from src.validation.record_classifier import classify_and_tag_record

def test_classify_valid_record():
    record = {
        "order_id": "order-1",
        "order_date": "2025-01-01T12:00:00",
        "customer_id": "customer-1",
        "customer_name": "Mohammed",
        "total_amount": "5000.0",
        "currency": "YER",
        "items_json": json.dumps([{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": 5000.0, "total": 5000.0}])
    }
    
    seen_ids = set()
    errors, details = classify_quarantine_errors(record, seen_ids)
    assert len(errors) == 0
    
    enriched, status = classify_and_tag_record(record, record, [], seen_ids)
    assert status == "valid"
    assert enriched["quality_status"] == "valid"

def test_classify_missing_order_id():
    record = {
        "order_date": "2025-01-01T12:00:00",
        "customer_id": "customer-1",
        "total_amount": "5000.0",
        "currency": "YER",
        "items_json": json.dumps([{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": 5000.0, "total": 5000.0}])
    }
    
    seen_ids = set()
    errors, details = classify_quarantine_errors(record, seen_ids)
    assert "MISSING_ORDER_ID" in errors
    
    enriched, status = classify_and_tag_record(record, record, [], seen_ids)
    assert status == "quarantined"
    assert "MISSING_ORDER_ID" in enriched["error_codes"]
    assert "raw_record" in enriched

def test_classify_duplicate_order_id():
    record = {
        "order_id": "order-1",
        "order_date": "2025-01-01T12:00:00",
        "customer_id": "customer-1",
        "total_amount": "5000.0",
        "currency": "YER",
        "items_json": json.dumps([{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": 5000.0, "total": 5000.0}])
    }
    
    seen_ids = {"order-1"}
    errors, details = classify_quarantine_errors(record, seen_ids)
    assert "DUPLICATE_ORDER_ID" in errors
    
    enriched, status = classify_and_tag_record(record, record, [], seen_ids)
    assert status == "quarantined"
    assert "DUPLICATE_ORDER_ID" in enriched["error_codes"]

def test_classify_negative_values():
    record = {
        "order_id": "order-1",
        "order_date": "2025-01-01T12:00:00",
        "customer_id": "customer-1",
        "total_amount": "-500.0", # Negative
        "currency": "YER",
        "items_json": json.dumps([{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": -50.0, "total": -50.0}])
    }
    
    seen_ids = set()
    errors, details = classify_quarantine_errors(record, seen_ids)
    assert "AMBIGUOUS_NEGATIVE_VALUE" in errors

def test_classify_multiple_conflicting_errors():
    # missing customer id, impossible date, corrupted items
    record = {
        "order_id": "order-1",
        "order_date": "invalid-date",
        "total_amount": "5000.0",
        "currency": "YER",
        "items_json": "corrupted-json-string"
    }
    
    seen_ids = set()
    errors, details = classify_quarantine_errors(record, seen_ids)
    assert "MISSING_CUSTOMER_ID" in errors
    assert "INVALID_IMPOSSIBLE_DATE" in errors
    assert "CORRUPTED_ITEMS_JSON" in errors
    assert "MULTIPLE_CONFLICTING_ERRORS" in errors

def test_classify_missing_customer_id():
    record = {
        "order_id": "order-101",
        "order_date": "2025-01-01T12:00:00",
        "customer_id": "",
        "total_amount": "5000.0",
        "items_json": json.dumps([{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": 5000.0, "total": 5000.0}])
    }
    errors, details = classify_quarantine_errors(record)
    assert "MISSING_CUSTOMER_ID" in errors

def test_classify_invalid_impossible_date():
    for bad_date in ["1899-12-31T00:00:00", "2099-01-01T12:00:00", "invalid-date-string"]:
        record = {
            "order_id": "order-102",
            "order_date": bad_date,
            "customer_id": "cust-1",
            "total_amount": "5000.0",
            "items_json": json.dumps([{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": 5000.0, "total": 5000.0}])
        }
        errors, _ = classify_quarantine_errors(record)
        assert "INVALID_IMPOSSIBLE_DATE" in errors

def test_classify_corrupted_items_json():
    record = {
        "order_id": "order-103",
        "order_date": "2025-01-01T12:00:00",
        "customer_id": "cust-1",
        "total_amount": "5000.0",
        "items_json": "{not valid json at all"
    }
    errors, _ = classify_quarantine_errors(record)
    assert "CORRUPTED_ITEMS_JSON" in errors

def test_classify_empty_items():
    record = {
        "order_id": "order-104",
        "order_date": "2025-01-01T12:00:00",
        "customer_id": "cust-1",
        "total_amount": "5000.0",
        "items_json": "[]"
    }
    errors, _ = classify_quarantine_errors(record)
    assert "EMPTY_ITEMS" in errors

def test_classify_unknown_price():
    record = {
        "order_id": "order-105",
        "order_date": "2025-01-01T12:00:00",
        "customer_id": "cust-1",
        "total_amount": "unknown",
        "items_json": json.dumps([{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": None}])
    }
    errors, _ = classify_quarantine_errors(record)
    assert "UNKNOWN_PRICE" in errors

def test_classify_negative_values_in_item_qty_or_price():
    record = {
        "order_id": "order-106",
        "order_date": "2025-01-01T12:00:00",
        "customer_id": "cust-1",
        "total_amount": "5000.0",
        "items_json": json.dumps([{"sku": "SKU-1", "name": "Item 1", "qty": -2, "unit_price": 50.0, "total": -100.0}])
    }
    errors, _ = classify_quarantine_errors(record)
    assert "AMBIGUOUS_NEGATIVE_VALUE" in errors

def test_classify_single_argument_call():
    record = {
        "order_id": "order-107",
        "order_date": "2025-01-01T12:00:00",
        "customer_id": "cust-1",
        "total_amount": "100.0",
        "items_json": json.dumps([{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": 100.0, "total": 100.0}])
    }
    errors, details = classify_quarantine_errors(record)
    assert len(errors) == 0

