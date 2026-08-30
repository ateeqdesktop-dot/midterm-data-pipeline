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
