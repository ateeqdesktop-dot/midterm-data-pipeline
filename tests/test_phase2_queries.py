import pytest
from src.queries.order_queries import (
    get_available_queries,
    execute_query,
    query_orders_by_customer,
    query_orders_by_city_and_status,
    query_quarantine_records_by_error,
    query_recent_orders_by_date_range,
    query_high_value_orders_by_delivery,
    AVAILABLE_QUERIES
)

def test_available_queries_catalog():
    queries = get_available_queries()
    assert len(queries) == 5
    names = [q["name"] for q in queries]
    assert "orders_by_customer" in names
    assert "orders_by_city_and_status" in names
    assert "quarantine_records_by_error" in names
    assert "recent_orders_by_date_range" in names
    assert "high_value_orders_by_delivery" in names

def test_query_orders_by_customer():
    results = query_orders_by_customer(customer_id="عميل-0", limit=5)
    assert isinstance(results, list)
    if results:
        doc = results[0]
        assert "customer_id" in doc
        assert "order_id" in doc
        assert doc["customer_id"] == "عميل-0"

def test_query_orders_by_city_and_status():
    results = query_orders_by_city_and_status(city="صنعاء", status="مؤكد", limit=5)
    assert isinstance(results, list)
    if results:
        doc = results[0]
        assert doc["city"] == "صنعاء"
        assert doc["status"] == "مؤكد"

def test_query_quarantine_records_by_error():
    results = query_quarantine_records_by_error(error_code="CORRUPTED_ITEMS_JSON", limit=5)
    assert isinstance(results, list)
    if results:
        doc = results[0]
        assert "error_codes" in doc
        assert "CORRUPTED_ITEMS_JSON" in doc["error_codes"]

def test_execute_query_dispatcher_valid_and_invalid():
    res = execute_query("orders_by_customer", {"customer_id": "عميل-0", "limit": 2})
    assert res["status"] == "success"
    assert res["query_name"] == "orders_by_customer"
    assert "results" in res
    
    with pytest.raises(KeyError):
        execute_query("non_existent_query_xyz")
