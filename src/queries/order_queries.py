from typing import Dict, Any, List, Optional
from config import settings
from src import mongo_setup
from src.utils.json_encoder import serialize_mongo_doc

# Registered queries catalog
AVAILABLE_QUERIES = {
    "orders_by_customer": {
        "name": "orders_by_customer",
        "description": "Retrieves orders for a specific customer, sorted by order_date descending.",
        "params": {
            "customer_id": {"type": "str", "required": False, "default": "عميل-0", "description": "Customer identifier"}
        },
        "default_limit": 50,
        "targeted_index": "idx_customer_id"
    },
    "orders_by_city_and_status": {
        "name": "orders_by_city_and_status",
        "description": "Retrieves orders filtered by city and order status.",
        "params": {
            "city": {"type": "str", "required": False, "default": "صنعاء", "description": "City name (e.g., صنعاء, عدن, تعز)"},
            "status": {"type": "str", "required": False, "default": "مؤكد", "description": "Order status (e.g., مؤكد, توصيل, ملغي)"}
        },
        "default_limit": 50,
        "targeted_index": "idx_city_status"
    },
    "quarantine_records_by_error": {
        "name": "quarantine_records_by_error",
        "description": "Retrieves quarantined orders flagged with a specific error code.",
        "params": {
            "error_code": {"type": "str", "required": False, "default": "CORRUPTED_ITEMS_JSON", "description": "Quarantine error code"}
        },
        "default_limit": 50,
        "targeted_index": "idx_quarantine_error_codes"
    },
    "recent_orders_by_date_range": {
        "name": "recent_orders_by_date_range",
        "description": "Retrieves orders within a specific ISO datetime range.",
        "params": {
            "start_date": {"type": "str", "required": False, "default": "2025-01-01T00:00:00", "description": "Start ISO datetime"},
            "end_date": {"type": "str", "required": False, "default": "2025-03-31T23:59:59", "description": "End ISO datetime"}
        },
        "default_limit": 50,
        "targeted_index": "idx_version_date"
    },
    "high_value_orders_by_delivery": {
        "name": "high_value_orders_by_delivery",
        "description": "Retrieves high-value orders filtered by delivery method.",
        "params": {
            "delivery_type": {"type": "str", "required": False, "default": "سريع", "description": "Delivery type (سريع / عادي)"},
            "min_amount": {"type": "float", "required": False, "default": 100000.0, "description": "Minimum order total amount"}
        },
        "default_limit": 50,
        "targeted_index": "idx_delivery_type"
    }
}

def get_available_queries() -> List[Dict[str, Any]]:
    """Returns metadata for all available queries."""
    return list(AVAILABLE_QUERIES.values())

def query_orders_by_customer(customer_id: str = "عميل-0", limit: int = 50, db=None) -> List[Dict[str, Any]]:
    """Query 1: Find orders by customer_id sorted by order_date desc."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        coll = db[settings.COLLECTION_VALIDATED]
        cursor = coll.find(
            {"customer_id": customer_id},
            {"_id": 0, "order_id": 1, "customer_id": 1, "customer_name": 1, "order_date": 1, "status": 1, "total_amount": 1, "city": 1}
        ).sort("order_date", -1).limit(limit)
        results = [serialize_mongo_doc(doc) for doc in cursor]
        return results
    finally:
        if close_client:
            client.close()

def query_orders_by_city_and_status(city: str = "صنعاء", status: str = "مؤكد", limit: int = 50, db=None) -> List[Dict[str, Any]]:
    """Query 2: Filter orders by city and status."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        coll = db[settings.COLLECTION_VALIDATED]
        cursor = coll.find(
            {"city": city, "status": status},
            {"_id": 0, "order_id": 1, "city": 1, "status": 1, "order_date": 1, "total_amount": 1, "payment_method": 1}
        ).sort("order_date", -1).limit(limit)
        results = [serialize_mongo_doc(doc) for doc in cursor]
        return results
    finally:
        if close_client:
            client.close()

def query_quarantine_records_by_error(error_code: str = "CORRUPTED_ITEMS_JSON", limit: int = 50, db=None) -> List[Dict[str, Any]]:
    """Query 3: Filter quarantined orders by error_codes."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        coll = db[settings.COLLECTION_QUARANTINE]
        cursor = coll.find(
            {"error_codes": error_code},
            {"_id": 0, "order_id": 1, "error_codes": 1, "error_details": 1, "ingested_at": 1}
        ).limit(limit)
        results = [serialize_mongo_doc(doc) for doc in cursor]
        return results
    finally:
        if close_client:
            client.close()

def query_recent_orders_by_date_range(start_date: str = "2025-01-01T00:00:00", end_date: str = "2025-03-31T23:59:59", limit: int = 50, db=None) -> List[Dict[str, Any]]:
    """Query 4: Filter orders by order_date range."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        coll = db[settings.COLLECTION_VALIDATED]
        cursor = coll.find(
            {"order_date": {"$gte": start_date, "$lte": end_date}},
            {"_id": 0, "order_id": 1, "order_date": 1, "city": 1, "status": 1, "total_amount": 1}
        ).sort("order_date", -1).limit(limit)
        results = [serialize_mongo_doc(doc) for doc in cursor]
        return results
    finally:
        if close_client:
            client.close()

def query_high_value_orders_by_delivery(delivery_type: str = "سريع", min_amount: float = 100000.0, limit: int = 50, db=None) -> List[Dict[str, Any]]:
    """Query 5: Filter high value orders by delivery_type."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        coll = db[settings.COLLECTION_VALIDATED]
        cursor = coll.find(
            {
                "delivery_type": delivery_type,
                "$expr": {"$gte": [{"$toDouble": "$total_amount"}, float(min_amount)]}
            },
            {"_id": 0, "order_id": 1, "delivery_type": 1, "total_amount": 1, "city": 1, "status": 1}
        ).limit(limit)
        results = [serialize_mongo_doc(doc) for doc in cursor]
        return results
    finally:
        if close_client:
            client.close()

def execute_query(name: str, params: Optional[Dict[str, Any]] = None, db=None) -> Dict[str, Any]:
    """
    Executes a query by name with given parameters.
    Returns:
        Dict with status, query_name, count, and results.
    """
    if name not in AVAILABLE_QUERIES:
        raise KeyError(f"Query '{name}' not found. Available queries: {list(AVAILABLE_QUERIES.keys())}")
        
    params = params or {}
    limit = int(params.get("limit", AVAILABLE_QUERIES[name]["default_limit"]))
    
    if name == "orders_by_customer":
        cid = params.get("customer_id") or AVAILABLE_QUERIES[name]["params"]["customer_id"]["default"]
        results = query_orders_by_customer(customer_id=cid, limit=limit, db=db)
    elif name == "orders_by_city_and_status":
        city = params.get("city") or AVAILABLE_QUERIES[name]["params"]["city"]["default"]
        status = params.get("status") or AVAILABLE_QUERIES[name]["params"]["status"]["default"]
        results = query_orders_by_city_and_status(city=city, status=status, limit=limit, db=db)
    elif name == "quarantine_records_by_error":
        err = params.get("error_code") or AVAILABLE_QUERIES[name]["params"]["error_code"]["default"]
        results = query_quarantine_records_by_error(error_code=err, limit=limit, db=db)
    elif name == "recent_orders_by_date_range":
        s_date = params.get("start_date") or AVAILABLE_QUERIES[name]["params"]["start_date"]["default"]
        e_date = params.get("end_date") or AVAILABLE_QUERIES[name]["params"]["end_date"]["default"]
        results = query_recent_orders_by_date_range(start_date=s_date, end_date=e_date, limit=limit, db=db)
    elif name == "high_value_orders_by_delivery":
        deliv = params.get("delivery_type") or AVAILABLE_QUERIES[name]["params"]["delivery_type"]["default"]
        min_amt = float(params.get("min_amount") or AVAILABLE_QUERIES[name]["params"]["min_amount"]["default"])
        results = query_high_value_orders_by_delivery(delivery_type=deliv, min_amount=min_amt, limit=limit, db=db)
    else:
        results = []
        
    return {
        "status": "success",
        "query_name": name,
        "count": len(results),
        "results": results
    }
