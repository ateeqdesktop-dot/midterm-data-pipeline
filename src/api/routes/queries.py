from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from src.queries.order_queries import get_available_queries, execute_query, AVAILABLE_QUERIES

router = APIRouter()

@router.get("/queries", tags=["Queries"])
def list_queries():
    """
    Returns the list of available database queries along with their descriptions and parameters.
    """
    return {
        "queries": [q["name"] for q in get_available_queries()],
        "details": get_available_queries()
    }

@router.get("/queries/{name}", tags=["Queries"])
def run_query_by_name(
    name: str,
    customer_id: Optional[str] = Query(None, description="Customer ID filter (e.g. عميل-0)"),
    city: Optional[str] = Query(None, description="City name filter (e.g. صنعاء)"),
    status: Optional[str] = Query(None, description="Order status filter (e.g. مؤكد)"),
    error_code: Optional[str] = Query(None, description="Quarantine error code (e.g. CORRUPTED_ITEMS_JSON)"),
    start_date: Optional[str] = Query(None, description="Start ISO datetime"),
    end_date: Optional[str] = Query(None, description="End ISO datetime"),
    delivery_type: Optional[str] = Query(None, description="Delivery type (سريع / عادي)"),
    min_amount: Optional[float] = Query(None, description="Minimum order amount threshold"),
    limit: int = Query(50, ge=1, le=500, description="Maximum documents to return")
):
    """
    Executes a specified query by name against MongoDB.
    Validates name and parameters, returning authentic JSON query results.
    """
    if name not in AVAILABLE_QUERIES:
        raise HTTPException(
            status_code=404,
            detail=f"Query '{name}' not found. Available queries: {list(AVAILABLE_QUERIES.keys())}"
        )
        
    params = {"limit": limit}
    if customer_id is not None:
        params["customer_id"] = customer_id
    if city is not None:
        params["city"] = city
    if status is not None:
        params["status"] = status
    if error_code is not None:
        params["error_code"] = error_code
    if start_date is not None:
        params["start_date"] = start_date
    if end_date is not None:
        params["end_date"] = end_date
    if delivery_type is not None:
        params["delivery_type"] = delivery_type
    if min_amount is not None:
        params["min_amount"] = min_amount
        
    try:
        result = execute_query(name, params)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Query execution error: {str(e)}"
        )
