from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from src.aggregations.reports import get_available_aggregations, execute_aggregation, AVAILABLE_AGGREGATIONS

router = APIRouter()

@router.get("/aggregations", tags=["Aggregations"])
def list_aggregations():
    """
    Returns the catalog of available aggregation reports with descriptions.
    """
    return {
        "aggregations": [a["name"] for a in get_available_aggregations()],
        "details": get_available_aggregations()
    }

@router.get("/aggregations/{name}", tags=["Aggregations"])
def run_aggregation_by_name(
    name: str,
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Optional result row limit")
):
    """
    Executes a specified aggregation report by name.
    Validates name and returns authentic real-time aggregation results.
    """
    if name not in AVAILABLE_AGGREGATIONS:
        raise HTTPException(
            status_code=404,
            detail=f"Aggregation '{name}' not found. Available reports: {list(AVAILABLE_AGGREGATIONS.keys())}"
        )
        
    params = {}
    if limit is not None:
        params["limit"] = limit
        
    try:
        result = execute_aggregation(name, params)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Aggregation execution failed: {str(e)}"
        )
