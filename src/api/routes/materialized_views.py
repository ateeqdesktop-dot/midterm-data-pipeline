from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from src.materialized_views.mv_manager import (
    refresh_materialized_views,
    query_materialized_view,
    get_mv_catalog,
    MV_CATALOG
)

router = APIRouter()

class RefreshRequest(BaseModel):
    mode: Optional[str] = "incremental"

@router.post("/refresh-mv", tags=["Materialized Views"])
def refresh_mv_endpoint(request: Optional[RefreshRequest] = None, mode: Optional[str] = None):
    """
    Refreshes materialized views using incremental watermark logic by default,
    or full build if specified.
    """
    refresh_mode = mode or (request.mode if request else "incremental")
    if refresh_mode not in ("incremental", "full"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid refresh mode '{refresh_mode}'. Allowed values: 'incremental', 'full'."
        )
        
    try:
        result = refresh_materialized_views(mode=refresh_mode)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Materialized view refresh failed: {str(e)}"
        )

@router.get("/materialized-views", tags=["Materialized Views"])
def list_materialized_views():
    """
    Lists configured materialized views and metadata.
    """
    return {
        "materialized_views": [v["name"] for v in get_mv_catalog()],
        "details": get_mv_catalog()
    }

@router.get("/materialized-views/{name}", tags=["Materialized Views"])
def get_materialized_view_data(
    name: str,
    city: Optional[str] = Query(None, description="Optional city filter"),
    date: Optional[str] = Query(None, description="Optional date filter (YYYY-MM-DD)"),
    limit: int = Query(50, ge=1, le=500, description="Maximum documents to return")
):
    """
    Queries pre-computed metrics from materialized views with sub-millisecond response.
    """
    if name not in MV_CATALOG:
        raise HTTPException(
            status_code=404,
            detail=f"Materialized view '{name}' not found. Available views: {list(MV_CATALOG.keys())}"
        )
        
    filters = {}
    if city:
        filters["city"] = city
    if date:
        filters["date"] = date
        
    try:
        data = query_materialized_view(name, limit=limit, filter_query=filters)
        return {
            "view_name": name,
            "count": len(data),
            "data": data
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to query materialized view: {str(e)}"
        )
