import pytest
from src.materialized_views.mv_manager import (
    get_mv_catalog,
    refresh_materialized_views,
    query_materialized_view,
    MV_DAILY_SALES,
    MV_CITY_PERFORMANCE
)

def test_materialized_view_catalog():
    catalog = get_mv_catalog()
    assert len(catalog) >= 2
    names = [v["name"] for v in catalog]
    assert "daily_sales_summary" in names
    assert "city_performance_summary" in names

def test_incremental_refresh_idempotent():
    res = refresh_materialized_views(mode="incremental")
    assert res["status"] in ("success", "up_to_date")
    assert "mode" in res
    assert res["mode"] == "incremental"

def test_query_materialized_view_performance():
    data = query_materialized_view("daily_sales_summary", limit=5)
    assert isinstance(data, list)
    assert len(data) > 0
    row = data[0]
    assert "date" in row
    assert "city" in row
    assert "total_revenue" in row
    assert "order_count" in row

def test_query_invalid_materialized_view():
    with pytest.raises(KeyError):
        query_materialized_view("unknown_view_123")
