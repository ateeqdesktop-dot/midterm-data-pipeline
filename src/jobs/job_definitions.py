import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any
from config import settings
from src import mongo_setup
from src.materialized_views.mv_manager import refresh_materialized_views
from src.utils.json_encoder import serialize_mongo_doc

def run_refresh_mv_job() -> Dict[str, Any]:
    """
    Job 1: Materialized Views Incremental Refresh.
    Refreshes daily_sales_summary and city_performance_summary using incremental watermark.
    """
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    try:
        res = refresh_materialized_views(mode="incremental", db=db)
        records_processed = res.get("new_records_detected", 0)
        return {
            "records_processed": records_processed,
            "details": res
        }
    finally:
        client.close()

def run_daily_audit_job() -> Dict[str, Any]:
    """
    Job 2: Daily Data Health and Quarantine Audit.
    Analyzes collection metrics, error distributions, and system integrity.
    Saves snapshot into audit_health_reports collection and reports/daily_health_report.json.
    """
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    try:
        raw_count = db[settings.COLLECTION_RAW].estimated_document_count()
        val_count = db[settings.COLLECTION_VALIDATED].estimated_document_count()
        quar_count = db[settings.COLLECTION_QUARANTINE].estimated_document_count()
        
        # Aggregate top quarantine error codes
        pipeline = [
            {"$unwind": "$error_codes"},
            {"$group": {"_id": "$error_codes", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        error_breakdown = list(db[settings.COLLECTION_QUARANTINE].aggregate(pipeline))
        
        audit_doc = {
            "report_name": "daily_data_health_and_quarantine_audit",
            "generated_at": datetime.now(timezone.utc),
            "collections_summary": {
                "orders_raw_estimated": raw_count,
                "orders_validated_estimated": val_count,
                "orders_quarantine_estimated": quar_count
            },
            "quarantine_error_breakdown": [
                {"error_code": item["_id"], "count": item["count"]}
                for item in error_breakdown
            ],
            "system_health": "healthy" if quar_count < raw_count else "degraded"
        }
        
        # Save to DB collection
        db["audit_health_reports"].insert_one(audit_doc)
        
        # Save to JSON file on disk
        reports_dir = Path("reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        with open(reports_dir / "daily_health_report.json", "w", encoding="utf-8") as f:
            json.dump(serialize_mongo_doc(audit_doc), f, ensure_ascii=False, indent=2)
            
        return {
            "records_processed": len(error_breakdown),
            "details": serialize_mongo_doc(audit_doc)
        }
    finally:
        client.close()
