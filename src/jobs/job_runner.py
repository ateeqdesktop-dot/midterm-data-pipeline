import time
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from config import settings
from src import mongo_setup
from src.jobs.job_definitions import run_refresh_mv_job, run_daily_audit_job
from src.utils.json_encoder import serialize_mongo_doc

JOB_LOGS_COLLECTION = "job_execution_logs"

JOBS_REGISTRY = {
    "refresh_materialized_views_job": {
        "name": "refresh_materialized_views_job",
        "description": "Incrementally refreshes daily sales and city performance materialized views based on data watermark.",
        "schedule": "*/15 * * * *",
        "func": run_refresh_mv_job,
        "trigger": CronTrigger(minute="*/15")
    },
    "daily_data_health_and_quarantine_audit_job": {
        "name": "daily_data_health_and_quarantine_audit_job",
        "description": "Generates data health, collection size, and quarantine error breakdown audit reports.",
        "schedule": "0 2 * * *",
        "func": run_daily_audit_job,
        "trigger": CronTrigger(hour=2, minute=0)
    }
}

_scheduler: Optional[BackgroundScheduler] = None

def get_job_history(job_name: Optional[str] = None, limit: int = 10, db=None) -> List[Dict[str, Any]]:
    """Retrieves recent execution logs from job_execution_logs collection."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        coll = db[JOB_LOGS_COLLECTION]
        query = {"job_name": job_name} if job_name else {}
        cursor = coll.find(query, {"_id": 0}).sort("started_at", -1).limit(limit)
        return [serialize_mongo_doc(doc) for doc in cursor]
    finally:
        if close_client:
            client.close()

def log_job_execution(log_entry: Dict[str, Any], db=None):
    """Writes execution log entry to MongoDB."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        db[JOB_LOGS_COLLECTION].insert_one(log_entry)
    finally:
        if close_client:
            client.close()

def run_job(name: str, db=None) -> Dict[str, Any]:
    """
    Executes a scheduled job manually or programmatically.
    Measures elapsed time, tracks status, logs to MongoDB, and returns JSON-safe report.
    """
    if name not in JOBS_REGISTRY:
        raise KeyError(f"Job '{name}' not found. Available jobs: {list(JOBS_REGISTRY.keys())}")
        
    job_meta = JOBS_REGISTRY[name]
    started_at = datetime.now(timezone.utc)
    t0 = time.time()
    status = "failed"
    error = None
    records_processed = 0
    details = {}
    
    try:
        result = job_meta["func"]()
        status = "success"
        records_processed = result.get("records_processed", 0)
        details = result.get("details", {})
    except Exception as e:
        status = "failed"
        error = str(e)
        details = {"traceback": traceback.format_exc()}
        
    finished_at = datetime.now(timezone.utc)
    duration_seconds = round(time.time() - t0, 3)
    
    log_doc = {
        "job_name": name,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "duration_seconds": duration_seconds,
        "records_processed": records_processed,
        "error": error,
        "details": details
    }
    
    log_job_execution(log_doc, db=db)
    return serialize_mongo_doc(log_doc)

def get_jobs_list() -> List[Dict[str, Any]]:
    """Returns list of registered jobs with schedule, status, and last run."""
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    try:
        jobs_list = []
        for name, meta in JOBS_REGISTRY.items():
            last_runs = get_job_history(job_name=name, limit=1, db=db)
            last_run = last_runs[0] if last_runs else None
            
            jobs_list.append({
                "job_name": name,
                "description": meta["description"],
                "schedule": meta["schedule"],
                "is_active": _scheduler.running if _scheduler else False,
                "last_execution": last_run
            })
        return jobs_list
    finally:
        client.close()

def start_scheduler():
    """Initializes and starts the background job scheduler."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return _scheduler
        
    _scheduler = BackgroundScheduler(daemon=True)
    for name, meta in JOBS_REGISTRY.items():
        _scheduler.add_job(
            func=run_job,
            trigger=meta["trigger"],
            args=[name],
            id=name,
            replace_existing=True
        )
    _scheduler.start()
    print("Background job scheduler started successfully.")
    return _scheduler

def shutdown_scheduler():
    """Stops the background job scheduler cleanly."""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        print("Background job scheduler stopped.")
