from fastapi import APIRouter, HTTPException
from src.jobs.job_runner import get_jobs_list, run_job, get_job_history, JOBS_REGISTRY

router = APIRouter()

@router.get("/jobs", tags=["Scheduled Jobs"])
def list_scheduled_jobs():
    """
    Returns registered background jobs, schedules, status, and latest execution record.
    """
    return {
        "jobs": get_jobs_list()
    }

@router.post("/jobs/{name}/run", tags=["Scheduled Jobs"])
def trigger_job_run(name: str):
    """
    Triggers an immediate manual execution of a registered job.
    Logs execution metrics to MongoDB and returns execution summary.
    """
    if name not in JOBS_REGISTRY:
        raise HTTPException(
            status_code=404,
            detail=f"Job '{name}' not found. Available jobs: {list(JOBS_REGISTRY.keys())}"
        )
        
    try:
        execution_report = run_job(name)
        return {
            "status": "success",
            "message": f"Job '{name}' executed.",
            "execution": execution_report
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Job execution failed: {str(e)}"
        )

@router.get("/jobs/{name}/history", tags=["Scheduled Jobs"])
def get_job_execution_history(name: str, limit: int = 10):
    """
    Returns recent historical execution logs for a specific job.
    """
    if name not in JOBS_REGISTRY:
        raise HTTPException(
            status_code=404,
            detail=f"Job '{name}' not found."
        )
    logs = get_job_history(job_name=name, limit=limit)
    return {
        "job_name": name,
        "count": len(logs),
        "history": logs
    }
