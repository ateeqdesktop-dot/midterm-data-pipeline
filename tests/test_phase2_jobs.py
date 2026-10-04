import pytest
from src.jobs.job_runner import get_jobs_list, run_job, get_job_history, JOBS_REGISTRY

def test_jobs_registry_catalog():
    jobs = get_jobs_list()
    assert len(jobs) >= 2
    names = [j["job_name"] for j in jobs]
    assert "refresh_materialized_views_job" in names
    assert "daily_data_health_and_quarantine_audit_job" in names

def test_manual_job_execution_logging():
    res = run_job("daily_data_health_and_quarantine_audit_job")
    assert res["status"] == "success"
    assert res["job_name"] == "daily_data_health_and_quarantine_audit_job"
    assert "duration_seconds" in res
    assert "started_at" in res
    assert "finished_at" in res
    
    # Check that execution log was recorded in MongoDB
    history = get_job_history("daily_data_health_and_quarantine_audit_job", limit=1)
    assert len(history) > 0
    assert history[0]["status"] == "success"

def test_run_unknown_job():
    with pytest.raises(KeyError):
        run_job("non_existent_fake_job")

def test_job_failure_handling_and_logging():
    """Verify that failing jobs log status='failed' with error, timestamps, and do not crash."""
    def failing_task():
        raise RuntimeError("Controlled failure test: downstream service timeout")

    JOBS_REGISTRY["test_failing_job"] = {
        "name": "test_failing_job",
        "description": "Simulated failing job for safety verification",
        "schedule": "0 0 * * *",
        "func": failing_task
    }
    try:
        report = run_job("test_failing_job")
        assert report["status"] == "failed"
        assert "Controlled failure test" in report["error"]
        assert "started_at" in report
        assert "finished_at" in report
        assert "duration_seconds" in report
        
        # Verify persistence in MongoDB
        history = get_job_history("test_failing_job", limit=1)
        assert len(history) >= 1
        assert history[0]["status"] == "failed"
        assert "Controlled failure test" in history[0]["error"]
    finally:
        del JOBS_REGISTRY["test_failing_job"]

