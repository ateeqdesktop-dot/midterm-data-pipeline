import pytest
from fastapi.testclient import TestClient
from src.api.app import app

@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c

def test_api_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["database"] == "connected"

def test_api_indexes(client):
    response = client.get("/indexes")
    assert response.status_code == 200
    assert len(response.json()["indexes"]) >= 3
    
    post_res = client.post("/indexes")
    assert post_res.status_code == 200
    assert post_res.json()["status"] == "success"

def test_api_queries_list(client):
    response = client.get("/queries")
    assert response.status_code == 200
    data = response.json()
    assert len(data["queries"]) == 5

def test_api_query_execution_valid(client):
    response = client.get("/queries/orders_by_customer?customer_id=عميل-0&limit=3")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "results" in data

def test_api_query_execution_404(client):
    response = client.get("/queries/invalid_query_name")
    assert response.status_code == 404

def test_api_aggregations_list(client):
    response = client.get("/aggregations")
    assert response.status_code == 200
    data = response.json()
    assert len(data["aggregations"]) == 5

def test_api_aggregation_execution_valid(client):
    response = client.get("/aggregations/sales_by_city?limit=2")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert len(data["results"]) <= 2

def test_api_aggregation_execution_404(client):
    response = client.get("/aggregations/unknown_aggregation_name")
    assert response.status_code == 404

def test_api_refresh_mv(client):
    response = client.post("/refresh-mv")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ("success", "up_to_date")

def test_api_jobs_list(client):
    response = client.get("/jobs")
    assert response.status_code == 200
    data = response.json()
    assert len(data["jobs"]) >= 2

def test_api_job_run_valid(client):
    response = client.post("/jobs/daily_data_health_and_quarantine_audit_job/run")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "execution" in data

def test_api_job_run_404(client):
    response = client.post("/jobs/non_existent_job/run")
    assert response.status_code == 404

def test_api_ingest_bad_file(client):
    response = client.post("/ingest", json={"file_path": "invalid_missing_file.csv"})
    assert response.status_code == 400
