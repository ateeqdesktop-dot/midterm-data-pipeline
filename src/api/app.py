from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from src.api.routes import (
    health_router,
    ingest_router,
    indexes_router,
    queries_router,
    aggregations_router,
    mv_router,
    jobs_router
)
from src.jobs.job_runner import start_scheduler, shutdown_scheduler

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start background scheduler
    try:
        start_scheduler()
    except Exception as e:
        print(f"Warning: Failed to start background scheduler: {e}")
    yield
    # Shutdown: Stop scheduler
    try:
        shutdown_scheduler()
    except Exception as e:
        print(f"Warning: Error stopping scheduler: {e}")

app = FastAPI(
    title="Big Data - Phase 2 Unified E-Commerce API",
    description="Unified API gateway for Queries, Indexes, Aggregations, Materialized Views, and Scheduled Jobs on MongoDB.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global exception handler to prevent leaking internal stack traces / secrets
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": "Internal server error occurred.",
            "error_type": type(exc).__name__,
            "detail": str(exc)
        }
    )

# Include Routers
app.include_router(health_router)
app.include_router(ingest_router)
app.include_router(indexes_router)
app.include_router(queries_router)
app.include_router(aggregations_router)
app.include_router(mv_router)
app.include_router(jobs_router)

@app.get("/", tags=["System"])
def root():
    return {
        "title": "Big Data - Phase 2 Unified API",
        "version": "2.0.0",
        "documentation": "/docs",
        "endpoints": {
            "health": "/health",
            "ingest": "POST /ingest",
            "indexes": "/indexes",
            "queries": "/queries",
            "aggregations": "/aggregations",
            "refresh_mv": "POST /refresh-mv",
            "jobs": "/jobs"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.app:app", host="0.0.0.0", port=8000, reload=True)
