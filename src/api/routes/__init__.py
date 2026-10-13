from src.api.routes.health import router as health_router
from src.api.routes.ingest import router as ingest_router
from src.api.routes.indexes import router as indexes_router
from src.api.routes.queries import router as queries_router
from src.api.routes.aggregations import router as aggregations_router
from src.api.routes.materialized_views import router as mv_router
from src.api.routes.jobs import router as jobs_router

__all__ = [
    "health_router",
    "ingest_router",
    "indexes_router",
    "queries_router",
    "aggregations_router",
    "mv_router",
    "jobs_router"
]
