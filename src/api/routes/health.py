from fastapi import APIRouter
from config import settings
from src import mongo_setup

router = APIRouter()

@router.get("/health", tags=["System"])
def get_health():
    """
    Health check endpoint verifying system responsiveness and MongoDB connectivity.
    """
    client = mongo_setup.get_mongo_client()
    try:
        db = mongo_setup.get_database(client)
        db.command("ping")
        collections = db.list_collection_names()
        
        counts = {}
        for coll in [settings.COLLECTION_RAW, settings.COLLECTION_VALIDATED, settings.COLLECTION_QUARANTINE]:
            if coll in collections:
                counts[coll] = db[coll].estimated_document_count()
                
        return {
            "status": "ok",
            "database": "connected",
            "db_name": settings.MONGO_DB_NAME,
            "collections_count": len(collections),
            "estimated_documents": counts
        }
    except Exception as e:
        return {
            "status": "error",
            "database": "disconnected",
            "error": str(e)
        }
    finally:
        client.close()
