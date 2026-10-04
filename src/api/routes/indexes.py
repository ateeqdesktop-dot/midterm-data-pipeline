from fastapi import APIRouter, HTTPException
from src.indexes.index_manager import create_phase2_indexes, get_index_definitions

router = APIRouter()

@router.post("/indexes", tags=["Indexes"])
def build_indexes():
    """
    Creates required Phase 2 indexes idempotently.
    Checks existing indexes beforehand to avoid unnecessary overhead.
    """
    try:
        result = create_phase2_indexes()
        return {
            "status": "success",
            "message": "Index verification and creation finished.",
            "data": result
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create indexes: {str(e)}"
        )

@router.get("/indexes", tags=["Indexes"])
def list_indexes():
    """
    Lists metadata and specifications of Phase 2 indexes.
    """
    return {
        "status": "success",
        "indexes": get_index_definitions()
    }
