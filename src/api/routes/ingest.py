import os
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from src.main import process_single_file
from src.utils.json_encoder import serialize_mongo_doc

router = APIRouter()

class IngestRequest(BaseModel):
    file_path: Optional[str] = "data/sample_orders.csv"
    incremental: Optional[bool] = False

@router.post("/ingest", tags=["Ingestion Pipeline"])
def ingest_data(request: Optional[IngestRequest] = None, file_path: Optional[str] = None, incremental: Optional[bool] = None):
    """
    Triggers the existing hybrid ELT ingestion pipeline from the midterm project.
    Delegates directly to process_single_file without inventing a separate ingestion pipeline.
    """
    # Support both JSON payload and Query parameters
    target_path = file_path or (request.file_path if request else "data/sample_orders.csv")
    is_incremental = incremental if incremental is not None else (request.incremental if request else False)
    
    if not os.path.exists(target_path):
        raise HTTPException(
            status_code=400,
            detail=f"Target input file '{target_path}' not found on server."
        )
        
    try:
        metrics = process_single_file(target_path, incremental=is_incremental)
        if not metrics:
            raise HTTPException(
                status_code=500,
                detail=f"Ingestion pipeline completed for '{target_path}', but returned empty metrics."
            )
        return {
            "status": "success",
            "message": f"File '{os.path.basename(target_path)}' processed successfully.",
            "metrics": serialize_mongo_doc(metrics)
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ingestion pipeline execution failed: {str(e)}"
        )
