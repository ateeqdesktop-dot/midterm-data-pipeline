from typing import Dict, Any, List
import pymongo
from config import settings
from src import mongo_setup

INDEX_DEFINITIONS = [
    {
        "name": "idx_customer_id",
        "collection": settings.COLLECTION_VALIDATED,
        "keys": [("customer_id", pymongo.ASCENDING)],
        "type": "Single Field Index",
        "serves_query": "orders_by_customer",
        "description": "Accelerates customer order history lookups from COLLSCAN to IXSCAN.",
        "unique": False
    },
    {
        "name": "idx_quarantine_error_codes",
        "collection": settings.COLLECTION_QUARANTINE,
        "keys": [("error_codes", pymongo.ASCENDING)],
        "type": "Multikey Index (Array Field)",
        "serves_query": "quarantine_records_by_error",
        "description": "Accelerates quarantine error triage by indexing the error_codes array.",
        "unique": False
    },
    {
        "name": "idx_city_status",
        "collection": settings.COLLECTION_VALIDATED,
        "keys": [("city", pymongo.ASCENDING), ("status", pymongo.ASCENDING)],
        "type": "Compound Index",
        "serves_query": "orders_by_city_and_status",
        "description": "Optimizes combined city and order status filtering for logistics management.",
        "unique": False
    }
]

def get_index_definitions() -> List[Dict[str, Any]]:
    """Returns definitions and metadata for all Phase 2 indexes."""
    return INDEX_DEFINITIONS

def create_phase2_indexes(db=None) -> Dict[str, Any]:
    """
    Creates the required Phase 2 indexes idempotently.
    Checks existing indexes before creation to prevent redundant index builds.
    Returns:
        Summary dict containing created, existing, and error lists.
    """
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    created = []
    already_existing = []
    errors = []
    
    try:
        for idx in INDEX_DEFINITIONS:
            coll_name = idx["collection"]
            coll = db[coll_name]
            idx_name = idx["name"]
            keys = idx["keys"]
            
            # Check existing indexes on the collection
            existing_info = coll.index_information()
            if idx_name in existing_info:
                already_existing.append({
                    "name": idx_name,
                    "collection": coll_name,
                    "status": "already_exists"
                })
            else:
                try:
                    coll.create_index(keys, name=idx_name, background=True)
                    created.append({
                        "name": idx_name,
                        "collection": coll_name,
                        "keys": keys,
                        "type": idx["type"],
                        "status": "created"
                    })
                except Exception as e:
                    errors.append({
                        "name": idx_name,
                        "collection": coll_name,
                        "error": str(e)
                    })
                    
        return {
            "status": "success" if not errors else "partial_success",
            "created_count": len(created),
            "existing_count": len(already_existing),
            "created": created,
            "already_existing": already_existing,
            "errors": errors
        }
    finally:
        if close_client:
            client.close()

def drop_phase2_indexes(db=None) -> Dict[str, Any]:
    """
    Drops Phase 2 indexes (primarily useful for Explain Before/After benchmarking).
    """
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    dropped = []
    try:
        for idx in INDEX_DEFINITIONS:
            coll = db[idx["collection"]]
            if idx["name"] in coll.index_information():
                coll.drop_index(idx["name"])
                dropped.append(idx["name"])
        return {"status": "success", "dropped": dropped}
    finally:
        if close_client:
            client.close()
