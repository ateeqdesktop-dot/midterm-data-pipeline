from typing import List, Dict, Any, Tuple
import pymongo
from pymongo import UpdateOne
from config import settings
from src import mongo_setup

def write_validated_records_bulk(records: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """
    Writes processed valid/corrected records to the validated collection
    using idempotent bulk upsert.
    Returns:
        Tuple[inserted_count, updated_count, unchanged_count]
    """
    if not records:
        return 0, 0, 0
        
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    collection = db[settings.COLLECTION_VALIDATED]
    
    operations = []
    for rec in records:
        order_id = rec.get("order_id")
        if not order_id:
            continue
            
        # Standardize order_id for match query
        match_query = {"order_id": str(order_id).strip()}
        
        # We separate system ingestion fields ($setOnInsert) from normal data ($set)
        # to preserve original ingested time.
        update_doc = {
            "$setOnInsert": {
                "ingested_at": rec.get("ingested_at"),
                "engine_used": rec.get("engine_used"),
                "source_file": rec.get("source_file"),
            },
            "$set": {
                "run_id": rec.get("run_id"),
                "order_date": rec.get("order_date"),
                "status": rec.get("status"),
                "customer_id": rec.get("customer_id"),
                "customer_name": rec.get("customer_name"),
                "customer_phone": rec.get("customer_phone"),
                "customer_email": rec.get("customer_email"),
                "city": rec.get("city"),
                "district": rec.get("district"),
                "delivery_type": rec.get("delivery_type"),
                "delivery_cost": rec.get("delivery_cost"),
                "payment_method": rec.get("payment_method"),
                "payment_status": rec.get("payment_status"),
                "payment_amount": rec.get("payment_amount"),
                "currency": rec.get("currency"),
                "total_amount": rec.get("total_amount"),
                "items_json": rec.get("items_json"),
                "quality_status": rec.get("quality_status"),
                "corrections": rec.get("corrections", [])
            }
        }
        
        operations.append(UpdateOne(match_query, update_doc, upsert=True))
        
    inserted = 0
    updated = 0
    unchanged = 0
    
    if operations:
        try:
            result = collection.bulk_write(operations, ordered=False)
            inserted = result.upserted_count
            updated = result.modified_count
            # matched but not modified = unchanged
            unchanged = max(0, result.matched_count - result.modified_count)
        except pymongo.errors.BulkWriteError as bwe:
            print(f"  [ERROR] Bulk Write Error during validated upsert: {bwe.details}")
            # In case of partial failures, we extract counts from details
            inserted = bwe.details.get("nUpserted", 0)
            updated = bwe.details.get("nModified", 0)
            matched = bwe.details.get("nMatched", 0)
            unchanged = max(0, matched - updated)
            
    client.close()
    return inserted, updated, unchanged

def write_quarantine_records_bulk(records: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """
    Writes quarantined records to the quarantine collection.
    Returns:
        Tuple[inserted_count, updated_count, unchanged_count]
    """
    if not records:
        return 0, 0, 0
        
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    collection = db[settings.COLLECTION_QUARANTINE]
    
    try:
        result = collection.insert_many(records, ordered=False)
        inserted = len(result.inserted_ids)
    except pymongo.errors.BulkWriteError as bwe:
        inserted = bwe.details.get("nInserted", 0)
    except Exception as e:
        print(f"  [ERROR] Quarantine write failed: {e}")
        inserted = 0
        
    client.close()
    return inserted, 0, 0
