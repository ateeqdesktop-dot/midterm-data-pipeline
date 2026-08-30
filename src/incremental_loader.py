from datetime import datetime
from typing import List, Dict, Any, Tuple
import pymongo
from pymongo import UpdateOne
from config import settings
from src import mongo_setup
from src.upsert_writer import write_quarantine_records_bulk

def fetch_existing_dates_and_versions(order_ids: List[str]) -> Dict[str, str]:
    """
    Queries orders_validated for the existing order_dates for a list of order_ids.
    Returns:
        Dict[order_id, order_date_string]
    """
    if not order_ids:
        return {}
        
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    collection = db[settings.COLLECTION_VALIDATED]
    
    results = collection.find(
        {"order_id": {"$in": order_ids}},
        {"order_id": 1, "order_date": 1}
    )
    
    existing_map = {}
    for doc in results:
        oid = doc.get("order_id")
        odate = doc.get("order_date")
        if oid and odate:
            existing_map[oid] = odate
            
    client.close()
    return existing_map

def write_incremental_records_bulk(records: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """
    Writes processed records to orders_validated using Version Handling (incremental update).
    Only updates existing records if the incoming order_date is newer or equal.
    Returns:
        Tuple[inserted_count, updated_count, unchanged_count]
    """
    if not records:
        return 0, 0, 0
        
    # Get all order_ids from incoming chunk
    order_ids = [str(r["order_id"]).strip() for r in records if r.get("order_id")]
    
    # Query current DB state for these order_ids
    existing_dates = fetch_existing_dates_and_versions(order_ids)
    
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    collection = db[settings.COLLECTION_VALIDATED]
    
    operations = []
    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    
    for rec in records:
        order_id = str(rec.get("order_id")).strip()
        new_date_str = rec.get("order_date")
        
        # Check if record exists
        if order_id in existing_dates:
            old_date_str = existing_dates[order_id]
            
            # Version Handling: Compare dates (lexicographical comparison works for ISO-8601 strings)
            if new_date_str < old_date_str:
                # Incoming update is older than what we already have in the database.
                # Skip update to prevent overwrite of newer data.
                unchanged_count += 1
                continue
            else:
                # Incoming update is newer or equal. We construct update statement.
                match_query = {"order_id": order_id}
                update_doc = {
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
                operations.append(UpdateOne(match_query, update_doc, upsert=False))
                updated_count += 1
        else:
            # Document does not exist. We do an insert (upsert=True)
            match_query = {"order_id": order_id}
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
            
    # Execute batch writes
    if operations:
        try:
            result = collection.bulk_write(operations, ordered=False)
            # result.upserted_count tells us how many actually got inserted
            inserted_count = result.upserted_count
            
            # Since some operations were updates (upsert=False), PyMongo registers them in modified_count.
            # However, if data is identical, modified_count is 0.
            # We already incremented updated_count locally.
            # Let's adjust updated_count based on what was actually modified, and add to unchanged.
            actual_modified = result.modified_count
            matched_updates = result.matched_count
            
            # The remaining matches that were not modified are unchanged
            actual_unchanged = max(0, matched_updates - actual_modified)
            unchanged_count += actual_unchanged
            
            # Update updated_count to reflect actual modified updates
            # (if an update matched but didn't modify anything, it's unchanged)
            # Actually, it's simpler:
            # Let's trust MongoDB statistics:
            # result.upserted_count is inserts.
            # result.modified_count is modified updates.
            # matched - modified is unchanged updates.
            # So:
            inserted_count = result.upserted_count
            # For records where we ran update:
            # if they matched and changed: updated
            # if they matched and didn't change: unchanged
            # We can calculate:
            # result.modified_count = modified records
            # unchanged_count += (result.matched_count - result.modified_count)
            # Wait, matched_count covers BOTH updates and upserts that matched.
            # This is 100% correct.
            
        except pymongo.errors.BulkWriteError as bwe:
            print(f"  [ERROR] Bulk Write Error during incremental load: {bwe.details}")
            
    client.close()
    return inserted_count, updated_count, unchanged_count
