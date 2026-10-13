from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import pymongo
from pymongo import UpdateOne
from config import settings
from src import mongo_setup
from src.utils.json_encoder import serialize_mongo_doc

MV_DAILY_SALES = "mv_daily_sales_summary"
MV_CITY_PERFORMANCE = "mv_city_performance_summary"
MV_METADATA_COLL = "mv_metadata"

MV_CATALOG = {
    "daily_sales_summary": {
        "name": "daily_sales_summary",
        "collection": MV_DAILY_SALES,
        "description": "Pre-aggregated daily sales metrics by date and city.",
        "unique_keys": [("date", 1), ("city", 1)]
    },
    "city_performance_summary": {
        "name": "city_performance_summary",
        "collection": MV_CITY_PERFORMANCE,
        "description": "Pre-aggregated all-time performance metrics grouped by city.",
        "unique_keys": [("city", 1)]
    }
}

def get_mv_catalog() -> List[Dict[str, Any]]:
    """Returns metadata for all configured materialized views."""
    return list(MV_CATALOG.values())

def setup_mv_collections(db=None):
    """Ensures unique indexes on materialized view collections."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        # 1. Unique index on daily sales: date + city
        db[MV_DAILY_SALES].create_index(
            [("date", pymongo.ASCENDING), ("city", pymongo.ASCENDING)],
            unique=True,
            name="idx_mv_date_city_unique"
        )
        # 2. Unique index on city performance: city
        db[MV_CITY_PERFORMANCE].create_index(
            [("city", pymongo.ASCENDING)],
            unique=True,
            name="idx_mv_city_unique"
        )
        # 3. Metadata index
        db[MV_METADATA_COLL].create_index([("view_name", pymongo.ASCENDING)], unique=True)
    finally:
        if close_client:
            client.close()

def get_mv_watermark(view_name: str, db=None) -> Optional[datetime]:
    """Retrieves last synced watermark for a given view."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        doc = db[MV_METADATA_COLL].find_one({"view_name": view_name})
        if doc and "last_synced_at" in doc:
            return doc["last_synced_at"]
        return None
    finally:
        if close_client:
            client.close()

def update_mv_watermark(view_name: str, sync_time: datetime, db=None):
    """Updates watermark timestamp in metadata collection."""
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        db[MV_METADATA_COLL].update_one(
            {"view_name": view_name},
            {"$set": {"view_name": view_name, "last_synced_at": sync_time, "updated_at": datetime.now(timezone.utc)}},
            upsert=True
        )
    finally:
        if close_client:
            client.close()

def initial_build_daily_sales(sample_days: Optional[int] = None, db=None) -> int:
    """
    Performs initial build for daily_sales_summary.
    Aggregates orders_validated and upserts into mv_daily_sales_summary.
    """
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        setup_mv_collections(db)
        coll_val = db[settings.COLLECTION_VALIDATED]
        
        pipeline = [
            {
                "$project": {
                    "date": {"$substrCP": ["$order_date", 0, 10]},
                    "city": 1,
                    "amount": {"$toDouble": "$total_amount"},
                    "delivery_cost": {"$toDouble": "$delivery_cost"},
                    "status": 1
                }
            },
            {
                "$group": {
                    "_id": {
                        "date": "$date",
                        "city": "$city"
                    },
                    "total_revenue": {"$sum": "$amount"},
                    "order_count": {"$sum": 1},
                    "total_delivery_cost": {"$sum": "$delivery_cost"}
                }
            }
        ]
        
        cursor = coll_val.aggregate(pipeline, allowDiskUse=True)
        ops = []
        count = 0
        sync_now = datetime.now(timezone.utc)
        
        for doc in cursor:
            date_val = doc["_id"]["date"]
            city_val = doc["_id"]["city"]
            tot_rev = round(doc.get("total_revenue", 0.0), 2)
            ord_cnt = doc.get("order_count", 0)
            avg_val = round(tot_rev / ord_cnt, 2) if ord_cnt > 0 else 0.0
            
            update_doc = {
                "$set": {
                    "date": date_val,
                    "city": city_val,
                    "total_revenue": tot_rev,
                    "order_count": ord_cnt,
                    "avg_order_value": avg_val,
                    "total_delivery_cost": round(doc.get("total_delivery_cost", 0.0), 2),
                    "last_refreshed_at": sync_now
                }
            }
            ops.append(UpdateOne({"date": date_val, "city": city_val}, update_doc, upsert=True))
            if len(ops) >= 1000:
                db[MV_DAILY_SALES].bulk_write(ops, ordered=False)
                count += len(ops)
                ops = []
                
        if ops:
            db[MV_DAILY_SALES].bulk_write(ops, ordered=False)
            count += len(ops)
            
        update_mv_watermark("daily_sales_summary", sync_now, db=db)
        return count
    finally:
        if close_client:
            client.close()

def initial_build_city_performance(db=None) -> int:
    """
    Performs initial build for city_performance_summary.
    """
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        setup_mv_collections(db)
        coll_val = db[settings.COLLECTION_VALIDATED]
        
        pipeline = [
            {
                "$project": {
                    "city": 1,
                    "amount": {"$toDouble": "$total_amount"},
                    "delivery_cost": {"$toDouble": "$delivery_cost"}
                }
            },
            {
                "$group": {
                    "_id": "$city",
                    "total_revenue": {"$sum": "$amount"},
                    "order_count": {"$sum": 1},
                    "total_delivery_cost": {"$sum": "$delivery_cost"}
                }
            }
        ]
        
        cursor = coll_val.aggregate(pipeline, allowDiskUse=True)
        ops = []
        count = 0
        sync_now = datetime.now(timezone.utc)
        
        for doc in cursor:
            city_val = doc["_id"]
            tot_rev = round(doc.get("total_revenue", 0.0), 2)
            ord_cnt = doc.get("order_count", 0)
            avg_val = round(tot_rev / ord_cnt, 2) if ord_cnt > 0 else 0.0
            
            update_doc = {
                "$set": {
                    "city": city_val,
                    "total_revenue": tot_rev,
                    "order_count": ord_cnt,
                    "avg_order_value": avg_val,
                    "total_delivery_cost": round(doc.get("total_delivery_cost", 0.0), 2),
                    "last_refreshed_at": sync_now
                }
            }
            ops.append(UpdateOne({"city": city_val}, update_doc, upsert=True))
            if len(ops) >= 1000:
                db[MV_CITY_PERFORMANCE].bulk_write(ops, ordered=False)
                count += len(ops)
                ops = []
                
        if ops:
            db[MV_CITY_PERFORMANCE].bulk_write(ops, ordered=False)
            count += len(ops)
            
        update_mv_watermark("city_performance_summary", sync_now, db=db)
        return count
    finally:
        if close_client:
            client.close()

def refresh_materialized_views(mode: str = "incremental", db=None) -> Dict[str, Any]:
    """
    Refreshes materialized views using either 'incremental' or 'full' strategy.
    
    Incremental Strategy:
      1. Reads last_synced_at watermark from mv_metadata.
      2. If never built, triggers initial build automatically.
      3. Queries orders_validated for records where ingested_at > last_synced_at.
      4. If no new records found: safely returns 'up_to_date' with zero recalculation.
      5. If new records found: identifies affected partitions (date, city),
         re-aggregates ONLY those specific partitions, and upserts them into MVs.
      6. Advances the watermark.
    """
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        setup_mv_collections(db)
        coll_val = db[settings.COLLECTION_VALIDATED]
        
        # Check watermarks and synced runs
        daily_wm = get_mv_watermark("daily_sales_summary", db=db)
        city_wm = get_mv_watermark("city_performance_summary", db=db)
        
        # Retrieve already synced run_ids
        meta_doc = db[MV_METADATA_COLL].find_one({"view_name": "daily_sales_summary"})
        synced_runs = set(meta_doc.get("synced_run_ids", [])) if meta_doc else set()
        
        # If never built or mode is full, run initial full build
        if mode == "full" or daily_wm is None or city_wm is None:
            print("Running initial full build of Materialized Views...")
            d_count = initial_build_daily_sales(db=db)
            c_count = initial_build_city_performance(db=db)
            # Store all existing distinct run_ids
            existing_runs = coll_val.distinct("run_id")
            db[MV_METADATA_COLL].update_one(
                {"view_name": "daily_sales_summary"},
                {"$set": {"synced_run_ids": existing_runs}},
                upsert=True
            )
            return {
                "status": "success",
                "mode": "full",
                "message": "Full initial build of materialized views completed.",
                "daily_sales_rows": d_count,
                "city_performance_rows": c_count,
                "refreshed_at": datetime.now(timezone.utc).isoformat()
            }
            
        # INCREMENTAL MODE:
        # Detect new runs using run_id index and metadata
        all_runs = db[settings.COLLECTION_RAW].distinct("run_id")
        unsynced_runs = [r for r in all_runs if r not in synced_runs]
        
        # If no unsynced runs and no watermark advance needed
        if not unsynced_runs:
            return {
                "status": "up_to_date",
                "mode": "incremental",
                "message": "Materialized views are completely up to date. No new records or runs detected.",
                "new_records_detected": 0,
                "affected_daily_partitions_updated": 0,
                "affected_cities_updated": 0,
                "refreshed_at": datetime.now(timezone.utc).isoformat()
            }
            
        # Fast query on indexed run_id
        new_docs_cursor = coll_val.find(
            {"run_id": {"$in": unsynced_runs}},
            {"_id": 0, "order_date": 1, "city": 1, "run_id": 1}
        )
        
        affected_daily_partitions = set()
        affected_cities = set()
        new_runs_discovered = set(unsynced_runs)
        total_new_docs = 0
        
        for doc in new_docs_cursor:
            total_new_docs += 1
            odate = doc.get("order_date")
            city = doc.get("city")
            if odate and city:
                date_part = odate[:10]
                affected_daily_partitions.add((date_part, city))
                affected_cities.add(city)
                
        # Safe check: No new data!
        if total_new_docs == 0:
            return {
                "status": "up_to_date",
                "mode": "incremental",
                "message": "Materialized views are completely up to date. No new records detected.",
                "new_records_detected": 0,
                "affected_daily_partitions_updated": 0,
                "affected_cities_updated": 0,
                "refreshed_at": datetime.now(timezone.utc).isoformat()
            }
            
        # Recalculate ONLY affected partitions for daily_sales_summary in a single batched aggregation
        sync_now = datetime.now(timezone.utc)
        daily_ops = []
        
        dates_list = sorted([d for d, c in affected_daily_partitions if d])
        cities_list = list(affected_cities)
        
        if dates_list and cities_list:
            pipeline_daily = [
                {
                    "$match": {
                        "city": {"$in": cities_list},
                        "order_date": {
                            "$gte": f"{dates_list[0]}T00:00:00",
                            "$lte": f"{dates_list[-1]}T23:59:59"
                        }
                    }
                },
                {
                    "$project": {
                        "date": {"$substrCP": ["$order_date", 0, 10]},
                        "city": 1,
                        "amount": {"$toDouble": "$total_amount"},
                        "delivery_cost": {"$toDouble": "$delivery_cost"}
                    }
                },
                {
                    "$group": {
                        "_id": {"date": "$date", "city": "$city"},
                        "total_revenue": {"$sum": "$amount"},
                        "order_count": {"$sum": 1},
                        "total_delivery_cost": {"$sum": "$delivery_cost"}
                    }
                }
            ]
            agg_daily = list(coll_val.aggregate(pipeline_daily, allowDiskUse=True))
            for r in agg_daily:
                d_val = r["_id"]["date"]
                c_val = r["_id"]["city"]
                tot_rev = round(r.get("total_revenue", 0.0), 2)
                ord_cnt = r.get("order_count", 0)
                avg_val = round(tot_rev / ord_cnt, 2) if ord_cnt > 0 else 0.0
                
                daily_ops.append(UpdateOne(
                    {"date": d_val, "city": c_val},
                    {"$set": {
                        "date": d_val,
                        "city": c_val,
                        "total_revenue": tot_rev,
                        "order_count": ord_cnt,
                        "avg_order_value": avg_val,
                        "total_delivery_cost": round(r.get("total_delivery_cost", 0.0), 2),
                        "last_refreshed_at": sync_now
                    }},
                    upsert=True
                ))
                
        if daily_ops:
            db[MV_DAILY_SALES].bulk_write(daily_ops, ordered=False)
            
        # Recalculate ONLY affected cities for city_performance_summary in a single batched aggregation
        city_ops = []
        if cities_list:
            pipeline_city = [
                {"$match": {"city": {"$in": cities_list}}},
                {
                    "$project": {
                        "city": 1,
                        "amount": {"$toDouble": "$total_amount"},
                        "delivery_cost": {"$toDouble": "$delivery_cost"}
                    }
                },
                {
                    "$group": {
                        "_id": "$city",
                        "total_revenue": {"$sum": "$amount"},
                        "order_count": {"$sum": 1},
                        "total_delivery_cost": {"$sum": "$delivery_cost"}
                    }
                }
            ]
            agg_city = list(coll_val.aggregate(pipeline_city, allowDiskUse=True))
            for r in agg_city:
                c_val = r["_id"]
                tot_rev = round(r.get("total_revenue", 0.0), 2)
                ord_cnt = r.get("order_count", 0)
                avg_val = round(tot_rev / ord_cnt, 2) if ord_cnt > 0 else 0.0
                
                city_ops.append(UpdateOne(
                    {"city": c_val},
                    {"$set": {
                        "city": c_val,
                        "total_revenue": tot_rev,
                        "order_count": ord_cnt,
                        "avg_order_value": avg_val,
                        "total_delivery_cost": round(r.get("total_delivery_cost", 0.0), 2),
                        "last_refreshed_at": sync_now
                    }},
                    upsert=True
                ))
                
        if city_ops:
            db[MV_CITY_PERFORMANCE].bulk_write(city_ops, ordered=False)
            
        # Update watermarks and synced runs
        update_mv_watermark("daily_sales_summary", sync_now, db=db)
        update_mv_watermark("city_performance_summary", sync_now, db=db)
        if new_runs_discovered:
            updated_runs = list(synced_runs.union(new_runs_discovered))
            db[MV_METADATA_COLL].update_one(
                {"view_name": "daily_sales_summary"},
                {"$set": {"synced_run_ids": updated_runs}},
                upsert=True
            )
        
        return {
            "status": "success",
            "mode": "incremental",
            "message": "Incremental refresh executed successfully on affected partitions.",
            "new_records_detected": total_new_docs,
            "affected_daily_partitions_updated": len(daily_ops),
            "affected_cities_updated": len(city_ops),
            "refreshed_at": sync_now.isoformat()
        }
        
    finally:
        if close_client:
            client.close()

def query_materialized_view(view_name: str, limit: int = 50, filter_query: Optional[Dict[str, Any]] = None, db=None) -> List[Dict[str, Any]]:
    """Fast O(1)/indexed query against pre-computed materialized view."""
    if view_name not in MV_CATALOG:
        raise KeyError(f"Materialized view '{view_name}' not found. Available views: {list(MV_CATALOG.keys())}")
        
    if db is None:
        client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(client)
        close_client = True
    else:
        close_client = False
        
    try:
        coll_name = MV_CATALOG[view_name]["collection"]
        coll = db[coll_name]
        query = filter_query or {}
        cursor = coll.find(query, {"_id": 0}).limit(limit)
        return [serialize_mongo_doc(doc) for doc in cursor]
    finally:
        if close_client:
            client.close()
