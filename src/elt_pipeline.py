import time
from typing import Dict, Any, List, Set
from config import settings
from src import mongo_setup
from src.batch_loader import load_csv_in_batches
from src.spark_loader import load_csv_with_spark
from src.cleaning.rule_registry import RuleRegistry
from src.validation.record_classifier import classify_and_tag_record
from src.upsert_writer import write_validated_records_bulk, write_quarantine_records_bulk

from src.incremental_loader import write_incremental_records_bulk
from src.spark_loader import spark_upsert_to_validated

def run_elt_pipeline(file_path: str, run_id: str, engine: str, file_size_mb: float, incremental: bool = False) -> Dict[str, Any]:
    """
    Coordinates the full ELT pipeline:
    1. Ingestion: Loads file to raw collection based on engine.
    2. Processing: Streams raw records, cleans them, classifies them, and bulk writes.
    3. Return metrics.
    """
    start_time = time.time()
    
    # Setup MongoDB collections and indexes
    mongo_setup.setup_mongodb()
    
    # --- PHASE 1: Raw Load ---
    print(f"[{engine.upper()}] Starting Ingestion Phase...")
    raw_loaded = 0
    partitions = None
    
    if engine == "python_batch":
        raw_loaded = load_csv_in_batches(file_path, run_id)
        partitions = 1
    elif engine == "pyspark":
        spark_results = load_csv_with_spark(file_path, run_id)
        raw_loaded = spark_results.get("raw_loaded", 0)
        partitions = spark_results.get("partitions", 1)
    else:
        raise ValueError(f"Unknown engine: {engine}")
        
    if raw_loaded == 0:
        print("No raw records ingested. Exiting pipeline.")
        return {}
        
    # --- PHASE 2 & 3: Clean, Validate, and Final Load ---
    if incremental:
        print("Starting Processing and Validation Phase (Incremental Load with Version Handling)...")
    else:
        print("Starting Processing and Validation Phase (Chunked Streaming Upsert)...")
    
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    raw_collection = db[settings.COLLECTION_RAW]
    
    # Registry of rules
    registry = RuleRegistry()
    
    # Track order_ids seen in this run to detect duplicates
    seen_order_ids: Set[str] = set()
    
    # Metric counters
    valid_count = 0
    corrected_count = 0
    quarantine_count = 0
    error_case_counts: Dict[str, int] = {}
    
    total_inserted = 0
    total_updated = 0
    total_unchanged = 0
    
    # Process raw records in memory-safe chunks
    chunk_size = 5000
    validated_chunk = []
    quarantine_chunk = []
    
    # Query raw documents for current run_id
    cursor = raw_collection.find({"run_id": run_id}, no_cursor_timeout=True)
    
    # Select appropriate writer based on engine and incremental flag:
    # - PySpark engine: uses spark_upsert_to_validated with Idempotency,
    #   Stable Business Key, and optional Version Protection
    # - Python Batch engine: uses existing upsert_writer / incremental_loader
    if engine == "pyspark":
        if incremental:
            validated_writer = lambda records: spark_upsert_to_validated(records, use_version_protection=True)
        else:
            validated_writer = lambda records: spark_upsert_to_validated(records, use_version_protection=False)
    else:
        validated_writer = write_incremental_records_bulk if incremental else write_validated_records_bulk
    
    try:
        processed_count = 0
        for doc in cursor:
            raw_record = doc.get("raw_record", {})
            source_row_number = doc.get("source_row_number", 0)
            ingested_at = doc.get("ingested_at")
            engine_used = doc.get("engine_used")
            
            # Run cleaning rules
            cleaned_rec, corrections = registry.clean_record(raw_record)
            
            # Map raw fields to root of cleaned record for database writing,
            # but keep operations properties.
            processed_rec = cleaned_rec.copy()
            processed_rec["run_id"] = run_id
            processed_rec["source_file"] = file_path
            processed_rec["source_row_number"] = source_row_number
            processed_rec["ingested_at"] = ingested_at
            processed_rec["engine_used"] = engine_used
            
            # Classify and tag record
            final_rec, status = classify_and_tag_record(
                processed_rec, 
                raw_record, 
                corrections, 
                seen_order_ids
            )
            
            # Collect metric states
            if status == "quarantined":
                quarantine_count += 1
                quarantine_chunk.append(final_rec)
                # Count individual error codes
                for err_code in final_rec.get("error_codes", []):
                    error_case_counts[err_code] = error_case_counts.get(err_code, 0) + 1
            elif status == "corrected":
                corrected_count += 1
                validated_chunk.append(final_rec)
            else:
                valid_count += 1
                validated_chunk.append(final_rec)
                
            # If chunk is full, write to MongoDB
            if len(validated_chunk) >= chunk_size:
                ins, upd, unc = validated_writer(validated_chunk)
                total_inserted += ins
                total_updated += upd
                total_unchanged += unc
                validated_chunk = []
                
            if len(quarantine_chunk) >= chunk_size:
                write_quarantine_records_bulk(quarantine_chunk)
                quarantine_chunk = []
                
            processed_count += 1
            if processed_count % 10000 == 0:
                print(f"  Processed {processed_count} records (Valid: {valid_count}, Corrected: {corrected_count}, Quarantine: {quarantine_count})...")
                
        # Write any remaining documents in final chunks
        if validated_chunk:
            ins, upd, unc = validated_writer(validated_chunk)
            total_inserted += ins
            total_updated += upd
            total_unchanged += unc
            
        if quarantine_chunk:
            write_quarantine_records_bulk(quarantine_chunk)
            
    finally:
        cursor.close()
        client.close()
        
    elapsed_seconds = time.time() - start_time
    throughput = raw_loaded / elapsed_seconds if elapsed_seconds > 0 else 0
    
    # Compile execution metrics
    metrics = {
        "run_id": run_id,
        "file_name": file_path.split("/")[-1],
        "file_size_mb": file_size_mb,
        "engine_used": engine,
        "rows_read": raw_loaded,
        "raw_loaded": raw_loaded,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "elapsed_seconds": elapsed_seconds,
        "throughput": throughput,
        "batch_size": settings.BATCH_SIZE if engine == "python_batch" else None,
        "partitions": partitions,
        "error_case_counts": error_case_counts,
        "inserted_count": total_inserted,
        "updated_count": total_updated,
        "unchanged_count": total_unchanged
    }
    
    return metrics
