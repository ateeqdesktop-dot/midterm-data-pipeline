import csv
import time
from datetime import datetime
from typing import Dict, List, Any
import pymongo
from config import settings
from src import mongo_setup

def load_csv_in_batches(file_path: str, run_id: str) -> int:
    """
    Reads a CSV file in a streaming fashion, chunks records into batches,
    and inserts them into the raw collection in MongoDB.
    """
    start_time = time.time()
    total_records = 0
    batch_count = 0
    
    print(f"Starting Python Batch Loader...")
    print(f"  Source File: {file_path}")
    print(f"  Run ID: {run_id}")
    print(f"  Batch Size: {settings.BATCH_SIZE}")
    
    # Establish MongoDB Connection
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    raw_collection = db[settings.COLLECTION_RAW]
    
    current_batch: List[Dict[str, Any]] = []
    
    try:
        with open(file_path, 'r', encoding='utf-8-sig', errors='replace') as f:
            # Using DictReader to map column names to values
            # If the CSV has a BOM (Byte Order Mark) like '\ufefforder_id', utf-8-sig handles it.
            reader = csv.DictReader(f)
            
            # Start streaming
            for i, row in enumerate(reader, start=1):
                # Clean row dictionary keys (strip any weird whitespace or characters)
                cleaned_row = {k.strip() if k else "": v for k, v in row.items()}
                
                # Construct raw ELT record structure
                raw_document = {
                    "run_id": run_id,
                    "source_file": file_path,
                    "source_row_number": i,
                    "ingested_at": datetime.utcnow(),
                    "engine_used": "python_batch",
                    "raw_record": cleaned_row
                }
                
                current_batch.append(raw_document)
                
                # Check if batch is full
                if len(current_batch) >= settings.BATCH_SIZE:
                    batch_count += 1
                    batch_start = time.time()
                    
                    try:
                        raw_collection.insert_many(current_batch, ordered=False)
                    except pymongo.errors.BulkWriteError as bwe:
                        # Log bulk write errors but continue
                        print(f"  [ERROR] Bulk Write Error in Batch {batch_count}: {bwe.details}")
                    except Exception as e:
                        print(f"  [ERROR] Failed to insert batch {batch_count}: {e}")
                        
                    batch_elapsed = time.time() - batch_start
                    total_records += len(current_batch)
                    
                    # Calculate throughput
                    elapsed_so_far = time.time() - start_time
                    throughput = total_records / elapsed_so_far if elapsed_so_far > 0 else 0
                    
                    print(
                        f"  Batch {batch_count:03d}: Inserted {len(current_batch)} records "
                        f"in {batch_elapsed:.3f}s | Cumulative: {total_records} | "
                        f"Rate: {throughput:.1f} rec/sec"
                    )
                    
                    # Reset batch
                    current_batch = []
                    
            # Insert remaining records
            if current_batch:
                batch_count += 1
                batch_start = time.time()
                try:
                    raw_collection.insert_many(current_batch, ordered=False)
                except pymongo.errors.BulkWriteError as bwe:
                    print(f"  [ERROR] Bulk Write Error in Final Batch: {bwe.details}")
                except Exception as e:
                    print(f"  [ERROR] Failed to insert final batch: {e}")
                
                batch_elapsed = time.time() - batch_start
                total_records += len(current_batch)
                
                elapsed_so_far = time.time() - start_time
                throughput = total_records / elapsed_so_far if elapsed_so_far > 0 else 0
                print(
                    f"  Final Batch {batch_count:03d}: Inserted {len(current_batch)} records "
                    f"in {batch_elapsed:.3f}s | Cumulative: {total_records} | "
                    f"Rate: {throughput:.1f} rec/sec"
                )
                
    except Exception as e:
        print(f"Fatal error during Python Batch streaming: {e}")
        raise
    finally:
        client.close()
        
    total_elapsed = time.time() - start_time
    overall_throughput = total_records / total_elapsed if total_elapsed > 0 else 0
    print(f"Python Batch Loading finished.")
    print(f"  Total Ingested: {total_records} records")
    print(f"  Total Time: {total_elapsed:.2f} seconds")
    print(f"  Average Speed: {overall_throughput:.1f} records/second")
    print("-" * 50)
    
    return total_records
