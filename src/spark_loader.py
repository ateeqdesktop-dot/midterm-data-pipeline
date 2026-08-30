import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, List, Tuple

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, current_timestamp, input_file_name, monotonically_increasing_id
import pymongo
from pymongo import UpdateOne
from config import settings
from src import mongo_setup

# Configure PySpark environment variables to run workers using the virtual environment python interpreter
venv_python = sys.executable
os.environ["PYSPARK_PYTHON"] = venv_python
os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python

def build_spark_session() -> SparkSession:
    """Builds a SparkSession with MongoDB Spark Connector configurations."""
    # We include both the standard spark mongo connector package and some dependencies
    mongo_connector_pkg = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    project_root = str(settings.BASE_DIR)
    
    # Configure Spark Session
    builder = SparkSession.builder \
        .appName(settings.SPARK_APP_NAME) \
        .master(settings.SPARK_MASTER) \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.mongodb.read.connection.uri", f"{settings.MONGO_URI}/{settings.MONGO_DB_NAME}") \
        .config("spark.mongodb.write.connection.uri", f"{settings.MONGO_URI}/{settings.MONGO_DB_NAME}") \
        .config("spark.jars.packages", mongo_connector_pkg) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executorEnv.PYTHONPATH", project_root) \
        .config("spark.executorEnv.PYSPARK_PYTHON", venv_python) \
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", venv_python) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        
    return builder.getOrCreate()

def load_csv_with_spark(file_path: str, run_id: str) -> Dict[str, Any]:
    """
    Loads a large CSV file using Spark DataFrame API, adds metadata columns,
    and writes the raw data to MongoDB in parallel.
    """
    start_time = time.time()
    print("Starting PySpark Loader...")
    print(f"  Source File: {file_path}")
    print(f"  Run ID: {run_id}")
    print(f"  Spark Master: {settings.SPARK_MASTER}")
    
    spark = build_spark_session()
    
    # Define a strict schema: read all columns as StringType to prevent automatic
    # parsing/truncation of dirty values (as required by section 6.4 of PDF spec).
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("delivery_type", StringType(), True),
        StructField("delivery_cost", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("items_json", StringType(), True)
    ])
    
    try:
        # Read the CSV file
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("encoding", "utf-8") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .schema(schema) \
            .load(file_path)
            
        input_partitions = df.rdd.getNumPartitions()
        print(f"  Input Partitions count: {input_partitions}")
        
        # Select all columns to wrap into raw_record
        columns = df.columns
        
        # Add metadata columns matching ELT format requirements (section 6.5)
        # We construct a struct containing all the raw fields, and add the wrapper fields.
        from pyspark.sql.functions import struct
        
        df_raw = df.withColumn("run_id", lit(run_id)) \
                   .withColumn("source_file", lit(file_path)) \
                   .withColumn("source_row_number", monotonically_increasing_id() + 1) \
                   .withColumn("ingested_at", current_timestamp()) \
                   .withColumn("engine_used", lit("pyspark")) \
                   .withColumn("raw_record", struct([col(c) for c in columns]))
                   
        # Project only the required ELT columns
        df_raw = df_raw.select("run_id", "source_file", "source_row_number", "ingested_at", "engine_used", "raw_record")
        
        # Write raw dataframe to MongoDB
        print(f"  Writing raw data to MongoDB '{settings.COLLECTION_RAW}' collection across {input_partitions} partitions in parallel...")
        
        def write_partition_to_mongo(partition_iterator):
            """Writes raw records using idempotent upsert keyed on (run_id, source_row_number)."""
            import sys
            from pathlib import Path
            base_dir = str(Path(__file__).resolve().parent.parent)
            if base_dir not in sys.path:
                sys.path.insert(0, base_dir)
            from config import settings
            from src import mongo_setup
            
            client = mongo_setup.get_mongo_client()
            db = mongo_setup.get_database(client)
            collection = db[settings.COLLECTION_RAW]
            
            batch = []
            for row in partition_iterator:
                doc = {
                    "run_id": row.run_id,
                    "source_file": row.source_file,
                    "source_row_number": int(row.source_row_number),
                    "ingested_at": row.ingested_at if row.ingested_at else datetime.utcnow(),
                    "engine_used": row.engine_used,
                    "raw_record": row.raw_record.asDict() if row.raw_record else {}
                }
                # Idempotency: use upsert keyed on (run_id, source_row_number)
                # so re-runs of the same run_id skip already-inserted rows.
                match_filter = {
                    "run_id": doc["run_id"],
                    "source_row_number": doc["source_row_number"]
                }
                batch.append(UpdateOne(match_filter, {"$setOnInsert": doc}, upsert=True))
                if len(batch) >= 5000:
                    collection.bulk_write(batch, ordered=False)
                    batch = []
            if batch:
                collection.bulk_write(batch, ordered=False)
            client.close()
            
        df_raw.foreachPartition(write_partition_to_mongo)
        print("  Parallel Spark partition write completed successfully.")
            
        # Count rows written
        db_client = mongo_setup.get_mongo_client()
        db = mongo_setup.get_database(db_client)
        rows_loaded = db[settings.COLLECTION_RAW].count_documents({"run_id": run_id})
        db_client.close()
        
        elapsed_seconds = time.time() - start_time
        throughput = rows_loaded / elapsed_seconds if elapsed_seconds > 0 else 0
        
        print("PySpark Loading finished successfully.")
        print(f"  Rows Loaded: {rows_loaded}")
        print(f"  Time Taken: {elapsed_seconds:.2f} seconds")
        print(f"  Throughput: {throughput:.1f} records/second")
        print("-" * 50)
        
        return {
            "rows_read": rows_loaded,
            "raw_loaded": rows_loaded,
            "elapsed_seconds": elapsed_seconds,
            "throughput": throughput,
            "partitions": input_partitions
        }
        
    except Exception as e:
        print(f"Fatal error during PySpark Loading: {e}")
        raise
    finally:
        try:
            spark.stop()
        except:
            pass


def spark_upsert_to_validated(records: List[Dict[str, Any]], use_version_protection: bool = False) -> Tuple[int, int, int]:
    """
    Writes validated records to orders_validated using Spark-driven
    idempotent bulk Upsert with Stable Business Key (order_id).
    
    Designed to be compatible with the existing upsert_writer.py and
    incremental_loader.py patterns, but called from PySpark partition context.
    
    Features:
        - Idempotency: Uses order_id as unique match key (Unique Index enforced in mongo_setup)
        - Version Protection: Optionally skips older records based on order_date comparison
        - Stable Business Key: order_id is standardized (str + strip) before matching
        - Upsert: New records are inserted via $setOnInsert, existing records updated via $set
    
    Args:
        records: List of validated record dicts to write.
        use_version_protection: If True, compare order_date before overwriting (incremental mode).
    
    Returns:
        Tuple[inserted_count, updated_count, unchanged_count]
    """
    if not records:
        return 0, 0, 0
    
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    collection = db[settings.COLLECTION_VALIDATED]
    
    inserted = 0
    updated = 0
    unchanged = 0
    
    if use_version_protection:
        # --- Version Protection path (matches incremental_loader.py logic) ---
        # Fetch existing order_dates for all order_ids in this batch
        order_ids = [str(r["order_id"]).strip() for r in records if r.get("order_id")]
        existing_docs = collection.find(
            {"order_id": {"$in": order_ids}},
            {"order_id": 1, "order_date": 1}
        )
        existing_dates = {}
        for doc in existing_docs:
            oid = doc.get("order_id")
            odate = doc.get("order_date")
            if oid and odate:
                existing_dates[oid] = odate
        
        operations = []
        for rec in records:
            order_id = str(rec.get("order_id", "")).strip()
            if not order_id:
                continue
            
            new_date_str = rec.get("order_date")
            match_query = {"order_id": order_id}
            
            if order_id in existing_dates:
                old_date_str = existing_dates[order_id]
                # Version check: skip if incoming record is older than existing
                if new_date_str and old_date_str and new_date_str < old_date_str:
                    unchanged += 1
                    continue
                # Incoming is newer or equal — update
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
            else:
                # New record — insert via upsert
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
        
        if operations:
            try:
                result = collection.bulk_write(operations, ordered=False)
                inserted = result.upserted_count
                actual_modified = result.modified_count
                actual_unchanged = max(0, result.matched_count - actual_modified)
                updated = actual_modified
                unchanged += actual_unchanged
            except pymongo.errors.BulkWriteError as bwe:
                print(f"  [ERROR] Bulk Write Error during spark version-protected upsert: {bwe.details}")
                inserted = bwe.details.get("nUpserted", 0)
                updated = bwe.details.get("nModified", 0)
                matched = bwe.details.get("nMatched", 0)
                unchanged += max(0, matched - updated)
    else:
        # --- Standard Idempotent Upsert path (matches upsert_writer.py logic) ---
        operations = []
        for rec in records:
            order_id = str(rec.get("order_id", "")).strip()
            if not order_id:
                continue
            
            # Stable Business Key: standardized order_id
            match_query = {"order_id": order_id}
            
            # Separate system ingestion fields ($setOnInsert) from data fields ($set)
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
        
        if operations:
            try:
                result = collection.bulk_write(operations, ordered=False)
                inserted = result.upserted_count
                updated = result.modified_count
                unchanged = max(0, result.matched_count - result.modified_count)
            except pymongo.errors.BulkWriteError as bwe:
                print(f"  [ERROR] Bulk Write Error during spark idempotent upsert: {bwe.details}")
                inserted = bwe.details.get("nUpserted", 0)
                updated = bwe.details.get("nModified", 0)
                matched = bwe.details.get("nMatched", 0)
                unchanged = max(0, matched - updated)
    
    client.close()
    return inserted, updated, unchanged
