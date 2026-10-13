import pytest
import pymongo
import uuid
from datetime import datetime
from config import settings
from src import mongo_setup
from src.upsert_writer import write_validated_records_bulk
from src.incremental_loader import write_incremental_records_bulk

# Check if MongoDB is running locally
try:
    client = pymongo.MongoClient(settings.MONGO_URI, serverSelectionTimeoutMS=500)
    client.server_info()
    MONGO_AVAILABLE = True
    client.close()
except Exception:
    MONGO_AVAILABLE = False

@pytest.mark.skipif(not MONGO_AVAILABLE, reason="MongoDB is not running locally")
def test_idempotent_upsert_flow():
    """
    Verifies that upserting records:
    1. Adds new documents.
    2. Overwrites existing documents if they exist.
    3. Does not duplicate order documents in orders_validated.
    """
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    collection = db[settings.COLLECTION_VALIDATED]
    
    # Generate unique run ID for this test
    run_id = f"test_run_{uuid.uuid4().hex[:6]}"
    order_id = f"test_order_{uuid.uuid4().hex[:6]}"
    
    # Cleanup previous run
    collection.delete_one({"order_id": order_id})
    
    record = {
        "order_id": order_id,
        "run_id": run_id,
        "order_date": "2025-02-24T21:29:00",
        "customer_id": "customer-1",
        "customer_name": "محمد علي",
        "customer_phone": "+967771234567",
        "customer_email": "user@example.com",
        "city": "تعز",
        "district": "شعوب",
        "delivery_type": "سريع",
        "delivery_cost": "5000.0",
        "payment_method": "محفظة إلكترونية",
        "payment_status": "تم الدفع",
        "payment_amount": "769000.0",
        "currency": "YER",
        "total_amount": "769000.0",
        "items_json": "[]",
        "quality_status": "valid",
        "ingested_at": datetime.utcnow(),
        "engine_used": "python_batch",
        "source_file": "test.csv"
    }
    
    # 1. First run -> should insert 1
    ins, upd, unc = write_validated_records_bulk([record])
    assert ins == 1
    assert upd == 0
    assert unc == 0
    
    # Verify count
    assert collection.count_documents({"order_id": order_id}) == 1
    
    # 2. Second run with same data -> should be unchanged
    ins2, upd2, unc2 = write_validated_records_bulk([record])
    assert ins2 == 0
    assert upd2 == 0
    assert unc2 == 1
    
    # Verify count remains 1
    assert collection.count_documents({"order_id": order_id}) == 1
    
    # 3. Third run with updated data -> should be updated
    updated_record = record.copy()
    updated_record["customer_name"] = "علي حسين"
    ins3, upd3, unc3 = write_validated_records_bulk([updated_record])
    assert ins3 == 0
    assert upd3 == 1
    assert unc3 == 0
    
    # Verify content was updated
    doc = collection.find_one({"order_id": order_id})
    assert doc["customer_name"] == "علي حسين"
    assert collection.count_documents({"order_id": order_id}) == 1
    
    # Clean up
    collection.delete_one({"order_id": order_id})
    client.close()

@pytest.mark.skipif(not MONGO_AVAILABLE, reason="MongoDB is not running locally")
def test_incremental_version_handling():
    """
    Verifies that incremental loading respects versions (order_date).
    Only updates when order_date is newer or equal.
    """
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    collection = db[settings.COLLECTION_VALIDATED]
    
    run_id = f"test_run_{uuid.uuid4().hex[:6]}"
    order_id = f"test_order_{uuid.uuid4().hex[:6]}"
    collection.delete_one({"order_id": order_id})
    
    # Version 1: 2025-01-10
    record_v1 = {
        "order_id": order_id,
        "run_id": run_id,
        "order_date": "2025-01-10T12:00:00",
        "customer_id": "customer-1",
        "customer_name": "محمد علي",
        "total_amount": "1000.0",
        "currency": "YER",
        "items_json": "[]",
        "quality_status": "valid",
        "ingested_at": datetime.utcnow(),
        "engine_used": "python_batch",
        "source_file": "test.csv"
    }
    
    # Insert version 1
    ins, upd, unc = write_incremental_records_bulk([record_v1])
    assert ins == 1
    
    # Version 2: 2025-01-20 (newer) -> should update
    record_v2 = record_v1.copy()
    record_v2["order_date"] = "2025-01-20T12:00:00"
    record_v2["customer_name"] = "محمد علي الجديد"
    
    ins2, upd2, unc2 = write_incremental_records_bulk([record_v2])
    assert upd2 == 1 or unc2 == 0 # should perform update
    
    doc = collection.find_one({"order_id": order_id})
    assert doc["customer_name"] == "محمد علي الجديد"
    
    # Version 0: 2025-01-05 (older) -> should skip / be unchanged
    record_v0 = record_v1.copy()
    record_v0["order_date"] = "2025-01-05T12:00:00"
    record_v0["customer_name"] = "محمد علي القديم جدا"
    
    ins3, upd3, unc3 = write_incremental_records_bulk([record_v0])
    assert ins3 == 0
    assert upd3 == 0
    assert unc3 == 1 # skipped!
    
    # Verify content remained version 2
    doc_final = collection.find_one({"order_id": order_id})
    assert doc_final["customer_name"] == "محمد علي الجديد"
    assert doc_final["order_date"] == "2025-01-20T12:00:00"
    
    # Clean up
    collection.delete_one({"order_id": order_id})
    client.close()

@pytest.mark.skipif(not MONGO_AVAILABLE, reason="MongoDB is not running locally")
def test_spark_upsert_idempotency():
    """
    Verifies that spark_upsert_to_validated enforces Idempotency and Stable Business Key.
    """
    from src.spark_loader import spark_upsert_to_validated
    
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    collection = db[settings.COLLECTION_VALIDATED]
    
    run_id = f"test_run_{uuid.uuid4().hex[:6]}"
    order_id = f"spark_test_order_{uuid.uuid4().hex[:6]}"
    collection.delete_one({"order_id": order_id})
    
    record = {
        "order_id": f"  {order_id}  ", # Test stable business key trimming
        "run_id": run_id,
        "order_date": "2025-02-24T21:29:00",
        "customer_id": "customer-spark-1",
        "customer_name": "سامي أحمد",
        "total_amount": "50000.0",
        "currency": "YER",
        "items_json": "[]",
        "quality_status": "valid",
        "ingested_at": datetime.utcnow(),
        "engine_used": "pyspark",
        "source_file": "spark_test.csv"
    }
    
    # 1. First write -> inserted = 1
    ins, upd, unc = spark_upsert_to_validated([record], use_version_protection=False)
    assert ins == 1
    assert upd == 0
    assert collection.count_documents({"order_id": order_id}) == 1
    
    # 2. Re-run identical data -> unchanged = 1, inserts = 0
    ins2, upd2, unc2 = spark_upsert_to_validated([record], use_version_protection=False)
    assert ins2 == 0
    assert unc2 == 1
    assert collection.count_documents({"order_id": order_id}) == 1
    
    # Clean up
    collection.delete_one({"order_id": order_id})
    client.close()

@pytest.mark.skipif(not MONGO_AVAILABLE, reason="MongoDB is not running locally")
def test_spark_upsert_version_protection():
    """
    Verifies that spark_upsert_to_validated enforces Version Protection when enabled.
    """
    from src.spark_loader import spark_upsert_to_validated
    
    client = mongo_setup.get_mongo_client()
    db = mongo_setup.get_database(client)
    collection = db[settings.COLLECTION_VALIDATED]
    
    run_id = f"test_run_{uuid.uuid4().hex[:6]}"
    order_id = f"spark_v_order_{uuid.uuid4().hex[:6]}"
    collection.delete_one({"order_id": order_id})
    
    # V1
    rec_v1 = {
        "order_id": order_id,
        "run_id": run_id,
        "order_date": "2025-05-10T10:00:00",
        "customer_id": "cust-v1",
        "customer_name": "نسخة قديمة",
        "total_amount": "1000.0",
        "currency": "YER",
        "items_json": "[]",
        "quality_status": "valid",
        "ingested_at": datetime.utcnow(),
        "engine_used": "pyspark",
        "source_file": "spark_test.csv"
    }
    
    # Insert V1
    ins, upd, unc = spark_upsert_to_validated([rec_v1], use_version_protection=True)
    assert ins == 1
    
    # Older Version (2025-01-01) -> must be skipped
    rec_older = rec_v1.copy()
    rec_older["order_date"] = "2025-01-01T10:00:00"
    rec_older["customer_name"] = "محاولة كتابة قديمة"
    
    ins_old, upd_old, unc_old = spark_upsert_to_validated([rec_older], use_version_protection=True)
    assert ins_old == 0
    assert upd_old == 0
    assert unc_old == 1 # skipped!
    
    # Verify DB still has V1 data
    doc = collection.find_one({"order_id": order_id})
    assert doc["customer_name"] == "نسخة قديمة"
    assert doc["order_date"] == "2025-05-10T10:00:00"
    
    # Clean up
    collection.delete_one({"order_id": order_id})
    client.close()

