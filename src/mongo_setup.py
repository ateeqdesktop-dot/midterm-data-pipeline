import pymongo
from config import settings

def get_mongo_client():
    """Returns a PyMongo MongoClient instance."""
    return pymongo.MongoClient(settings.MONGO_URI)

def get_database(client=None):
    """Returns the MongoDB database instance."""
    if client is None:
        client = get_mongo_client()
    return client[settings.MONGO_DB_NAME]

def setup_mongodb():
    """Sets up the collections, validators, and indexes for the project."""
    client = get_mongo_client()
    db = get_database(client)
    
    print(f"Setting up MongoDB database: '{settings.MONGO_DB_NAME}'...")
    
    # 1. raw collection: no validators, no unique indexes to allow all dirty data
    if settings.COLLECTION_RAW not in db.list_collection_names():
        db.create_collection(settings.COLLECTION_RAW)
        print(f"Collection '{settings.COLLECTION_RAW}' created.")
    
    # Create indexes on orders_raw for efficient queries by run_id
    db[settings.COLLECTION_RAW].create_index("run_id")
    print(f"Created index on 'run_id' in '{settings.COLLECTION_RAW}'.")
    
    # Idempotency index: prevents duplicate raw records on re-run of same run_id
    db[settings.COLLECTION_RAW].create_index(
        [("run_id", pymongo.ASCENDING), ("source_row_number", pymongo.ASCENDING)],
        unique=True,
        name="idx_idempotency_raw"
    )
    print(f"Compound Unique Index 'idx_idempotency_raw' created on '{settings.COLLECTION_RAW}' for Idempotency.")
    
    # 2. validated collection: stable business key (order_id) must be unique
    if settings.COLLECTION_VALIDATED not in db.list_collection_names():
        # Let's create with validator to ensure proper types if required, or at least structure
        validation_schema = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["order_id", "order_date", "customer_id", "customer_name", "total_amount", "currency", "items_json"],
                "properties": {
                    "order_id": {
                        "bsonType": "string",
                        "description": "must be a string and is required"
                    },
                    "customer_id": {
                        "bsonType": "string",
                        "description": "must be a string and is required"
                    },
                    "total_amount": {
                        "bsonType": "double",
                        "description": "must be a double and is required"
                    },
                    "currency": {
                        "bsonType": "string",
                        "description": "must be a string and is required"
                    }
                }
            }
        }
        try:
            db.create_collection(
                settings.COLLECTION_VALIDATED,
                validator=validation_schema
            )
            print(f"Collection '{settings.COLLECTION_VALIDATED}' created with jsonSchema validation.")
        except Exception as e:
            # Fallback in case of strict BSON type errors or environment limitations
            db.create_collection(settings.COLLECTION_VALIDATED)
            print(f"Collection '{settings.COLLECTION_VALIDATED}' created without schema validator: {e}")
            
    # Create Unique Index on order_id
    db[settings.COLLECTION_VALIDATED].create_index([("order_id", pymongo.ASCENDING)], unique=True)
    print(f"Unique Index on 'order_id' created in '{settings.COLLECTION_VALIDATED}'.")
    
    # Index on run_id for performance
    db[settings.COLLECTION_VALIDATED].create_index("run_id")
    
    # Version protection: index on order_date for efficient version comparison
    db[settings.COLLECTION_VALIDATED].create_index("order_date", name="idx_version_date")
    print(f"Version Index 'idx_version_date' created on '{settings.COLLECTION_VALIDATED}' for Version Protection.")
    
    # 3. quarantine collection: for records that failed validation
    if settings.COLLECTION_QUARANTINE not in db.list_collection_names():
        db.create_collection(settings.COLLECTION_QUARANTINE)
        print(f"Collection '{settings.COLLECTION_QUARANTINE}' created.")
        
    # Non-unique indexes on orders_quarantine for query performance
    db[settings.COLLECTION_QUARANTINE].create_index("order_id")
    db[settings.COLLECTION_QUARANTINE].create_index("run_id")
    print(f"Indexes created on 'order_id' and 'run_id' in '{settings.COLLECTION_QUARANTINE}'.")
    
    client.close()
    print("MongoDB setup completed successfully.")

if __name__ == "__main__":
    setup_mongodb()
