from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pymongo import MongoClient, UpdateOne

VALIDATED_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["order_id", "customer_id", "quality_status", "error_codes"],
        "properties": {
            "order_id": {"bsonType": "string", "minLength": 1},
            "customer_id": {"bsonType": "string", "minLength": 1},
            "quality_status": {"enum": ["valid", "corrected"]},
            "error_codes": {"bsonType": "array"},
        },
    }
}
RAW_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["run_id", "source_file", "source_row_number", "raw_record"],
        "properties": {
            "run_id": {"bsonType": "string"},
            "source_file": {"bsonType": "string"},
            "source_row_number": {"bsonType": ["int", "long"]},
            "raw_record": {"bsonType": "object"},
        },
    }
}
QUARANTINE_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "source_file",
            "source_row_number",
            "quality_status",
            "error_codes",
            "raw_record",
        ],
        "properties": {
            "source_file": {"bsonType": "string"},
            "source_row_number": {"bsonType": ["int", "long", "string"]},
            "quality_status": {"enum": ["quarantined"]},
            "error_codes": {"bsonType": "array", "minItems": 1},
            "raw_record": {"bsonType": "object"},
        },
    }
}


class MongoRepository:
    def __init__(self, uri: str, database: str, client: Any | None = None) -> None:
        self.client = client or MongoClient(uri, serverSelectionTimeoutMS=3000)
        self.db = self.client[database]
        self.db.command("ping")
        self.setup()

    def _ensure_collection(self, name: str, validator: dict[str, Any]) -> None:
        try:
            if name not in self.db.list_collection_names():
                self.db.create_collection(
                    name,
                    validator=validator,
                    validationLevel="strict",
                    validationAction="error",
                )
            else:
                self.db.command(
                    "collMod",
                    name,
                    validator=validator,
                    validationLevel="strict",
                    validationAction="error",
                )
        except Exception as exc:
            # mongomock does not implement MongoDB validator/collMod; real MongoDB does.
            if isinstance(exc, NotImplementedError) or exc.__class__.__module__.startswith(
                "mongomock"
            ):
                if name not in self.db.list_collection_names():
                    self.db.create_collection(name)
            else:
                raise

    def setup(self) -> None:
        self._ensure_collection("orders_raw", RAW_VALIDATOR)
        self._ensure_collection("orders_validated", VALIDATED_VALIDATOR)
        self._ensure_collection("orders_quarantine", QUARANTINE_VALIDATOR)
        self.db.orders_validated.create_index("order_id", unique=True, name="uq_order_id")
        self.db.orders_quarantine.create_index(
            [("source_file", 1), ("source_row_number", 1)],
            unique=True,
            name="uq_quarantine_source_row",
        )
        self.db.orders_raw.create_index([("run_id", 1), ("source_row_number", 1)])

    def close(self) -> None:
        self.client.close()

    def insert_raw(self, rows: Iterable[dict[str, Any]]) -> int:
        docs = list(rows)
        if not docs:
            return 0
        self.db.orders_raw.insert_many(docs, ordered=False)
        return len(docs)

    def upsert_final(
        self, rows: Iterable[dict[str, Any]], quarantine: Iterable[dict[str, Any]]
    ) -> dict[str, int]:
        valid = list(rows)
        bad = list(quarantine)
        if bad:
            operations = [
                UpdateOne(
                    {
                        "source_file": row["source_file"],
                        "source_row_number": row["source_row_number"],
                    },
                    {"$set": row},
                    upsert=True,
                )
                for row in bad
            ]
            try:
                self.db.orders_quarantine.bulk_write(operations, ordered=False)
            except TypeError as exc:
                # Older mongomock releases do not accept PyMongo's newer sort argument.
                if "sort" not in str(exc):
                    raise
                for row in bad:
                    self.db.orders_quarantine.update_one(
                        {
                            "source_file": row["source_file"],
                            "source_row_number": row["source_row_number"],
                        },
                        {"$set": row},
                        upsert=True,
                    )
        inserted = updated = unchanged = 0
        ignored = {
            "_id",
            "run_id",
            "source_file",
            "source_row_number",
            "ingested_at",
            "engine_used",
        }
        for row in valid:
            key = {"order_id": row["order_id"]}
            old = self.db.orders_validated.find_one(key)
            if old is None:
                self.db.orders_validated.update_one(key, {"$set": row}, upsert=True)
                inserted += 1
                continue
            old_business = {k: v for k, v in old.items() if k not in ignored}
            new_business = {k: v for k, v in row.items() if k not in ignored}
            if old_business == new_business:
                unchanged += 1
            else:
                self.db.orders_validated.update_one(key, {"$set": row}, upsert=True)
                updated += 1
        return {"inserted_count": inserted, "updated_count": updated, "unchanged_count": unchanged}

    def counts(self) -> dict[str, int]:
        return {
            name: self.db[name].count_documents({})
            for name in ("orders_raw", "orders_validated", "orders_quarantine")
        }


class MemoryRepository:
    """mongomock-backed repository for deterministic tests and offline demo."""

    def __init__(self, database: str = "midterm_orders") -> None:
        import mongomock

        self._repo = MongoRepository("mongodb://mock", database, client=mongomock.MongoClient())

    def __getattr__(self, name: str) -> Any:
        return getattr(self._repo, name)
