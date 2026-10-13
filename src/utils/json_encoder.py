from datetime import datetime, date
from decimal import Decimal
from typing import Any
from bson import ObjectId

def serialize_mongo_doc(doc: Any) -> Any:
    """
    Recursively converts MongoDB BSON types (ObjectId, datetime, Decimal)
    into JSON-serializable standard Python data structures without losing information.
    """
    if doc is None:
        return None
    if isinstance(doc, dict):
        return {k: serialize_mongo_doc(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [serialize_mongo_doc(item) for item in doc]
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, (datetime, date)):
        return doc.isoformat()
    if isinstance(doc, Decimal):
        return float(doc)
    return doc
