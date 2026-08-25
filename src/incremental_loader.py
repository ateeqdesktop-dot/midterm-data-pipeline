from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any


def _version_key(value: Any) -> tuple[int, Any]:
    """Compare numeric, ISO datetime, and textual versions deterministically."""
    if value is None or value == "":
        return (0, "")
    try:
        return (2, float(value))
    except (TypeError, ValueError):
        pass
    try:
        return (3, datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return (1, str(value))


def apply_delta(
    repo: Any, rows: Iterable[dict[str, Any]], version_field: str = "version"
) -> dict[str, int]:
    """Apply only newer delta records and make replay a no-op."""
    inserted = updated = unchanged = 0
    for row in rows:
        order_id = row.get("order_id")
        if not order_id:
            unchanged += 1
            continue
        current = repo.db.orders_validated.find_one({"order_id": order_id})
        if current is None:
            repo.db.orders_validated.update_one({"order_id": order_id}, {"$set": row}, upsert=True)
            inserted += 1
            continue
        incoming_version = row.get(version_field, row.get("updated_at", ""))
        current_version = current.get(version_field, current.get("updated_at", ""))
        if _version_key(incoming_version) <= _version_key(current_version):
            unchanged += 1
            continue
        repo.db.orders_validated.update_one({"order_id": order_id}, {"$set": row}, upsert=True)
        updated += 1
    return {"inserted_count": inserted, "updated_count": updated, "unchanged_count": unchanged}
