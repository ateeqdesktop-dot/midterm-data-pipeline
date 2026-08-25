from incremental_loader import apply_delta
from mongo_setup import MemoryRepository


def _row(order_id: str, version: int, customer_id: str = "C-1"):
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "quality_status": "valid",
        "error_codes": [],
        "version": version,
    }


def test_incremental_initial_delta_update_and_replay():
    repo = MemoryRepository()
    initial = apply_delta(repo, [_row("O-1", 1)])
    delta = apply_delta(repo, [_row("O-1", 2, "C-2"), _row("O-2", 1, "C-3")])
    replay = apply_delta(repo, [_row("O-1", 2, "C-2"), _row("O-2", 1, "C-3")])
    stale = apply_delta(repo, [_row("O-1", 1, "C-old")])
    assert initial == {"inserted_count": 1, "updated_count": 0, "unchanged_count": 0}
    assert delta == {"inserted_count": 1, "updated_count": 1, "unchanged_count": 0}
    assert replay == {"inserted_count": 0, "updated_count": 0, "unchanged_count": 2}
    assert stale == {"inserted_count": 0, "updated_count": 0, "unchanged_count": 1}
    assert repo.db.orders_validated.find_one({"order_id": "O-1"})["customer_id"] == "C-2"
    assert repo.counts()["orders_validated"] == 2
    repo.close()
