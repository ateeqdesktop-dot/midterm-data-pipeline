import json
from pathlib import Path

from batch_loader import run_batch
from file_router import choose_engine
from mongo_setup import MemoryRepository


class RecordingRepository:
    def __init__(self) -> None:
        self.inner = MemoryRepository()
        self.raw_batch_sizes: list[int] = []

    def insert_raw(self, rows):
        docs = list(rows)
        self.raw_batch_sizes.append(len(docs))
        return self.inner.insert_raw(docs)

    def upsert_final(self, rows, quarantine):
        return self.inner.upsert_final(rows, quarantine)

    def counts(self):
        return self.inner.counts()

    def close(self):
        self.inner.close()


def test_router_uses_threshold(tmp_path: Path):
    source = tmp_path / "orders.csv"
    source.write_text("order_id\nO-1\n", encoding="utf-8")
    decision = choose_engine(source, threshold_mb=0.000001)
    assert decision.engine == "pyspark"
    assert "threshold" in decision.reason


def test_batch_loader_uses_bounded_insert_many_batches(tmp_path: Path):
    source = tmp_path / "orders.csv"
    source.write_text(
        "order_id,customer_id,customer_email,phone,order_date,items,price,quantity,shipping_cost,total,status\n"
        'O-1,C-1,u@example.com,777,2025-01-31,"[{""price"":2,""quantity"":1}]",2,1,0,2,مؤكد\n'
        'O-2,C-2,v@example.com,777,2025-01-31,"[{""price"":2,""quantity"":1}]",2,1,0,2,مؤكد\n'
        'O-3,C-3,w@example.com,777,2025-01-31,"[{""price"":2,""quantity"":1}]",2,1,0,2,مؤكد\n',
        encoding="utf-8",
    )
    repo = RecordingRepository()
    report = tmp_path / "results.json"
    metrics = run_batch(source, repo, report, batch_size=2)
    assert repo.raw_batch_sizes == [2, 1]
    assert metrics.batches == 2
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["batch_timings"]
    repo.close()


def test_memory_repository_indexes_and_quarantine_upsert():
    repo = MemoryRepository()
    validated_indexes = {item["name"] for item in repo.db.orders_validated.list_indexes()}
    quarantine_indexes = {item["name"] for item in repo.db.orders_quarantine.list_indexes()}
    assert "uq_order_id" in validated_indexes
    assert "uq_quarantine_source_row" in quarantine_indexes
    row = {
        "order_id": "O-1",
        "customer_id": "C-1",
        "quality_status": "quarantined",
        "error_codes": ["MISSING_CUSTOMER_ID"],
        "error_details": [{"code": "MISSING_CUSTOMER_ID", "detail": "required"}],
        "raw_record": {"order_id": "O-1"},
        "source_file": "orders.csv",
        "source_row_number": 2,
    }
    repo.upsert_final([], [row])
    repo.upsert_final([], [row | {"run_id": "second-run"}])
    assert repo.counts()["orders_quarantine"] == 1
    repo.close()
