import json
from pathlib import Path

from batch_loader import run_batch
from mongo_setup import MemoryRepository


def test_elt_raw_quarantine_and_idempotency(tmp_path: Path):
    source = tmp_path / "orders.csv"
    source.write_text(
        "order_id,customer_id,customer_email,phone,order_date,items,price,quantity,shipping_cost,total,status\n"
        'O-1,C-1,u@example.com,777,2025-01-31,"[{""price"":2,""quantity"":1}]",2,1,0,2,مؤكد\n'
        'O-2,C-2,u@@mail..com,777,31/01/2025,"[{""price"":2,""quantity"":1}]",2 ريال,1,0,2,مدفوع\n'
        "O-3,C-3,bad,777,2025-02-30,broken,unknown,1,0,0,مؤكد\n",
        encoding="utf-8",
    )
    repo = MemoryRepository()
    report = tmp_path / "results.json"
    first = run_batch(source, repo, report, batch_size=2)
    counts_first = repo.counts()
    second = run_batch(source, repo, report, batch_size=2)
    counts_second = repo.counts()
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert first.raw_loaded == 3
    assert first.raw_loaded == first.valid_count + first.corrected_count + first.quarantine_count
    assert counts_first["orders_raw"] == 3
    assert counts_second["orders_validated"] == counts_first["orders_validated"]
    assert second.unchanged_count >= 1
    assert payload["consistency_check"] is True
    repo.close()
