import pytest
from src.indexes.index_manager import get_index_definitions, create_phase2_indexes, INDEX_DEFINITIONS
from src.explain.explain_runner import BENCHMARK_QUERIES, run_single_explain

def test_index_definitions_structure():
    defs = get_index_definitions()
    assert len(defs) >= 3
    # Check for presence of single and compound indexes
    types = [d["type"] for d in defs]
    assert any("Compound" in t for t in types)
    assert any("Single" in t for t in types)
    assert any("Multikey" in t for t in types)

def test_idempotent_index_creation():
    res = create_phase2_indexes()
    assert res["status"] in ("success", "partial_success")
    assert "created_count" in res
    assert "existing_count" in res
    
    # Running a second time must not fail and should report indexes already existing
    res2 = create_phase2_indexes()
    assert res2["status"] == "success"
    assert res2["existing_count"] == 3

def test_explain_execution_stats_ixscan():
    for q in BENCHMARK_QUERIES:
        stats = run_single_explain(q["collection"], q["filter"], limit=5)
        assert stats["scanType"] == "IXSCAN"
        assert stats["totalKeysExamined"] > 0
        assert stats["executionTimeMillis"] <= 100  # Should be virtually instantaneous with index
