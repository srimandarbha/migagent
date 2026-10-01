import os
import pytest
from pathlib import Path
from scripts.benchmark_rag import run_benchmark

ROOT = Path(__file__).resolve().parents[1]

@pytest.mark.skipif(
    not os.getenv("RUN_LIVE_BENCHMARK"),
    reason="Live vector benchmark requires live PostgreSQL and embedding endpoint"
)
def test_rag_30_query_golden_benchmark():
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:5432/migration_agent")
    embed_url = os.getenv("LOCAL_EMBEDDING_URL", "http://127.0.0.1:11434/v1")
    metrics = run_benchmark(dsn, embed_url)
    assert metrics["recall_1"] >= 0.80
    assert metrics["recall_3"] >= 0.90
    assert metrics["recall_5"] >= 0.95
    assert metrics["mrr"] >= 0.85
