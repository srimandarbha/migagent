import pytest
psycopg = pytest.importorskip("psycopg")
from persistence.repository import SRETrackerRepository, ConnectionPool


def test_sre_tracker_repository_pool_initialization():
    dsn = "postgresql://postgres:postgres@127.0.0.1:5432/migration_agent"
    repo = SRETrackerRepository(dsn, min_size=2, max_size=5)

    assert repo.dsn == dsn
    assert repo.min_size == 2
    assert repo.max_size == 5

    if ConnectionPool is not None:
        assert repo._pool is not None
        assert repo._pool.min_size == 2
        assert repo._pool.max_size == 5
    repo.close()
