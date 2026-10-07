"""Acceptance tests for Task 7: Stop silent evidence drops."""
import logging
from unittest.mock import MagicMock
from uuid import UUID, uuid5, NAMESPACE_URL

from persistence.repository import SRETrackerRepository


def test_cross_case_ids_do_not_collide():
    """Same raw evidence id in two different cases generates two distinct UUIDs."""
    repo = SRETrackerRepository.__new__(SRETrackerRepository)
    executed_inserts = []
    mock_conn = MagicMock()

    def capture_execute(sql, params=None):
        if "INSERT INTO sre.evidence" in sql:
            executed_inserts.append(params)
        return MagicMock()

    mock_conn.execute.side_effect = capture_execute
    repo.connection = MagicMock()
    repo.connection.return_value.__enter__.return_value = mock_conn

    case_1 = UUID("11111111-1111-1111-1111-111111111111")
    case_2 = UUID("22222222-2222-2222-2222-222222222222")
    raw_ev = {"id": "ev-collision-test", "fact": "MIGRATION_FAILED"}

    id_1 = repo.save_evidence(case_1, raw_ev)
    id_2 = repo.save_evidence(case_2, raw_ev)

    assert id_1 != id_2
    assert id_1 == uuid5(case_1, "ev-collision-test")
    assert id_2 == uuid5(case_2, "ev-collision-test")

    # Non-UUID string case_id fallback works and namespaces properly
    id_str1 = repo.save_evidence("case-alpha", raw_ev)
    id_str2 = repo.save_evidence("case-beta", raw_ev)
    assert id_str1 != id_str2


def test_duplicate_drop_is_logged(caplog):
    """When evidence insertion returns no row due to conflict, a warning is logged."""
    repo = SRETrackerRepository.__new__(SRETrackerRepository)
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None  # Conflict: DO NOTHING returned no rows
    mock_conn.execute.return_value = mock_cursor

    repo.connection = MagicMock()
    repo.connection.return_value.__enter__.return_value = mock_conn

    case_id = UUID("33333333-3333-3333-3333-333333333333")
    raw_ev = {"id": "ev-dup", "fact": "PVC_PENDING"}

    with caplog.at_level(logging.WARNING):
        ev_id = repo.save_evidence(case_id, raw_ev)

    assert any(
        "evidence dropped as duplicate" in record.message
        and str(ev_id) in record.message
        and str(case_id) in record.message
        for record in caplog.records
    )
