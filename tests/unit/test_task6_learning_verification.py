"""Acceptance tests for Task 6: Learning verification fail-closed defaults."""
import pytest
from unittest.mock import MagicMock
from uuid import uuid4

from engine.integrations.local.tracker import FixtureSRETrackerAdapter
from engine.workflow.engine import MigrationFailureEngine
from persistence.repository import SRETrackerRepository


def test_default_resolution_is_unverified():
    """Omitting verification_status defaults to UNVERIFIED."""
    tracker = FixtureSRETrackerAdapter()
    case_id = tracker.create_or_get_failure_case(event_id="evt-task6-01", migration_id="mig-01")
    tracker.record_resolution(
        failure_case_id=case_id,
        resolution_code="FIX-01",
        description="Manual mitigation",
    )
    res = tracker.cases[0]["resolutions"][0]
    assert res["verification_status"] == "UNVERIFIED"


def test_passed_requires_state_evidence():
    """Setting verification_status='PASSED' without expected_state and observed_state raises ValueError."""
    tracker = FixtureSRETrackerAdapter()
    case_id = tracker.create_or_get_failure_case(event_id="evt-task6-02", migration_id="mig-02")
    engine = MigrationFailureEngine(registry=MagicMock(), tracker=tracker)
    with pytest.raises(ValueError, match="PASSED verification requires expected_state and observed_state"):
        engine.record_resolution(
            failure_case_id=case_id,
            resolution_code="FIX-02",
            description="Mitigation",
            verification_status="PASSED",
            expected_state=None,
            observed_state=None,
        )

    # Repository also raises ValueError
    repo = SRETrackerRepository.__new__(SRETrackerRepository)
    repo.connection = MagicMock()
    with pytest.raises(ValueError, match="PASSED verification requires expected_state and observed_state"):
        repo.record_resolution(
            failure_case_id=uuid4(),
            resolution_code="FIX-02",
            description="Mitigation",
            verification_status="PASSED",
            expected_state=None,
            observed_state=None,
        )


def test_verified_status_not_set_on_unverified():
    """When verification_status is UNVERIFIED (or != PASSED), case status is NOT set to VERIFIED."""
    tracker = FixtureSRETrackerAdapter()
    case_id = tracker.create_or_get_failure_case(event_id="evt-task6-03", migration_id="mig-03")
    tracker.record_resolution(
        failure_case_id=case_id,
        resolution_code="FIX-03",
        description="Mitigation",
        outcome_status="RESOLVED",
        verification_status="UNVERIFIED",
    )
    assert tracker.cases[0]["status"] != "VERIFIED"
    assert tracker.cases[0]["status"] == "RETAINED"
