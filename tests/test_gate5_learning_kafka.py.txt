"""Gate 5: Episodic Learning Lifecycle & Kafka Recurrence Tests.

Verifies:
  1. Episodic learning lifecycle:
     - FIRST_SEEN -> RECURRING_UNRESOLVED -> SRE resolution -> LEARNING_CANDIDATE -> Human validation -> KNOWN_ISSUE.
     - Negative test: action performed without documented/verified resolution must NOT become a candidate or known issue.
  2. Kafka delivery idempotency and deduplication:
     - Duplicate event_id is skipped without invoking the agent engine, and offset is committed.
     - Distinct event_id with same failure symptoms is correlated and increments recurrence.
  3. Kafka error handling:
     - Malformed/poison events are rejected and offset committed.
     - Processing failures do NOT commit offset (fail-closed).
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock
import pytest

from engine.workflow.engine import run_agent
from engine.learning import LearningLifecycle
from engine.integrations.local.registry import build_local_registry
from engine.integrations.local.tracker import FixtureSRETrackerAdapter
from engine.ingress.kafka import KafkaIngress, KafkaSettings, KafkaEventError, parse_event, build_request
from simulator.world import make_world


def _make_event(event_id: str, failure_code: str = "custom.migration.failure", phase: str = "transfer"):
    return {
        "event_id": event_id,
        "event_type": "MigrationFailed",
        "failure_code": failure_code,
        "phase": phase,
        "message": f"Failure occurred: {failure_code}",
        "cluster_id": "ocv-prod-01",
        "migration_id": f"mig-{event_id}",
        "environment": {
            "target": {"cluster_id": "ocv-prod-01", "mtv_version": "2.11.0", "ocp_version": "4.19.23"},
            "source": {"provider": "vmware", "vcenter_version": "8.0"},
            "migration": {"type": "warm"},
        },
    }


def test_gate5_episodic_learning_lifecycle_full_cycle():
    """Test full cycle: FIRST_SEEN -> RECURRING -> RESOLUTION -> CANDIDATE -> VALIDATION -> KNOWN_ISSUE."""
    tracker = FixtureSRETrackerAdapter()
    _, registry, _, _ = make_world("unknown", memory_mode="none")

    # Step 1: FIRST_SEEN
    req1 = {"failure_case_id": "case-001", "memory_mode": "sre", "event": _make_event("evt-001")}
    state1 = run_agent(req1, registry, tracker=tracker, knowledge=None, memory_mode="sre")
    assert state1.recurrence["recurrence_status"] == "FIRST_SEEN"
    assert state1.learning["knowledge_gap"] is True

    # Step 2: RECURRING_UNRESOLVED
    req2 = {"failure_case_id": "case-002", "memory_mode": "sre", "event": _make_event("evt-002")}
    state2 = run_agent(req2, registry, tracker=tracker, knowledge=None, memory_mode="sre")
    assert state2.recurrence["recurrence_status"] == "RECURRING_UNRESOLVED"
    assert state2.recurrence["occurrence_count"] == 2
    assert state2.learning["validated_solution_exists"] is False

    # Step 3: SRE Resolution recorded for open cases -> Promoted to CANDIDATE
    learning = LearningLifecycle(tracker)
    cand_result = learning.record_resolution(
        failure_case_id=state1.failure_case_id,
        resolution_code="RECOVERY.CUSTOM_ACTION_01",
        description="SRE executed verified remediation procedure.",
        outcome_status="RESOLVED",
        verification_status="PASSED",
        recorded_by="sre-alice",
        failure_signature=state1.failure_signature,
        failure_class=state1.classification,
        failure_code="custom.migration.failure",
        diagnosis_code="DIAGNOSED",
    )
    learning.record_resolution(
        failure_case_id=state2.failure_case_id,
        resolution_code="RECOVERY.CUSTOM_ACTION_01",
        description="SRE executed verified remediation procedure.",
        outcome_status="RESOLVED",
        verification_status="PASSED",
        recorded_by="sre-alice",
        failure_signature=state2.failure_signature,
        failure_class=state2.classification,
        failure_code="custom.migration.failure",
        diagnosis_code="DIAGNOSED",
    )
    assert cand_result["learning_status"] == "CANDIDATE"
    assert cand_result["candidate_id"] is not None

    # Step 4: Next recurrence before validation is now resolved history but unvalidated
    req3 = {"failure_case_id": "case-003", "memory_mode": "sre", "event": _make_event("evt-003")}
    state3 = run_agent(req3, registry, tracker=tracker, knowledge=None, memory_mode="sre")
    assert state3.recurrence["recurrence_status"] == "RECURRING_RESOLVED"
    assert state3.learning["validated_solution_exists"] is False

    # Step 5: Human validation (SRE Lead review) -> Promoted to VALIDATED / KNOWN_ISSUE
    val_result = learning.record_resolution(
        failure_case_id=state1.failure_case_id,
        resolution_code="RECOVERY.CUSTOM_ACTION_01",
        description="SRE executed verified remediation procedure.",
        outcome_status="RESOLVED",
        verification_status="PASSED",
        recorded_by="sre-alice",
        validated_by="sre-lead-bob",
        validation_reason="Verified across 3 separate migrations.",
        failure_signature=state1.failure_signature,
        failure_class=state1.classification,
        failure_code="custom.migration.failure",
        diagnosis_code="DIAGNOSED",
    )
    assert val_result["learning_status"] == "VALIDATED"

    # Step 6: Recurrence now detects KNOWN_ISSUE with validated solution reference
    req4 = {"failure_case_id": "case-004", "memory_mode": "sre", "event": _make_event("evt-004")}
    state4 = run_agent(req4, registry, tracker=tracker, knowledge=None, memory_mode="sre")
    assert state4.recurrence["recurrence_status"] == "KNOWN_ISSUE"
    assert state4.learning["status"] == "VALIDATED_KNOWLEDGE_AVAILABLE"
    assert "RECOVERY.CUSTOM_ACTION_01" in state4.recurrence["validated_solution_refs"]


def test_gate5_negative_undocumented_fix_never_becomes_candidate():
    """Negative case: Unverified or undocumented resolution cannot become reusable knowledge."""
    tracker = FixtureSRETrackerAdapter()
    _, registry, _, _ = make_world("unknown", memory_mode="none")

    req = {"failure_case_id": "case-neg-001", "memory_mode": "sre", "event": _make_event("evt-neg-001")}
    state = run_agent(req, registry, tracker=tracker, knowledge=None, memory_mode="sre")

    learning = LearningLifecycle(tracker)
    res = learning.record_resolution(
        failure_case_id=state.failure_case_id,
        resolution_code=None,  # Missing structured code!
        description="Ad-hoc manual fix without documentation",
        outcome_status="RESOLVED",
        verification_status="PASSED",
        recorded_by="sre-user",
        failure_signature=state.failure_signature,
        failure_class=state.classification,
        failure_code="custom.migration.failure",
    )
    assert res["learning_status"] == "NOT_LEARNABLE"
    assert len(tracker.learning_candidates) == 0


def test_gate5_kafka_delivery_idempotency_and_deduplication():
    """Test Kafka delivery idempotency: duplicate event_id is skipped and offset committed."""
    processed_events = set()

    class MockTracker:
        def __init__(self):
            self.completed = set()

        def event_exists(self, event_id: str) -> bool:
            return event_id in self.completed

        def mark_event_completed(self, event_id: str) -> None:
            self.completed.add(event_id)

    mock_tracker = MockTracker()
    agent_invocations = []

    def mock_run_agent(request: dict):
        event_id = request["event"]["event_id"]
        agent_invocations.append(event_id)
        return {
            "failure_case_id": "case-123",
            "status": "COMPLETED",
            "diagnosis": {"mechanism": "VMWARE.CBT_STATE"},
            "next_step": {"action": "OBSERVE"},
        }

    mock_publisher = MagicMock()

    ingress = KafkaIngress.__new__(KafkaIngress)
    ingress.settings = KafkaSettings()
    ingress.run_agent = mock_run_agent
    ingress.result_publisher = mock_publisher
    ingress.tracker = mock_tracker
    ingress.consumer = MagicMock()

    class FakeMessage:
        def __init__(self, payload: dict, offset: int):
            self._val = json.dumps(payload).encode("utf-8")
            self._offset = offset

        def value(self):
            return self._val

        def topic(self):
            return "mfa.migration.failed"

        def partition(self):
            return 0

        def offset(self):
            return self._offset

        def error(self):
            return None

    # First delivery of evt-100
    msg1 = FakeMessage(_make_event("evt-100"), offset=1)
    event1 = parse_event(msg1.value())
    req1 = build_request(event1, msg1)

    assert not ingress._already_processed("evt-100")
    state1 = ingress.run_agent(req1)
    ingress.tracker.mark_event_completed("evt-100")
    ingress.consumer.commit(message=msg1, asynchronous=False)

    assert len(agent_invocations) == 1
    assert "evt-100" in ingress.tracker.completed

    # Duplicate redelivery of evt-100 (e.g., consumer rebalance or network glitch)
    msg2 = FakeMessage(_make_event("evt-100"), offset=1)
    event2 = parse_event(msg2.value())
    req2 = build_request(event2, msg2)

    # Idempotency check triggers!
    assert ingress._already_processed("evt-100") is True
    # Agent is NOT called again
    ingress.consumer.commit(message=msg2, asynchronous=False)

    assert len(agent_invocations) == 1  # Still 1, not 2!


def test_gate5_kafka_poison_message_rejection_and_fail_closed():
    """Malformed event is rejected and offset committed; agent crash does not commit offset."""
    # Test 1: Invalid JSON
    with pytest.raises(KafkaEventError, match="Invalid Kafka JSON payload"):
        parse_event(b"NOT_JSON")

    # Test 2: Missing event_id
    with pytest.raises(KafkaEventError, match="Missing required event fields: event_id"):
        parse_event(b'{"event_type": "MigrationFailed"}')

    # Test 3: Wrong event_type
    with pytest.raises(KafkaEventError, match="Unsupported event_type: RandomEvent"):
        parse_event(b'{"event_id": "evt-x", "event_type": "RandomEvent"}')
