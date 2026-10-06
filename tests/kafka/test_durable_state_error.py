"""Acceptance tests for Task 3: Surface persistence failure before commit."""
import json
import pytest
from unittest.mock import MagicMock

from engine.contracts import DurableStateError
from engine.ingress.kafka import KafkaIngress, KafkaSettings
from engine.integrations.registry import InMemoryCapabilityRegistry
from engine.workflow.engine import MigrationFailureEngine


class StubTrackerWithFailingDiagnosis:
    def __init__(self):
        self.cases = []

    def create_or_get_failure_case(self, **kwargs):
        return "case-stub-001"

    def save_event(self, **kwargs):
        pass

    def save_evidence(self, case_id, evidence):
        return "ev-001"

    def update_case_metadata(self, *args, **kwargs):
        pass

    def save_diagnosis(self, case_id, diagnosis, evidence_ids=None):
        raise RuntimeError("PostgreSQL dead: connection lost mid-transaction")

    def mark_event_completed(self, event_id):
        pass


def test_tracker_failure_raises_durable_state_error():
    """Tracker stub whose save_diagnosis raises -> run() raises DurableStateError."""
    registry = InMemoryCapabilityRegistry({
        "observability.search": lambda p: {"evidence": []},
    })
    tracker = StubTrackerWithFailingDiagnosis()
    engine = MigrationFailureEngine(registry, tracker=tracker)

    req = {
        "event": {
            "event_id": "evt-durable-01",
            "event_type": "MigrationFailed",
            "message": "VM disk migration failed",
        }
    }

    with pytest.raises(DurableStateError, match="tracker diagnosis persistence"):
        engine.run(req)


def test_no_tracker_no_raise():
    """tracker=None (dev/fixture mode) with same event -> returns normally without raising DurableStateError."""
    registry = InMemoryCapabilityRegistry({
        "observability.search": lambda p: {"evidence": []},
    })
    engine = MigrationFailureEngine(registry, tracker=None)

    req = {
        "event": {
            "event_id": "evt-durable-02",
            "event_type": "MigrationFailed",
            "message": "VM disk migration failed",
        }
    }

    state = engine.run(req)
    assert state is not None
    assert state.failure_case_id is not None


def test_kafka_generic_handler_no_commit_on_durable_error():
    """Feed a run_agent that raises DurableStateError into KafkaIngress -> no commit call."""
    from tests.kafka.test_offset_discipline import FakeConsumer, FakeMessage

    msg = FakeMessage({
        "event_id": "evt-durable-03",
        "event_type": "MigrationFailed",
        "message": "VM disk migration failed",
    }, offset=500)

    fake_consumer = FakeConsumer([msg])
    settings = KafkaSettings()

    def run_agent_durable_error(request):
        raise DurableStateError("tracker persistence failed")

    ingress = KafkaIngress.__new__(KafkaIngress)
    ingress.settings = settings
    ingress.run_agent = run_agent_durable_error
    ingress.result_publisher = None
    ingress.tracker = None
    ingress.consumer = fake_consumer
    ingress._shutdown = False
    ingress._partition_failures = {}
    ingress._infra_failures = {}
    ingress._retry_deadlines = {}

    def poll_once(timeout):
        m = fake_consumer._messages.pop(0) if fake_consumer._messages else None
        ingress._shutdown = True
        return m

    fake_consumer.poll = poll_once
    ingress.run_forever()

    # Offset must NOT be committed!
    assert len(fake_consumer.commit_calls) == 0
    # Partition must be seeked back and paused
    assert len(fake_consumer.seek_calls) == 1
    assert fake_consumer.seek_calls[0].offset == 500
    assert len(fake_consumer.pause_calls) == 1
