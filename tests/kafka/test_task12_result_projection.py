"""Acceptance tests for Task 12: Result projection and ingress sanitization."""
import json
from unittest.mock import MagicMock
import pytest

confluent_kafka = pytest.importorskip("confluent_kafka")
from confluent_kafka import TopicPartition

from engine.ingress.kafka import KafkaIngress, KafkaSettings, build_request


class FakeMessage:
    def __init__(self, value, offset=0, partition=0, topic="mfa.migration.failed"):
        self._val = value if isinstance(value, bytes) else json.dumps(value).encode("utf-8")
        self._offset = offset
        self._partition = partition
        self._topic = topic

    def value(self):
        return self._val

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def topic(self):
        return self._topic

    def error(self):
        return None


def test_canary_secret_scrubbed_from_request():
    """Event secrets are scrubbed before placing into the engine request contract."""
    raw_event = {
        "event_id": "evt-sec-01",
        "event_type": "MigrationFailed",
        "token": "bearer secret-token-xyz-123456",
        "authorization": "Bearer TopSecretAuthToken",
        "client_secret": "my-client-secret-pass",
        "message": "failed with secret bearer super-secret-123",
    }
    msg = FakeMessage(raw_event)
    req = build_request(raw_event, msg)

    assert req["event"]["token"] == "***REDACTED***"
    assert req["event"]["authorization"] == "***REDACTED***"
    assert req["event"]["client_secret"] == "***REDACTED***"
    assert "secret-token-xyz" not in json.dumps(req["event"])


def test_result_payload_is_projection():
    """The result topic payload contains projected summary schema_version 1, not full AgentState or evidence dump."""
    settings = KafkaSettings()

    published = []
    publisher = MagicMock()
    publisher.publish = lambda p: published.append(p)

    ingress = None

    def fake_agent(req):
        if ingress:
            ingress._shutdown = True
        return {
            "failure_case_id": "case-proj-01",
            "status": "SUFFICIENT_EVIDENCE",
            "next_step": "CHECK_STORAGE",
            "diagnosis": {"status": "LIKELY", "code": "STORAGE.CSI"},
            "decision_readiness": {"RETRY": {"status": "NOT_READY"}},
            "capability_coverage": {"status": "READY"},
            "recurrence": {"status": "FIRST_SEEN"},
            "learning": {"status": "NEW_FAILURE"},
            "recommendation": {"action": "NONE"},
            "errors": ["dummy error 1"],
            "trace": [f"step {i}" for i in range(25)],
            "agent_version": "2.12.3",
            "policy_version": "1.0",
            "classification": "STORAGE.CSI",
            # Sensitive or bulky raw internal fields
            "evidence": [{"fact": "PVC_PENDING", "raw_dump": "large_hex"}],
            "context": {"internal_keys": "data"},
        }

    raw_event = {
        "event_id": "evt-proj-01",
        "event_type": "MigrationFailed",
        "message": "pvc timeout",
    }
    msg = FakeMessage(raw_event)

    consumer = MagicMock()
    consumer.poll.return_value = msg
    consumer.commit = MagicMock()

    ingress = KafkaIngress(settings=settings, run_agent=fake_agent, result_publisher=publisher)
    ingress.consumer = consumer
    ingress._already_processed = lambda eid: False

    ingress.run_forever()

    assert len(published) == 1
    payload = published[0]

    # Verify top-level structure
    assert "evidence" not in payload
    assert "context" not in payload
    assert payload["event_id"] == "evt-proj-01"

    # Verify projection
    proj = payload["result"]
    assert proj["schema_version"] == 1
    assert "evidence" not in proj
    assert "context" not in proj
    assert proj["error_count"] == 1
    assert len(proj["trace_tail"]) == 20
    assert proj["trace_tail"][-1] == "step 24"
    assert proj["classification"] == "STORAGE.CSI"
