import json

from engine.ingress.kafka import KafkaEventError, parse_event


def test_migration_failed_event_is_valid():
    event = parse_event(json.dumps({"event_id": "evt-1", "event_type": "MigrationFailed"}).encode())
    assert event["event_id"] == "evt-1"


def test_missing_event_id_rejected():
    try:
        parse_event(b'{"event_type":"MigrationFailed"}')
        assert False, "expected KafkaEventError"
    except KafkaEventError as exc:
        assert "event_id" in str(exc)


def test_wrong_event_type_rejected():
    try:
        parse_event(b'{"event_id":"evt-1","event_type":"SomethingElse"}')
        assert False, "expected KafkaEventError"
    except KafkaEventError as exc:
        assert "Unsupported event_type" in str(exc)
