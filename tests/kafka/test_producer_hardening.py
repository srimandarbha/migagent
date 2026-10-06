"""Acceptance tests for Task 2: Producer hardening."""
import pytest
from unittest.mock import MagicMock

from engine.ingress.kafka import (
    KafkaResultPublisher,
    KafkaSettings,
    KafkaDeliveryError,
)


class FakeProducer:
    def __init__(self, flush_remaining=0, delivery_err=None):
        self.flush_remaining = flush_remaining
        self.delivery_err = delivery_err
        self.produced = []
        self.flush_timeouts = []

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.produced.append((topic, value, key, headers))
        if on_delivery:
            on_delivery(self.delivery_err, MagicMock())

    def flush(self, timeout=None):
        self.flush_timeouts.append(timeout)
        return self.flush_remaining


def test_delivery_failure_raises():
    """When broker returns an error to the delivery callback, KafkaDeliveryError is raised."""
    producer = FakeProducer(delivery_err="Broker transport error: connection reset")
    publisher = KafkaResultPublisher.__new__(KafkaResultPublisher)
    publisher.settings = KafkaSettings()
    publisher.producer = producer

    with pytest.raises(KafkaDeliveryError, match="Result delivery failed: Broker transport error"):
        publisher.publish({"event_id": "evt-001", "status": "COMPLETED"})

    assert len(producer.produced) == 1
    assert producer.flush_timeouts == [10.0]


def test_flush_timeout_raises():
    """When flush() returns remaining > 0, KafkaDeliveryError is raised."""
    # 2 messages remained unacknowledged after timeout
    producer = FakeProducer(flush_remaining=2)
    publisher = KafkaResultPublisher.__new__(KafkaResultPublisher)
    publisher.settings = KafkaSettings(publish_timeout_seconds=10.0)
    publisher.producer = producer

    with pytest.raises(KafkaDeliveryError, match=r"Result flush timed out after 10.0s \(2 messages unacknowledged\)"):
        publisher.publish({"event_id": "evt-002", "status": "COMPLETED"})

    assert producer.flush_timeouts == [10.0]


def test_successful_delivery_does_not_raise():
    """Successful production and timely flush succeeds without error."""
    producer = FakeProducer(flush_remaining=0, delivery_err=None)
    publisher = KafkaResultPublisher.__new__(KafkaResultPublisher)
    publisher.settings = KafkaSettings()
    publisher.producer = producer

    publisher.publish({"event_id": "evt-003", "status": "COMPLETED"})

    assert len(producer.produced) == 1
    assert producer.flush_timeouts == [10.0]
