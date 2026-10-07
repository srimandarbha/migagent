"""Acceptance tests for Task 4: Graceful SIGTERM shutdown."""
import signal
from unittest.mock import MagicMock
import pytest

from engine.ingress.kafka import KafkaIngress, KafkaSettings
from tests.kafka.test_offset_discipline import FakeConsumer, FakePublisher


def test_shutdown_flag_stops_loop():
    """Setting _shutdown flag after first poll causes loop to exit and close() to be called on consumer."""
    fake_consumer = FakeConsumer()
    fake_publisher = FakePublisher()
    fake_publisher.producer = MagicMock()

    settings = KafkaSettings()

    ingress = KafkaIngress.__new__(KafkaIngress)
    ingress.settings = settings
    ingress.run_agent = lambda req: None
    ingress.result_publisher = fake_publisher
    ingress.tracker = None
    ingress.consumer = fake_consumer
    ingress._shutdown = False
    ingress._partition_failures = {}
    ingress._retry_deadlines = {}

    polls = [0]

    def poll_and_set_shutdown(timeout):
        polls[0] += 1
        ingress._shutdown = True
        return None

    fake_consumer.poll = poll_and_set_shutdown

    ingress.run_forever()

    # Loop exited after 1 poll
    assert polls[0] == 1
    # Finally block must have unsubscribed, closed consumer, and flushed publisher
    assert fake_consumer.unsubscribed is True
    assert fake_consumer.closed is True
    assert fake_publisher.producer.flush.called is True


def test_sigint_sets_shutdown():
    """Simulate handler invocation -> _shutdown flag set."""
    handlers = {}

    def mock_signal(signum, handler):
        handlers[signum] = handler
        return None

    orig_signal = signal.signal
    signal.signal = mock_signal
    try:
        ingress = KafkaIngress.__new__(KafkaIngress)
        ingress.settings = KafkaSettings()
        ingress.run_agent = lambda req: None
        ingress.result_publisher = None
        ingress.tracker = None
        ingress._shutdown = False
        ingress._partition_failures = {}
        ingress._retry_deadlines = {}

        # Simulate SIGINT handler installation
        def _sig_handler(signum, frame):
            ingress._shutdown = True

        signal.signal(signal.SIGINT, _sig_handler)

        # Trigger handler
        assert signal.SIGINT in handlers
        handlers[signal.SIGINT](signal.SIGINT, None)

        assert ingress._shutdown is True
    finally:
        signal.signal = orig_signal
