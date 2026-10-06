"""Acceptance tests for Task 1: Kafka Offset discipline + DLQ."""
import json
import os
import time
import pytest

confluent_kafka = pytest.importorskip("confluent_kafka")
from confluent_kafka import TopicPartition
from engine.ingress.kafka import (
    KafkaIngress,
    KafkaSettings,
    KafkaEventError,
)


class FakeMessage:
    def __init__(self, value, offset, partition=0, topic="mfa.migration.failed", key=None):
        self._val = value if isinstance(value, bytes) else json.dumps(value).encode("utf-8")
        self._offset = offset
        self._partition = partition
        self._topic = topic
        self._key = key

    def value(self):
        return self._val

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, messages=None):
        self._messages = list(messages or [])
        self.subscribed = []
        self.seek_calls = []
        self.pause_calls = []
        self.resume_calls = []
        self.commit_calls = []
        self.closed = False
        self.unsubscribed = False

    def subscribe(self, topics):
        self.subscribed = list(topics)

    def unsubscribe(self):
        self.unsubscribed = True

    def poll(self, timeout):
        if self._messages:
            return self._messages.pop(0)
        return None

    def seek(self, partition):
        self.seek_calls.append(partition)

    def pause(self, partitions):
        self.pause_calls.append(list(partitions))

    def resume(self, partitions):
        self.resume_calls.append(list(partitions))

    def commit(self, offsets=None, asynchronous=False):
        self.commit_calls.append((offsets, asynchronous))

    def close(self):
        self.closed = True


class FakePublisher:
    def __init__(self):
        self.published = []
        self.dlq_published = []

    def publish(self, payload):
        self.published.append(payload)

    def publish_dlq(self, raw_value, key=None, reason=""):
        self.dlq_published.append({"raw_value": raw_value, "key": key, "reason": reason})


def _valid_event(event_id="evt-100"):
    return {
        "event_id": event_id,
        "event_type": "MigrationFailed",
        "message": "VM disk migration failed",
    }


def test_failed_event_is_not_committed_past():
    """Fail offset 100, succeed 100 on retry -> committed offset is 101, offset 100 was seeked back and paused."""
    msg100 = FakeMessage(_valid_event("evt-100"), offset=100)
    fake_consumer = FakeConsumer([msg100])
    fake_publisher = FakePublisher()
    settings = KafkaSettings()

    attempts = [0]

    def mock_run_agent(request):
        attempts[0] += 1
        if attempts[0] == 1:
            raise RuntimeError("Transient cluster error")
        return {"status": "COMPLETED", "failure_case_id": "case-100"}

    ingress = KafkaIngress.__new__(KafkaIngress)
    ingress.settings = settings
    ingress.run_agent = mock_run_agent
    ingress.result_publisher = fake_publisher
    ingress.tracker = None
    ingress.consumer = fake_consumer
    ingress._shutdown = False
    ingress._partition_failures = {}
    ingress._retry_deadlines = {}

    # Run one poll: message 100 fails
    # Let run_forever stop after 1 poll
    def poll_with_stop(timeout):
        msg = fake_consumer._messages.pop(0) if fake_consumer._messages else None
        if not fake_consumer._messages:
            ingress._shutdown = True
        return msg

    fake_consumer.poll = poll_with_stop
    ingress.run_forever()

    # 1. Processing failed -> do NOT commit
    assert len(fake_consumer.commit_calls) == 0

    # 2. Offset 100 was seeked back and paused
    assert len(fake_consumer.seek_calls) == 1
    assert fake_consumer.seek_calls[0].topic == "mfa.migration.failed"
    assert fake_consumer.seek_calls[0].partition == 0
    assert fake_consumer.seek_calls[0].offset == 100

    assert len(fake_consumer.pause_calls) == 1
    assert fake_consumer.pause_calls[0][0].partition == 0

    # 3. Simulate retry: partition resumed, msg redelivered and succeeds
    fake_consumer._messages = [msg100]
    ingress._shutdown = False
    fake_consumer.poll = poll_with_stop
    ingress.run_forever()

    # 4. Now succeeded -> offset committed is 101 (msg.offset + 1)
    assert len(fake_consumer.commit_calls) == 1
    committed_tp = fake_consumer.commit_calls[0][0][0]
    assert committed_tp.topic == "mfa.migration.failed"
    assert committed_tp.partition == 0
    assert committed_tp.offset == 101


def test_poison_event_routed_to_dlq():
    """KafkaEventError -> DLQ produce called with reason header, then commit."""
    bad_msg = FakeMessage(b"{invalid-json", offset=200)
    fake_consumer = FakeConsumer([bad_msg])
    fake_publisher = FakePublisher()
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

    def poll_with_stop(timeout):
        msg = fake_consumer._messages.pop(0) if fake_consumer._messages else None
        if not fake_consumer._messages:
            ingress._shutdown = True
        return msg

    fake_consumer.poll = poll_with_stop
    ingress.run_forever()

    # DLQ produce must be called with reason
    assert len(fake_publisher.dlq_published) == 1
    dlq_entry = fake_publisher.dlq_published[0]
    assert dlq_entry["raw_value"] == b"{invalid-json"
    assert "Invalid Kafka JSON payload" in dlq_entry["reason"]

    # Offset 201 must be committed to prevent partition stall
    assert len(fake_consumer.commit_calls) == 1
    assert fake_consumer.commit_calls[0][0][0].offset == 201


def test_retry_exhaustion_parks_in_dlq():
    """5 consecutive failures at the same offset -> DLQ produce + commit."""
    msg = FakeMessage(_valid_event("evt-crash"), offset=300)
    fake_consumer = FakeConsumer()
    fake_publisher = FakePublisher()
    settings = KafkaSettings()

    ingress = KafkaIngress.__new__(KafkaIngress)
    ingress.settings = settings
    ingress.run_agent = lambda req: (_ for _ in ()).throw(RuntimeError("Deterministic crash"))
    ingress.result_publisher = fake_publisher
    ingress.tracker = None
    ingress.consumer = fake_consumer
    ingress._shutdown = False
    ingress._partition_failures = {}
    ingress._retry_deadlines = {}

    # Feed the message 5 times
    for i in range(5):
        fake_consumer._messages = [msg]
        ingress._shutdown = False

        def poll_once(timeout):
            m = fake_consumer._messages.pop(0) if fake_consumer._messages else None
            ingress._shutdown = True
            return m

        fake_consumer.poll = poll_once
        ingress.run_forever()

    # On the 5th failure: DLQ produce was called and offset 301 committed
    assert len(fake_publisher.dlq_published) == 1
    assert fake_publisher.dlq_published[0]["reason"] == "processing retries exhausted"

    assert len(fake_consumer.commit_calls) == 1
    assert fake_consumer.commit_calls[0][0][0].offset == 301


def test_paused_partition_resumes_after_backoff():
    """When retry deadline passes, resume is called on the partition."""
    fake_consumer = FakeConsumer()
    settings = KafkaSettings()

    ingress = KafkaIngress.__new__(KafkaIngress)
    ingress.settings = settings
    ingress.run_agent = lambda req: None
    ingress.result_publisher = None
    ingress.tracker = None
    ingress.consumer = fake_consumer
    ingress._shutdown = False
    ingress._partition_failures = {("mfa.migration.failed", 0): 1}
    # Set deadline in the past
    ingress._retry_deadlines = {("mfa.migration.failed", 0): time.time() - 10.0}

    def poll_stop(timeout):
        ingress._shutdown = True
        return None

    fake_consumer.poll = poll_stop
    ingress.run_forever()

    # Resume must have been called
    assert len(fake_consumer.resume_calls) == 1
    assert fake_consumer.resume_calls[0][0].partition == 0
    # Deadline must be cleared
    assert ("mfa.migration.failed", 0) not in ingress._retry_deadlines


def test_processing_exception_does_not_commit():
    """Generic exception in run_agent -> no commit call."""
    msg = FakeMessage(_valid_event("evt-boom"), offset=400)
    fake_consumer = FakeConsumer([msg])
    settings = KafkaSettings()

    ingress = KafkaIngress.__new__(KafkaIngress)
    ingress.settings = settings
    ingress.run_agent = lambda req: (_ for _ in ()).throw(RuntimeError("DB timeout"))
    ingress.result_publisher = None
    ingress.tracker = None
    ingress.consumer = fake_consumer
    ingress._shutdown = False
    ingress._partition_failures = {}
    ingress._retry_deadlines = {}

    def poll_stop(timeout):
        m = fake_consumer._messages.pop(0) if fake_consumer._messages else None
        ingress._shutdown = True
        return m

    fake_consumer.poll = poll_stop
    ingress.run_forever()

    assert len(fake_consumer.commit_calls) == 0


def test_dlq_publish_failure_recovers_gracefully():
    """If DLQ publishing throws an exception on poison event, do not crash run_forever; seek and pause partition without committing."""
    msg = FakeMessage("not-json", offset=500)
    fake_consumer = FakeConsumer([msg])
    fake_publisher = FakePublisher()

    def failing_publish_dlq(*args, **kwargs):
        raise RuntimeError("DLQ broker timeout")

    fake_publisher.publish_dlq = failing_publish_dlq
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

    def poll_stop(timeout):
        m = fake_consumer._messages.pop(0) if fake_consumer._messages else None
        ingress._shutdown = True
        return m

    fake_consumer.poll = poll_stop
    ingress.run_forever()

    # Offset must NOT have been committed
    assert len(fake_consumer.commit_calls) == 0
    # Consumer should have seeked back and paused partition
    assert len(fake_consumer.seek_calls) == 1
    assert fake_consumer.seek_calls[0].offset == 500
    assert len(fake_consumer.pause_calls) == 1


def test_n2_transient_infrastructure_outage_does_not_dlq_or_commit():
    """N2: Database / tracker outages must back off without burning poison-pill budget or DLQing."""
    from engine.contracts import DurableStateError

    fake_msg = FakeMessage(_valid_event("evt-infra-outage"), offset=100)
    fake_consumer = FakeConsumer([fake_msg] * 7)
    fake_publisher = FakePublisher()

    ingress = KafkaIngress.__new__(KafkaIngress)
    ingress.settings = KafkaSettings()
    ingress.run_agent = lambda req: (_ for _ in ()).throw(DurableStateError("Postgres connection dropped"))
    ingress.result_publisher = fake_publisher
    ingress.tracker = None
    ingress.consumer = fake_consumer
    ingress._shutdown = False
    ingress._partition_failures = {}
    ingress._infra_failures = {}
    ingress._retry_deadlines = {}

    calls = 0
    def poll_infra(timeout):
        nonlocal calls
        calls += 1
        if calls > 6:
            ingress._shutdown = True
            return None
        return fake_msg

    fake_consumer.poll = poll_infra
    ingress.run_forever()

    # Zero commits — offset must stay uncommitted
    assert len(fake_consumer.commit_calls) == 0
    # Zero DLQ publishes — valid event must NEVER be DLQ'd during DB outage
    assert len(fake_publisher.dlq_published) == 0
    # Partition failures (poison-pill budget) must NOT have been burned
    part_key = ("mfa.migration.failed", 0)
    assert ingress._partition_failures.get(part_key, 0) == 0
    # Infra failures tracked with backoff capped at 60s
    assert ingress._infra_failures.get(part_key) == 6
    # Partition paused and seeked
    assert len(fake_consumer.seek_calls) >= 6
    assert fake_consumer.seek_calls[-1].offset == 100


def test_n2_deterministic_error_burns_5_strike_budget_and_dlqs():
    """N2: Deterministic application bugs burn 5-strike budget and route to DLQ on exhaustion."""
    fake_msg = FakeMessage(_valid_event("evt-deterministic-bug"), offset=200)
    fake_consumer = FakeConsumer([fake_msg] * 6)
    fake_publisher = FakePublisher()

    ingress = KafkaIngress.__new__(KafkaIngress)
    ingress.settings = KafkaSettings()
    ingress.run_agent = lambda req: (_ for _ in ()).throw(ValueError("Deterministic bug in custom logic"))
    ingress.result_publisher = fake_publisher
    ingress.tracker = None
    ingress.consumer = fake_consumer
    ingress._shutdown = False
    ingress._partition_failures = {}
    ingress._infra_failures = {}
    ingress._retry_deadlines = {}

    calls = 0
    def poll_bug(timeout):
        nonlocal calls
        calls += 1
        if calls > 5:
            ingress._shutdown = True
            return None
        return fake_msg

    fake_consumer.poll = poll_bug
    ingress.run_forever()

    # Exactly 1 DLQ publish after 5 strikes
    assert len(fake_publisher.dlq_published) == 1
    assert "processing retries exhausted" in fake_publisher.dlq_published[0]["reason"]
    # Offset committed after DLQ to prevent pipeline freeze
    assert len(fake_consumer.commit_calls) == 1
    assert fake_consumer.commit_calls[0][0][0].offset == 201


@pytest.mark.skipif(not os.getenv("KAFKA_BOOTSTRAP_SERVERS"), reason="Requires running Kafka broker")
def test_e2e_failed_then_success_no_loss():
    """Integration test: failing event followed by healthy event causes no loss."""
    from confluent_kafka import Producer, Consumer
    from engine.ingress.kafka import KafkaResultPublisher

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    ts = int(time.time() * 1000)
    settings = KafkaSettings(
        bootstrap_servers=bootstrap,
        consumer_group=f"test-e2e-discipline-{ts}",
        auto_offset_reset="earliest",
        input_topic=f"test.failed.{ts}",
        result_topic=f"test.result.{ts}",
        dlq_topic=f"test.dlq.{ts}",
    )
    producer = Producer({"bootstrap.servers": bootstrap})
    # Produce 1 malformed poison pill and 1 valid event
    poison = b"poison-not-json"
    valid = json.dumps(_valid_event("evt-e2e-001")).encode("utf-8")
    producer.produce(settings.input_topic, value=poison)
    producer.produce(settings.input_topic, value=valid)
    producer.flush(5.0)

    publisher = KafkaResultPublisher(settings)
    processed = []
    ingress = KafkaIngress(settings=settings, run_agent=lambda req: processed.append(req), result_publisher=publisher)
    ingress.consumer.assign([TopicPartition(settings.input_topic, 0, 0)])

    from engine.ingress.kafka import parse_event

    count = 0
    start = time.time()
    while count < 2 and time.time() - start < 10.0:
        msg = ingress.consumer.poll(1.0)
        if not msg:
            continue
        if msg.error():
            continue
        try:
            event = parse_event(msg.value())
            processed.append(event)
            ingress.consumer.commit(offsets=[TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)], asynchronous=False)
        except KafkaEventError as exc:
            ingress._produce_dlq(msg, str(exc))
            ingress.consumer.commit(offsets=[TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)], asynchronous=False)
        count += 1
    ingress.close()

    assert count == 2
    assert len(processed) == 1
    assert processed[0]["event_id"] == "evt-e2e-001"


def test_kafka_settings_tls_and_sasl_configuration():
    """Verify that KafkaSettings correctly parses TLS and SASL environment variables and builds config."""
    settings = KafkaSettings(
        bootstrap_servers="kafka.corp.internal:9093",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username="mfa-service",
        sasl_password="secret-password",
        ssl_ca_location="/etc/pki/ca.crt",
    )
    conf = settings.to_kafka_config({"group.id": "test-group"})
    assert conf["bootstrap.servers"] == "kafka.corp.internal:9093"
    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.mechanism"] == "SCRAM-SHA-512"
    assert conf["sasl.username"] == "mfa-service"
    assert conf["sasl.password"] == "secret-password"
    assert conf["ssl.ca.location"] == "/etc/pki/ca.crt"
    assert conf["group.id"] == "test-group"

    # Default PLAINTEXT settings should not populate security or SASL fields
    default_conf = KafkaSettings().to_kafka_config()
    assert "security.protocol" not in default_conf
    assert "sasl.mechanism" not in default_conf


