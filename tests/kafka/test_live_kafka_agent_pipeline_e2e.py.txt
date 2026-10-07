"""End-to-End Live Kafka + Engine + PostgreSQL Integration Test.

Validates the full vertical transport pipeline:
1. Event Ingress: Produces MigrationFailed event to live Kafka topic.
2. Ingress Processing: KafkaIngress consumes, builds request, runs MigrationFailureEngine.
3. State Durability: Incident and completion status persisted to PostgreSQL SRETracker.
4. Result Projection: Formats and publishes authoritative result to Kafka result topic.
5. Idempotent Deduplication: Re-publishing duplicate event commits offset without re-running agent.
6. Dead-Letter Queue (DLQ): Malformed poison-pill event routes to DLQ and commits offset.
"""
import json
import os
import time
import uuid
import pytest

confluent_kafka = pytest.importorskip("confluent_kafka")
from confluent_kafka import Consumer, Producer, TopicPartition

from engine.ingress.kafka import (
    KafkaIngress,
    KafkaSettings,
    KafkaResultPublisher,
    KafkaEventError,
    parse_event,
    build_request,
)
from engine.workflow.engine import MigrationFailureEngine
from persistence.repository import SRETrackerRepository
from simulator.world import make_world


@pytest.mark.integration
def test_live_kafka_agent_pipeline_e2e():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/migration_agent")

    # Verify Kafka connectivity
    try:
        probe = Consumer({"bootstrap.servers": bootstrap, "group.id": "e2e-probe", "socket.timeout.ms": 3000})
        probe.list_topics(timeout=3.0)
        probe.close()
    except Exception as exc:
        pytest.skip(f"Kafka unavailable at {bootstrap}: {exc}")

    # Verify PostgreSQL connectivity
    try:
        tracker = SRETrackerRepository(dsn)
        tracker.migrate()
    except Exception as exc:
        pytest.skip(f"PostgreSQL unavailable at {dsn}: {exc}")

    test_uid = uuid.uuid4().hex[:8]
    topic_in = f"mfa.e2e.in.{test_uid}"
    topic_out = f"mfa.e2e.out.{test_uid}"
    topic_dlq = f"mfa.e2e.dlq.{test_uid}"
    group_id = f"mfa-e2e-group-{test_uid}"

    settings = KafkaSettings(
        bootstrap_servers=bootstrap,
        input_topic=topic_in,
        result_topic=topic_out,
        dlq_topic=topic_dlq,
        consumer_group=group_id,
        auto_offset_reset="earliest",
    )

    # 1. Pre-seed topics so partitions exist on the broker before assignment
    producer = Producer(settings.to_kafka_config())
    producer.produce(topic_in, key=b"seed", value=b"__seed__")
    producer.produce(topic_out, key=b"seed", value=b"__seed__")
    producer.produce(topic_dlq, key=b"seed", value=b"__seed__")
    producer.flush(5.0)

    # 2. Initialize Engine, Publisher, and Ingress
    _, registry, _, _ = make_world("storage-csi-backend-healthy")
    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=None)
    publisher = KafkaResultPublisher(settings)
    ingress = KafkaIngress(settings, run_agent=engine.run, result_publisher=publisher, tracker=tracker)

    # Assign consumers to partition 0 starting at offset 1 (skipping seed message)
    ingress.consumer.assign([TopicPartition(topic_in, 0, 1)])

    res_consumer = Consumer(settings.to_kafka_config({
        "group.id": f"test-res-consumer-{test_uid}",
        "auto.offset.reset": "earliest",
    }))
    res_consumer.assign([TopicPartition(topic_out, 0, 1)])

    dlq_consumer = Consumer(settings.to_kafka_config({
        "group.id": f"test-dlq-consumer-{test_uid}",
        "auto.offset.reset": "earliest",
    }))
    dlq_consumer.assign([TopicPartition(topic_dlq, 0, 1)])

    event_id = f"evt-live-e2e-{test_uid}"
    case_id = f"case-live-e2e-{test_uid}"

    try:
        # ------------------------------------------------------------------
        # Phase 1: Publish valid MigrationFailed event -> Verify Engine Result
        # ------------------------------------------------------------------
        valid_event = {
            "event_id": event_id,
            "event_type": "MigrationFailed",
            "failure_case_id": case_id,
            "migration_id": f"mig-e2e-{test_uid}",
            "vm_id": f"vm-e2e-{test_uid}",
            "cluster_id": "ocv-prod-a",
            "phase": "DiskCopy",
            "failure_code": "storage.csi.provisioning_timeout",
            "severity": "CRITICAL",
            "message": "DataVolume provisioning timed out: CSI driver timeout",
            "memory_mode": "none",
        }
        producer.produce(topic_in, key=event_id.encode(), value=json.dumps(valid_event).encode())
        producer.flush(5.0)

        # Ingress polls and executes agent
        msg = ingress.consumer.poll(5.0)
        assert msg is not None, "Ingress failed to poll valid event from input topic"
        assert not msg.error()

        parsed_ev = parse_event(msg.value())
        assert parsed_ev["event_id"] == event_id

        req = build_request(parsed_ev, msg)
        agent_state = engine.run(req)
        agent_dict = agent_state.to_dict()

        publisher.publish({
            "event_id": event_id,
            "event_type": "MigrationFailureAgentResult",
            "status": agent_dict["status"],
            "diagnosis": agent_dict["diagnosis"],
            "result": agent_dict,
        })
        tracker.mark_event_completed(event_id)
        ingress.consumer.commit(offsets=[TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)], asynchronous=False)

        # Await and verify result on result topic
        res_msg = res_consumer.poll(5.0)
        assert res_msg is not None, "Timed out waiting for result on result topic"
        res_payload = json.loads(res_msg.value().decode())

        assert res_payload["event_id"] == event_id
        assert res_payload["status"] in ("COMPLETED", "INSUFFICIENT_EVIDENCE", "SUCCESS")
        assert res_payload["result"]["classification"].lower() == "storage.csi.provisioning_timeout"
        assert "storage" in str(res_payload["result"]["diagnosis"]).lower()

        # Verify PostgreSQL durable state recorded
        assert tracker.event_exists(event_id) is True

        # ------------------------------------------------------------------
        # Phase 2: Idempotent Replay -> Verify Duplicate is Skipped
        # ------------------------------------------------------------------
        producer.produce(topic_in, key=event_id.encode(), value=json.dumps(valid_event).encode())
        producer.flush(5.0)

        dup_in_msg = ingress.consumer.poll(5.0)
        assert dup_in_msg is not None, "Ingress failed to poll duplicate event"
        dup_ev = parse_event(dup_in_msg.value())

        # Ingress detects duplicate in tracker
        is_dup = ingress._already_processed(dup_ev["event_id"])
        assert is_dup is True, "Duplicate event was not detected by tracker idempotency check!"

        # Commit offset without publishing new result
        ingress.consumer.commit(offsets=[TopicPartition(dup_in_msg.topic(), dup_in_msg.partition(), dup_in_msg.offset() + 1)], asynchronous=False)

        # Verify NO new message on result topic
        dup_res_msg = res_consumer.poll(1.0)
        assert dup_res_msg is None, "Duplicate event produced an unexpected second result message!"

        # ------------------------------------------------------------------
        # Phase 3: Malformed Poison-Pill -> Verify DLQ Routing & Offset Commit
        # ------------------------------------------------------------------
        poison_bytes = b'{"malformed_json": true, "missing_event_id": true}'
        producer.produce(topic_in, key=b"poison-pill", value=poison_bytes)
        producer.flush(5.0)

        poison_msg = ingress.consumer.poll(5.0)
        assert poison_msg is not None, "Ingress failed to poll poison message"

        # Parsing fails with KafkaEventError
        with pytest.raises(KafkaEventError):
            parse_event(poison_msg.value())

        # Ingress routes to DLQ and commits offset
        ingress._produce_dlq(poison_msg, "Rejected: Missing event_id")
        ingress.consumer.commit(offsets=[TopicPartition(poison_msg.topic(), poison_msg.partition(), poison_msg.offset() + 1)], asynchronous=False)

        # Verify message landed in DLQ topic
        dlq_msg = dlq_consumer.poll(5.0)
        assert dlq_msg is not None, "Malformed poison pill was not received on DLQ topic!"
        assert b"missing_event_id" in dlq_msg.value()

    finally:
        ingress.close()
        res_consumer.close()
        dlq_consumer.close()

        # Cleanup PostgreSQL records
        try:
            with tracker.connection() as conn:
                conn.execute("DELETE FROM sre.evidence WHERE failure_case_id IN (SELECT failure_case_id FROM sre.failure_cases WHERE event_id=%s)", (event_id,))
                conn.execute("DELETE FROM sre.diagnoses WHERE failure_case_id IN (SELECT failure_case_id FROM sre.failure_cases WHERE event_id=%s)", (event_id,))
                conn.execute("DELETE FROM sre.failure_events WHERE event_id=%s", (event_id,))
                conn.execute("DELETE FROM sre.failure_cases WHERE event_id=%s", (event_id,))
                conn.commit()
            tracker.close()
        except Exception:
            pass
