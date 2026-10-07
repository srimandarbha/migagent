from __future__ import annotations

import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from engine.observability.prometheus_exporter import GLOBAL_METRICS

try:
    from confluent_kafka import Consumer, Producer, KafkaException, Message, TopicPartition
except ImportError:  # pragma: no cover
    Consumer = Producer = None
    KafkaException = Exception
    Message = Any

    class TopicPartition:  # type: ignore[no-redef]
        def __init__(self, topic: str, partition: int = 0, offset: int = -1001) -> None:
            self.topic = topic
            self.partition = partition
            self.offset = offset

        def __eq__(self, other: Any) -> bool:
            return (
                isinstance(other, TopicPartition)
                and (self.topic, self.partition, self.offset) == (other.topic, other.partition, other.offset)
            )

        def __repr__(self) -> str:
            return f"TopicPartition({self.topic}, {self.partition}, {self.offset})"

LOG = logging.getLogger("migration-failure-agent.kafka")


class KafkaConfigurationError(RuntimeError):
    pass


class KafkaDeliveryError(RuntimeError):
    pass


class KafkaEventError(ValueError):
    def __init__(self, message: str, event_id: str | None = None) -> None:
        super().__init__(message)
        self.event_id = event_id


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = "127.0.0.1:9092"
    input_topic: str = "mfa.migration.failed"
    result_topic: str = "mfa.agent.result"
    dlq_topic: str = "mfa.migration.failed.dlq"
    consumer_group: str = "migration-failure-agent"
    auto_offset_reset: str = "earliest"
    poll_timeout_seconds: float = 1.0
    publish_timeout_seconds: float = 10.0
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = ""
    sasl_username: str = ""
    sasl_password: str = ""
    ssl_ca_location: str = ""
    ssl_certificate_location: str = ""
    ssl_key_location: str = ""

    @classmethod
    def from_env(cls) -> "KafkaSettings":
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            input_topic=os.getenv("KAFKA_MIGRATION_FAILED_TOPIC", cls.input_topic),
            result_topic=os.getenv("KAFKA_AGENT_RESULT_TOPIC", cls.result_topic),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", cls.dlq_topic),
            consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", cls.consumer_group),
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", cls.auto_offset_reset),
            poll_timeout_seconds=float(os.getenv("KAFKA_POLL_TIMEOUT_SECONDS", str(cls.poll_timeout_seconds))),
            publish_timeout_seconds=float(os.getenv("KAFKA_PUBLISH_TIMEOUT_SECONDS", str(cls.publish_timeout_seconds))),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", cls.security_protocol),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", cls.sasl_mechanism),
            sasl_username=os.getenv("KAFKA_SASL_USERNAME", cls.sasl_username),
            sasl_password=os.getenv("KAFKA_SASL_PASSWORD", cls.sasl_password),
            ssl_ca_location=os.getenv("KAFKA_SSL_CA_LOCATION", cls.ssl_ca_location),
            ssl_certificate_location=os.getenv("KAFKA_SSL_CERTIFICATE_LOCATION", cls.ssl_certificate_location),
            ssl_key_location=os.getenv("KAFKA_SSL_KEY_LOCATION", cls.ssl_key_location),
        )

    def to_kafka_config(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        conf: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
        }
        if self.security_protocol and self.security_protocol.upper() != "PLAINTEXT":
            conf["security.protocol"] = self.security_protocol.upper()
        if self.sasl_mechanism:
            conf["sasl.mechanism"] = self.sasl_mechanism.upper()
        if self.sasl_username:
            conf["sasl.username"] = self.sasl_username
        if self.sasl_password:
            conf["sasl.password"] = self.sasl_password
        if self.ssl_ca_location:
            conf["ssl.ca.location"] = self.ssl_ca_location
        if self.ssl_certificate_location:
            conf["ssl.certificate.location"] = self.ssl_certificate_location
        if self.ssl_key_location:
            conf["ssl.key.location"] = self.ssl_key_location
        if extra:
            conf.update(extra)
        return conf


REQUIRED_EVENT_FIELDS = ("event_id", "event_type")


def validate_event(event: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise KafkaEventError("Kafka payload must be a JSON object")
    missing = [key for key in REQUIRED_EVENT_FIELDS if not event.get(key)]
    if missing:
        raise KafkaEventError(f"Missing required event fields: {', '.join(missing)}", event_id=event.get("event_id"))
    if event["event_type"] != "MigrationFailed":
        raise KafkaEventError(f"Unsupported event_type: {event['event_type']}", event_id=event.get("event_id"))
    return event


def parse_event(raw: bytes | str) -> dict[str, Any]:
    try:
        value = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        return validate_event(json.loads(value))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise KafkaEventError(f"Invalid Kafka JSON payload: {exc}") from exc


def _event_time(value: Any) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.now(timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def build_request(event: dict[str, Any], message: Message) -> dict[str, Any]:
    """Convert Kafka transport data into the framework's plain request contract."""
    from engine.security.sanitizer import sanitize_object
    sanitized_event = sanitize_object(event)
    return {
        "event": sanitized_event,
        "failure_case_id": sanitized_event.get("failure_case_id"),
        "kafka": {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "event_time": _event_time(sanitized_event.get("event_time")).isoformat(),
        },
    }


def is_transient_infrastructure_error(exc: Exception) -> bool:
    """Checks whether an exception represents a transient infrastructure outage (e.g. database down).

    Transient outages must NOT burn the 5-strike poison-pill budget or send valid events to DLQ.
    Instead, they must back off (up to 60s), pause/seek, and retry until infrastructure recovers.
    """
    from ..contracts import DurableStateError
    if isinstance(exc, DurableStateError):
        return True
    exc_type = type(exc).__name__
    if exc_type in (
        "DurableStateError",
        "OperationalError",
        "InterfaceError",
        "DatabaseError",
        "ConnectionRefusedError",
        "ConnectionResetError",
    ):
        return True
    msg = str(exc).lower()
    if any(k in msg for k in ("tracker", "connection refused", "psycopg", "could not connect to server", "connection closed", "broken pipe")):
        return True
    if getattr(exc, "__cause__", None) and is_transient_infrastructure_error(exc.__cause__):
        return True
    return False


class KafkaIngress:
    """Kafka adapter. It knows Kafka; the agent engine does not."""

    def __init__(
        self,
        settings: KafkaSettings,
        run_agent: Callable[[dict[str, Any]], Any],
        result_publisher: "KafkaResultPublisher | None" = None,
        tracker: Any | None = None,
    ) -> None:
        if Consumer is None:
            raise KafkaConfigurationError("confluent-kafka is required for Kafka ingress")
        self.settings = settings
        self.run_agent = run_agent
        self.result_publisher = result_publisher
        self.tracker = tracker
        self._shutdown = False
        self._partition_failures: dict[tuple[str, int], int] = {}
        self._infra_failures: dict[tuple[str, int], int] = {}
        self._retry_deadlines: dict[tuple[str, int], float] = {}

        try:
            def _sig_handler(signum: int, frame: Any) -> None:
                LOG.info("Received termination signal %s; initiating graceful shutdown", signum)
                self._shutdown = True

            signal.signal(signal.SIGTERM, _sig_handler)
            signal.signal(signal.SIGINT, _sig_handler)
        except (ValueError, AttributeError):
            pass

        consumer_conf = settings.to_kafka_config({
            "group.id": settings.consumer_group,
            "auto.offset.reset": settings.auto_offset_reset,
            "enable.auto.commit": False,
        })
        self.consumer = Consumer(consumer_conf)

    def stop(self) -> None:
        self._shutdown = True

    def close(self) -> None:
        self._shutdown = True
        if self.result_publisher and hasattr(self.result_publisher, "producer"):
            try:
                self.result_publisher.producer.flush(timeout=5.0)
            except Exception:
                pass
        try:
            self.consumer.unsubscribe()
        except Exception:
            pass
        try:
            self.consumer.close()
        except Exception:
            pass

    def run_forever(self) -> None:
        if not hasattr(self, "_partition_failures"):
            self._partition_failures = {}
        if not hasattr(self, "_infra_failures"):
            self._infra_failures = {}
        if not hasattr(self, "_retry_deadlines"):
            self._retry_deadlines = {}
        self.consumer.subscribe([self.settings.input_topic])
        LOG.info("Listening on %s as group %s", self.settings.input_topic, self.settings.consumer_group)

        try:
            while not self._shutdown:
                # Top of poll loop: resume partitions whose retry deadlines have passed
                now = time.time()
                to_resume = [
                    (topic, part)
                    for (topic, part), deadline in list(self._retry_deadlines.items())
                    if now >= deadline
                ]
                for topic, part in to_resume:
                    self.consumer.resume([TopicPartition(topic, part)])
                    self._retry_deadlines.pop((topic, part), None)

                msg = self.consumer.poll(self.settings.poll_timeout_seconds)
                if msg is None:
                    continue
                if msg.error():
                    LOG.error("Kafka consumer error: %s", msg.error())
                    continue

                part_key = (msg.topic(), msg.partition())

                try:
                    event = parse_event(msg.value())
                    request = build_request(event, msg)

                    if self._already_processed(event["event_id"]):
                        LOG.info("Skipping duplicate event_id=%s at offset=%s", event["event_id"], msg.offset())
                        GLOBAL_METRICS.record_event_processed("SKIPPED")
                        self.consumer.commit(
                            offsets=[TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)],
                            asynchronous=False,
                        )
                        self._partition_failures.pop(part_key, None)
                        self._infra_failures.pop(part_key, None)
                        continue

                    t_start = time.time()
                    state = self.run_agent(request)
                    result = state.to_dict() if hasattr(state, "to_dict") else state
                    errors = getattr(state, "errors", None) or (result.get("errors") if isinstance(result, dict) else [])
                    trace = getattr(state, "trace", None) or (result.get("trace") if isinstance(result, dict) else [])
                    projection = {
                        "schema_version": 1,
                        "status": result.get("status"),
                        "next_step": result.get("next_step"),
                        "diagnosis": result.get("diagnosis"),
                        "decision_readiness": result.get("decision_readiness"),
                        "capability_coverage": result.get("capability_coverage"),
                        "recurrence": result.get("recurrence"),
                        "learning": result.get("learning"),
                        "recommendation": result.get("recommendation"),
                        "error_count": len(errors or []),
                        "trace_tail": list((trace or [])[-20:]),
                        "failure_case_id": result.get("failure_case_id"),
                        "agent_version": result.get("agent_version"),
                        "policy_version": result.get("policy_version"),
                        "classification": result.get("classification"),
                    }
                    result_payload = {
                        "event_id": event["event_id"],
                        "event_type": "MigrationFailureAgentResult",
                        "event_time": datetime.now(timezone.utc).isoformat(),
                        "failure_case_id": result.get("failure_case_id"),
                        "status": result.get("status"),
                        "next_step": result.get("next_step"),
                        "diagnosis": result.get("diagnosis"),
                        "source": {
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                        },
                        "agent_version": result.get("agent_version"),
                        "policy_version": result.get("policy_version"),
                        "result": projection,
                    }

                    if self.result_publisher:
                        self.result_publisher.publish(result_payload)

                    if self.tracker and hasattr(self.tracker, "mark_event_completed"):
                        self.tracker.mark_event_completed(event["event_id"])

                    # Commit up to last success: offset + 1
                    self.consumer.commit(
                        offsets=[TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)],
                        asynchronous=False,
                    )
                    self._partition_failures.pop(part_key, None)
                    self._infra_failures.pop(part_key, None)
                    GLOBAL_METRICS.record_event_processed(
                        result.get("status", "COMPLETED"),
                        duration_seconds=time.time() - t_start,
                    )
                    diag_raw = result.get("diagnosis")
                    diag_st = diag_raw.get("status") if isinstance(diag_raw, dict) else str(diag_raw or "UNKNOWN")
                    GLOBAL_METRICS.record_diagnosis(result.get("classification") or "UNKNOWN", diag_st)
                    LOG.info("Successfully processed event_id=%s at offset=%s", event["event_id"], msg.offset())

                except KafkaEventError as exc:
                    LOG.error("Rejected Kafka event at offset=%s: %s", msg.offset(), exc)
                    try:
                        self._produce_dlq(msg, str(exc))
                        GLOBAL_METRICS.record_event_processed("DLQ")
                        self.consumer.commit(
                            offsets=[TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)],
                            asynchronous=False,
                        )
                        self._partition_failures.pop(part_key, None)
                        self._infra_failures.pop(part_key, None)
                    except Exception as dlq_exc:
                        LOG.error("Failed to produce to DLQ for rejected event at offset=%s: %s", msg.offset(), dlq_exc)
                        self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                        self.consumer.pause([TopicPartition(msg.topic(), msg.partition())])
                        self._retry_deadlines[part_key] = time.time() + 2.0

                except Exception as exc:
                    # Do NOT commit on processing failure
                    if is_transient_infrastructure_error(exc):
                        # Transient infrastructure outage (database / tracker down)
                        # Does NOT burn 5-strike poison-pill budget and does NOT route to DLQ!
                        GLOBAL_METRICS.record_infra_outage()
                        infra_count = self._infra_failures.get(part_key, 0) + 1
                        self._infra_failures[part_key] = infra_count
                        # Exponential backoff capped at 60s
                        backoff = min(60.0, float(2 ** min(infra_count - 1, 6)))
                        self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                        self.consumer.pause([TopicPartition(msg.topic(), msg.partition())])
                        self._retry_deadlines[part_key] = time.time() + backoff
                        LOG.error(
                            "Transient infrastructure outage (%s: %s) at offset %s; paused partition %s (attempt %d, no DLQ exhaustion); retrying in %.1fs",
                            type(exc).__name__, exc, msg.offset(), part_key, infra_count, backoff,
                        )
                    else:
                        LOG.exception("Agent processing failed for offset=%s", msg.offset())
                        count = self._partition_failures.get(part_key, 0) + 1
                        self._partition_failures[part_key] = count

                        if count < 5:
                            self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                            self.consumer.pause([TopicPartition(msg.topic(), msg.partition())])
                            backoff = min(16.0, float(2 ** (count - 1)))
                            self._retry_deadlines[part_key] = time.time() + backoff
                            LOG.warning(
                                "Partition %s paused after failure %d/5 at offset %s; retrying in %.1fs",
                                part_key, count, msg.offset(), backoff,
                            )
                        else:
                            LOG.critical(
                                "Partition %s exhausted 5 consecutive retries at offset %s; routing to DLQ",
                                part_key, msg.offset(),
                            )
                            try:
                                self._produce_dlq(msg, "processing retries exhausted")
                                GLOBAL_METRICS.record_event_processed("DLQ")
                                self.consumer.commit(
                                    offsets=[TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)],
                                    asynchronous=False,
                                )
                                self._partition_failures.pop(part_key, None)
                                self._retry_deadlines.pop(part_key, None)
                            except Exception as dlq_exc:
                                LOG.error("Failed to produce to DLQ after retries exhausted at offset=%s: %s", msg.offset(), dlq_exc)
                                self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                                self.consumer.pause([TopicPartition(msg.topic(), msg.partition())])
                                self._retry_deadlines[part_key] = time.time() + 5.0
        finally:
            self.close()

    def _produce_dlq(self, msg: Message, reason: str) -> None:
        raw_val = msg.value()
        raw_bytes = raw_val if isinstance(raw_val, bytes) else str(raw_val).encode("utf-8")
        key = getattr(msg, "key", lambda: None)()
        key_bytes = key if isinstance(key, bytes) else (str(key).encode("utf-8") if key else None)
        if self.result_publisher:
            self.result_publisher.publish_dlq(raw_bytes, key=key_bytes, reason=reason)
        else:
            LOG.warning("No result_publisher configured; DLQ event dropped: reason=%s", reason)

    def _already_processed(self, event_id: str) -> bool:
        if not self.tracker:
            return False
        try:
            return bool(self.tracker.event_exists(event_id))
        except AttributeError:
            return False
        except Exception:
            LOG.exception("Could not check event idempotency; failing closed")
            raise


class KafkaResultPublisher:
    def __init__(self, settings: KafkaSettings) -> None:
        if Producer is None:
            raise KafkaConfigurationError("confluent-kafka is required for Kafka result publishing")
        self.settings = settings
        self.producer = Producer(settings.to_kafka_config())

    def publish(self, payload: dict[str, Any]) -> None:
        from engine.security.sanitizer import sanitize_object

        sanitized = sanitize_object(payload)
        value = json.dumps(sanitized, default=str, separators=(",", ":")).encode("utf-8")
        key = str(payload.get("event_id") or payload.get("source", {}).get("offset") or "").encode("utf-8")

        delivery_errors: list[Any] = []

        def _on_delivery(err: Any, msg: Any) -> None:
            if err is not None:
                delivery_errors.append(err)

        self.producer.produce(
            self.settings.result_topic,
            value=value,
            key=key,
            on_delivery=_on_delivery,
        )
        remaining = self.producer.flush(timeout=self.settings.publish_timeout_seconds)
        if delivery_errors:
            raise KafkaDeliveryError(f"Result delivery failed: {delivery_errors[0]}")
        if remaining > 0:
            raise KafkaDeliveryError(
                f"Result flush timed out after {self.settings.publish_timeout_seconds}s "
                f"({remaining} messages unacknowledged)"
            )

    def publish_dlq(self, raw_value: bytes | str, key: bytes | str | None = None, reason: str = "") -> None:
        val = raw_value if isinstance(raw_value, bytes) else str(raw_value).encode("utf-8")
        k = key if isinstance(key, bytes) else (str(key).encode("utf-8") if key else None)
        headers = [("dlq.reason", reason.encode("utf-8"))]
        delivery_errors: list[Any] = []

        def _on_delivery(err: Any, msg: Any) -> None:
            if err is not None:
                delivery_errors.append(err)

        self.producer.produce(
            self.settings.dlq_topic,
            value=val,
            key=k,
            headers=headers,
            on_delivery=_on_delivery,
        )
        remaining = self.producer.flush(timeout=self.settings.publish_timeout_seconds)
        if delivery_errors:
            raise KafkaDeliveryError(f"DLQ delivery failed: {delivery_errors[0]}")
        if remaining > 0:
            raise KafkaDeliveryError(
                f"DLQ flush timed out after {self.settings.publish_timeout_seconds}s "
                f"({remaining} messages unacknowledged)"
            )
