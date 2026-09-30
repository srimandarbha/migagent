from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

try:
    from confluent_kafka import Consumer, Producer, KafkaException, Message
except ImportError:  # pragma: no cover
    Consumer = Producer = None
    KafkaException = Exception
    Message = Any

LOG = logging.getLogger("migration-failure-agent.kafka")


class KafkaConfigurationError(RuntimeError):
    pass


class KafkaEventError(ValueError):
    pass


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = "127.0.0.1:9092"
    input_topic: str = "mfa.migration.failed"
    result_topic: str = "mfa.agent.result"
    consumer_group: str = "migration-failure-agent"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    poll_timeout_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> "KafkaSettings":
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            input_topic=os.getenv("KAFKA_MIGRATION_FAILED_TOPIC", cls.input_topic),
            result_topic=os.getenv("KAFKA_AGENT_RESULT_TOPIC", cls.result_topic),
            consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", cls.consumer_group),
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", cls.auto_offset_reset),
            enable_auto_commit=False,
            poll_timeout_seconds=float(os.getenv("KAFKA_POLL_TIMEOUT_SECONDS", str(cls.poll_timeout_seconds))),
        )


REQUIRED_EVENT_FIELDS = ("event_id", "event_type")


def validate_event(event: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise KafkaEventError("Kafka payload must be a JSON object")
    missing = [key for key in REQUIRED_EVENT_FIELDS if not event.get(key)]
    if missing:
        raise KafkaEventError(f"Missing required event fields: {', '.join(missing)}")
    if event["event_type"] != "MigrationFailed":
        raise KafkaEventError(f"Unsupported event_type: {event['event_type']}")
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
    return {
        "event": event,
        "failure_case_id": event.get("failure_case_id"),
        "kafka": {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "event_time": _event_time(event.get("event_time")).isoformat(),
        },
    }


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
        self.consumer = Consumer({
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.consumer_group,
            "auto.offset.reset": settings.auto_offset_reset,
            "enable.auto.commit": False,
        })

    def close(self) -> None:
        self.consumer.close()

    def run_forever(self) -> None:
        self.consumer.subscribe([self.settings.input_topic])
        LOG.info("Listening on %s as group %s", self.settings.input_topic, self.settings.consumer_group)
        try:
            while True:
                msg = self.consumer.poll(self.settings.poll_timeout_seconds)
                if msg is None:
                    continue
                if msg.error():
                    LOG.error("Kafka consumer error: %s", msg.error())
                    continue
                try:
                    event = parse_event(msg.value())
                    request = build_request(event, msg)
                    if self._already_processed(event["event_id"]):
                        LOG.info("Skipping duplicate event_id=%s", event["event_id"])
                        self.consumer.commit(message=msg, asynchronous=False)
                        continue

                    state = self.run_agent(request)
                    result = state.to_dict() if hasattr(state, "to_dict") else state
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
                        "result": result,
                    }
                    if self.result_publisher:
                        self.result_publisher.publish(result_payload)
                    self.consumer.commit(message=msg, asynchronous=False)
                    LOG.info("Processed event_id=%s", event["event_id"])
                except KafkaEventError as exc:
                    LOG.error("Rejected Kafka event: %s", exc)
                    # Commit malformed/unsupported events so one poison message does not block the partition.
                    self.consumer.commit(message=msg, asynchronous=False)
                except Exception:
                    # Do not commit on processing failure. Kafka will redeliver after restart/rebalance.
                    LOG.exception("Agent processing failed; offset will not be committed")
        finally:
            self.close()

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
        self.producer = Producer({"bootstrap.servers": settings.bootstrap_servers})

    def publish(self, payload: dict[str, Any]) -> None:
        value = json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8")
        self.producer.produce(
            self.settings.result_topic,
            value=value,
            key=str(payload["event_id"]).encode("utf-8"),
        )
        self.producer.flush()
