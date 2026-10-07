#!/usr/bin/env python3
"""Production Dead Letter Queue (DLQ) inspection and replay utility for MFA.

Features:
- Inspect events parked in `mfa.migration.failed.dlq` with reasons and timestamps.
- Filter DLQ events by error reason or event ID.
- Replay events back to the primary topic `mfa.migration.failed` with replay headers.
- Safe-by-default: runs in dry-run mode unless --execute is explicitly specified.
- Only commits DLQ offset when --execute is enabled and produce succeeds.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engine.ingress.kafka import KafkaSettings, parse_event

try:
    from confluent_kafka import Consumer, Producer, TopicPartition
except ImportError:
    Consumer = Producer = None  # type: ignore

LOG = logging.getLogger("mfa-dlq-replay")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect and replay messages from the MFA Dead Letter Queue (DLQ)."
    )
    parser.add_argument(
        "--mode",
        choices=["list", "replay"],
        default="list",
        help="Action to perform: 'list' inspects messages, 'replay' republishes to input topic (default: list).",
    )
    parser.add_argument(
        "--reason-filter",
        type=str,
        default=None,
        help="Regex pattern to filter DLQ messages by failure reason.",
    )
    parser.add_argument(
        "--event-filter",
        type=str,
        default=None,
        help="Event ID to inspect or replay.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of messages to inspect or replay (default: 50).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Explicitly execute replay and commit DLQ offset (default is dry-run preview).",
    )
    parser.add_argument(
        "--regenerate-event-id",
        action="store_true",
        help="Generate a new unique event_id (orig_id-replay-<timestamp>) instead of preserving original ID.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Kafka bootstrap servers (overrides KAFKA_BOOTSTRAP_SERVERS).",
    )
    parser.add_argument(
        "--dlq-topic",
        type=str,
        default=None,
        help="DLQ topic name (overrides KAFKA_DLQ_TOPIC).",
    )
    parser.add_argument(
        "--target-topic",
        type=str,
        default=None,
        help="Target topic to replay into (overrides KAFKA_MIGRATION_FAILED_TOPIC).",
    )
    return parser


def parse_headers(raw_headers: Optional[List[tuple]]) -> Dict[str, str]:
    if not raw_headers:
        return {}
    res = {}
    for key, val in raw_headers:
        try:
            res[key] = val.decode("utf-8") if isinstance(val, (bytes, bytearray)) else str(val)
        except Exception:
            res[key] = str(val)
    return res


def inspect_or_replay_dlq(
    settings: KafkaSettings,
    mode: str = "list",
    reason_filter: Optional[str] = None,
    event_filter: Optional[str] = None,
    limit: int = 50,
    execute: bool = False,
    regenerate_event_id: bool = False,
) -> int:
    if Consumer is None:
        LOG.error("confluent-kafka is not installed; cannot connect to Kafka cluster.")
        return 1

    consumer_group = f"mfa-dlq-tool-{int(time.time())}"
    consumer_conf = settings.to_kafka_config({
        "group.id": consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer = Consumer(consumer_conf)
    producer = None
    if mode == "replay" and execute:
        producer = Producer(settings.to_kafka_config())

    dlq_topic = settings.dlq_topic
    target_topic = settings.input_topic
    consumer.subscribe([dlq_topic])

    LOG.info("Reading DLQ topic '%s' (limit=%d)...", dlq_topic, limit)
    pattern = re.compile(reason_filter, re.IGNORECASE) if reason_filter else None

    inspected_count = 0
    matched_count = 0
    replayed_count = 0

    try:
        start_t = time.time()
        while inspected_count < limit and (time.time() - start_t) < 15.0:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                LOG.error("Consumer error: %s", msg.error())
                continue

            inspected_count += 1
            raw_val = msg.value()
            headers = parse_headers(msg.headers())
            reason = headers.get("dlq.reason", "Unknown reason")

            try:
                event_data = parse_event(raw_val)
                event_id = event_data.get("event_id", "<no-event-id>")
            except Exception:
                event_data = None
                event_id = "<unparseable>"

            if event_filter and event_filter != event_id:
                continue
            if pattern and not pattern.search(reason):
                continue

            matched_count += 1
            print(f"\n--- [DLQ Message #{matched_count}] Offset: {msg.offset()} Partition: {msg.partition()} ---")
            print(f"  Event ID: {event_id}")
            print(f"  Reason:   {reason}")
            if event_data:
                print(f"  Phase:    {event_data.get('phase')}")
                print(f"  Failure:  {event_data.get('failure_code')}")
                print(f"  Message:  {event_data.get('message')}")

            if mode == "replay":
                if not execute:
                    print(f"  [DRY RUN] Would republish to '{target_topic}' and commit DLQ offset {msg.offset() + 1}")
                else:
                    if not event_data:
                        print("  [SKIP] Cannot replay unparseable malformed event.")
                        continue

                    if regenerate_event_id:
                        event_data["event_id"] = f"{event_id}-replay-{int(time.time())}"
                        raw_to_send = json.dumps(event_data).encode("utf-8")
                        key_to_send = event_data["event_id"].encode("utf-8")
                    else:
                        raw_to_send = raw_val
                        key_to_send = msg.key()

                    replay_headers = [
                        ("replayed_from_dlq", b"true"),
                        ("original_dlq_offset", str(msg.offset()).encode("utf-8")),
                        ("replay_timestamp", str(time.time()).encode("utf-8")),
                    ]
                    producer.produce(  # type: ignore
                        topic=target_topic,
                        value=raw_to_send,
                        key=key_to_send,
                        headers=replay_headers,
                    )
                    producer.flush(timeout=5.0)  # type: ignore
                    consumer.commit(
                        offsets=[TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)],
                        asynchronous=False,
                    )
                    replayed_count += 1
                    print(f"  [REPLAYED] Republished to '{target_topic}' and committed DLQ offset.")

    finally:
        consumer.close()

    print("\n================ Summary ================")
    print(f"Messages Scanned:   {inspected_count}")
    print(f"Messages Matched:   {matched_count}")
    if mode == "replay":
        if execute:
            print(f"Messages Replayed:  {replayed_count}")
        else:
            print("Mode:               DRY-RUN (0 replayed, pass --execute to apply)")
    print("=========================================\n")
    return 0


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = create_parser()
    args = parser.parse_args()

    settings = KafkaSettings.from_env()
    if args.bootstrap_servers:
        settings = KafkaSettings(
            bootstrap_servers=args.bootstrap_servers,
            input_topic=args.target_topic or settings.input_topic,
            result_topic=settings.result_topic,
            dlq_topic=args.dlq_topic or settings.dlq_topic,
            consumer_group=settings.consumer_group,
            auto_offset_reset=settings.auto_offset_reset,
            poll_timeout_seconds=settings.poll_timeout_seconds,
            publish_timeout_seconds=settings.publish_timeout_seconds,
            security_protocol=settings.security_protocol,
            sasl_mechanism=settings.sasl_mechanism,
            sasl_username=settings.sasl_username,
            sasl_password=settings.sasl_password,
            ssl_ca_location=settings.ssl_ca_location,
            ssl_certificate_location=settings.ssl_certificate_location,
            ssl_key_location=settings.ssl_key_location,
        )

    sys.exit(
        inspect_or_replay_dlq(
            settings=settings,
            mode=args.mode,
            reason_filter=args.reason_filter,
            event_filter=args.event_filter,
            limit=args.limit,
            execute=args.execute,
            regenerate_event_id=args.regenerate_event_id,
        )
    )


if __name__ == "__main__":
    main()
