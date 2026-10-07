#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from confluent_kafka import Producer

REQUIRED = (
    "event_id", "event_type", "event_time", "migration_id",
    "vm_id", "cluster_id", "failure_code",
)

def read_payload() -> dict:
    parser = argparse.ArgumentParser(description="Publish one MigrationFailed event to Kafka")
    parser.add_argument("payload", nargs="?", help="JSON object. If omitted, read JSON from stdin when available.")
    parser.add_argument("--fixture-scenario", help="Local-test-only scenario label added to the event")
    args = parser.parse_args()

    raw = args.payload
    if raw is None:
        if sys.stdin.isatty():
            raise SystemExit("Provide a JSON payload argument or pipe a JSON object on stdin")
        raw = sys.stdin.read()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid JSON payload: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("payload must be a JSON object")
    if args.fixture_scenario:
        payload["scenario"] = args.fixture_scenario
    missing = [k for k in REQUIRED if not payload.get(k)]
    if missing:
        raise SystemExit("missing required fields: " + ", ".join(missing))
    if payload["event_type"] != "MigrationFailed":
        raise SystemExit("event_type must be MigrationFailed")
    return payload

def main() -> int:
    payload = read_payload()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    topic = os.getenv("KAFKA_MIGRATION_FAILED_TOPIC", "mfa.migration.failed")
    producer = Producer({"bootstrap.servers": bootstrap})
    producer.produce(topic, key=payload["event_id"], value=json.dumps(payload, separators=(",", ":")))
    remaining = producer.flush(10)
    if remaining:
        raise SystemExit(f"Kafka publish timed out with {remaining} message(s) still queued")
    print(f"published event_id={payload['event_id']} topic={topic}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
