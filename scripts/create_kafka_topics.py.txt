#!/usr/bin/env python3
from __future__ import annotations

import os
from confluent_kafka.admin import AdminClient, NewTopic

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
topics = [
    os.getenv("KAFKA_MIGRATION_FAILED_TOPIC", "mfa.migration.failed"),
    os.getenv("KAFKA_AGENT_RESULT_TOPIC", "mfa.agent.result"),
]
admin = AdminClient({"bootstrap.servers": bootstrap})
metadata = admin.list_topics(timeout=5)
missing = [NewTopic(t, num_partitions=3, replication_factor=1) for t in topics if t not in metadata.topics]
if missing:
    futures = admin.create_topics(missing)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"created {topic}")
        except Exception as exc:
            print(f"failed {topic}: {exc}")
else:
    print("topics already exist")
