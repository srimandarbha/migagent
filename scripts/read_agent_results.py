#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    "group.id": os.getenv("KAFKA_RESULT_CONSUMER_GROUP", "mfa-result-reader"),
    "auto.offset.reset": "earliest",
})
topic = os.getenv("KAFKA_AGENT_RESULT_TOPIC", "mfa.agent.result")
consumer.subscribe([topic])
print(f"reading {topic}; Ctrl-C to stop")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue
        print(json.dumps(json.loads(msg.value()), indent=2, default=str))
finally:
    consumer.close()
