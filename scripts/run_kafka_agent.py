#!/usr/bin/env python3
"""Run the Migration Failure Agent against the configured Kafka topic."""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engine.ingress.kafka import KafkaIngress, KafkaResultPublisher, KafkaSettings
from engine.integrations.local.registry import build_local_registry
from engine.workflow.engine import MigrationFailureEngine
from persistence.repository import SRETrackerRepository


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = KafkaSettings.from_env()
    registry, fixture_tracker = build_local_registry()

    # Local E2E uses PostgreSQL when configured; fixture tracker remains available for no-DB development.
    tracker = fixture_tracker
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        tracker = SRETrackerRepository(dsn)
        tracker.migrate()

    engine = MigrationFailureEngine(registry, tracker=tracker)
    publisher = KafkaResultPublisher(settings)
    ingress = KafkaIngress(settings, run_agent=engine.run, result_publisher=publisher, tracker=tracker)
    ingress.run_forever()


if __name__ == "__main__":
    main()
