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
from engine.llm.env import load_dotenv
from engine.workflow.engine import MigrationFailureEngine
from persistence.repository import SRETrackerRepository


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = KafkaSettings.from_env()
    registry, fixture_tracker = build_local_registry()

    # Local E2E uses PostgreSQL when configured; fixture tracker remains available for no-DB development.
    tracker = fixture_tracker
    dsn = os.getenv("DATABASE_URL")
    if not dsn and os.getenv("POSTGRES_HOST"):
        dsn = f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:{os.getenv('POSTGRES_PASSWORD', 'postgres')}@{os.getenv('POSTGRES_HOST', '127.0.0.1')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'migration_agent')}"
    knowledge = None
    if dsn:
        tracker = SRETrackerRepository(dsn)
        tracker.migrate()
        embed_url = os.getenv("EMBEDDING_BASE_URL", os.getenv("LOCAL_EMBEDDING_URL"))
        embed_model = os.getenv("EMBEDDING_MODEL", os.getenv("LOCAL_EMBEDDING_MODEL", "nomic-ai/nomic-embed-text-v1.5-GGUF:Q4_K_M"))
        if embed_url:
            try:
                import requests
                from persistence.knowledge import PostgresVectorKnowledgeRepository
                def _embed_fn(text: str):
                    try:
                        r = requests.post(
                            embed_url.rstrip("/") + "/embeddings",
                            json={"model": embed_model, "input": f"search_query: {text}"},
                            timeout=10,
                        )
                        r.raise_for_status()
                        return r.json()["data"][0]["embedding"]
                    except Exception:
                        return None
                knowledge = PostgresVectorKnowledgeRepository(tracker, embed=_embed_fn)
                logging.info("Connected PostgreSQL pgvector knowledge repository via %s", embed_url)
            except Exception as e:
                logging.warning("Could not initialize pgvector knowledge repository: %s", e)

    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=knowledge)
    publisher = KafkaResultPublisher(settings)
    from engine.observability import start_metrics_server
    metrics_server = start_metrics_server(ready_check_fn=lambda: ingress.consumer is not None)

    try:
        ingress.run_forever()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutting down Kafka ingress...")
    finally:
        ingress.close()
        if metrics_server:
            metrics_server.stop()


if __name__ == "__main__":
    main()
