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
    LOG = logging.getLogger("migration-failure-agent.launcher")

    mfa_env = os.getenv("MFA_ENV", "production").lower()
    registry_mode = os.getenv("REGISTRY_MODE", "dmz" if mfa_env == "production" else "local").lower()
    LOG.info("Initializing MFA Agent in env=%s, registry_mode=%s", mfa_env, registry_mode)

    # 1. Capability Registry selection & production enforcement
    if registry_mode == "dmz":
        from engine.integrations.dmz.registry import build_dmz_registry
        registry = build_dmz_registry()
        fixture_tracker = None
        LOG.info("Configured DMZ capability registry (Splunk MCP + Prometheus MCP)")
    elif registry_mode == "local":
        if mfa_env == "production":
            raise RuntimeError("FATAL: Refusing to start in production (MFA_ENV=production) with fixture registry. Set REGISTRY_MODE=dmz.")
        registry, fixture_tracker = build_local_registry()
        LOG.warning("[DEV-ONLY] Configured local/fixture capability registry")
    else:
        raise ValueError(f"Unknown REGISTRY_MODE: {registry_mode}. Valid options: 'dmz', 'local'.")

    # 2. Persistence & SRE Tracker wiring (Strict fail-closed in production)
    dsn = os.getenv("DATABASE_URL")
    if not dsn and os.getenv("POSTGRES_HOST"):
        dsn = f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:{os.getenv('POSTGRES_PASSWORD', 'postgres')}@{os.getenv('POSTGRES_HOST', '127.0.0.1')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'migration_agent')}"

    tracker = None
    if dsn:
        try:
            tracker = SRETrackerRepository(dsn)
            tracker.migrate()
            LOG.info("Connected PostgreSQL SRE Tracker repository")
        except Exception as exc:
            if mfa_env == "production":
                LOG.critical("FATAL: PostgreSQL connection failed in production: %s", exc)
                raise RuntimeError(f"FATAL: PostgreSQL unavailable in production: {exc}") from exc
            LOG.warning("PostgreSQL connection failed (%s); falling back to fixture tracker for dev", exc)
            tracker = fixture_tracker
    else:
        if mfa_env == "production":
            raise RuntimeError("FATAL: DATABASE_URL is mandatory in production (MFA_ENV=production). Refusing fallback to in-memory fixture tracker.")
        LOG.warning("[DEV-ONLY] No DATABASE_URL configured; using in-memory fixture tracker")
        tracker = fixture_tracker

    # 3. Vector Knowledge Repository (pgvector)
    knowledge = None
    if tracker and hasattr(tracker, "connection"):
        embed_url = os.getenv("EMBEDDING_BASE_URL", os.getenv("LOCAL_EMBEDDING_URL", "http://127.0.0.1:11434/v1"))
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
                            timeout=8.0,
                        )
                        r.raise_for_status()
                        return r.json()["data"][0]["embedding"]
                    except Exception:
                        return None

                knowledge = PostgresVectorKnowledgeRepository(tracker, embed=_embed_fn)
                LOG.info("Connected PostgreSQL pgvector knowledge repository via %s", embed_url)
            except Exception as e:
                LOG.warning("Could not initialize pgvector knowledge repository: %s", e)

    # 4. Engine & Production Guard Validation
    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=knowledge)
    if mfa_env == "production":
        # Ensure LangGraph compiles cleanly in production
        try:
            import langgraph  # noqa: F401
            graph = engine._get_graph()
            if graph is None:
                raise RuntimeError("LangGraph compiled graph returned None")
        except Exception as exc:
            raise RuntimeError(f"FATAL: LangGraph workflow compilation failed in production: {exc}") from exc

    # 5. Kafka Transport & Ingress
    settings = KafkaSettings.from_env()
    publisher = KafkaResultPublisher(settings)
    ingress = KafkaIngress(settings, run_agent=engine.run, result_publisher=publisher, tracker=tracker)

    from engine.observability import start_metrics_server
    metrics_server = start_metrics_server(ready_check_fn=lambda: getattr(ingress, "consumer", None) is not None)

    try:
        LOG.info("Migration Failure Agent is running and listening on %s...", settings.input_topic)
        ingress.run_forever()
    except (KeyboardInterrupt, SystemExit):
        LOG.info("Shutting down Kafka ingress...")
    finally:
        ingress.close()
        if metrics_server:
            metrics_server.stop()


if __name__ == "__main__":
    main()
