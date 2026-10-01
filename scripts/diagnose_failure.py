#!/usr/bin/env python3
"""Run real-time failure diagnosis on a JSON MigrationFailed event.

Usage:
  # From a JSON file:
  python scripts/diagnose_failure.py --file examples/sample_failure_event.json

  # Piped from stdin:
  cat examples/sample_failure_event.json | python scripts/diagnose_failure.py

  # Direct inline JSON string:
  python scripts/diagnose_failure.py --json '{"event_id": "evt-001", "event_type": "MigrationFailed", ...}'

  # Output machine-readable JSON:
  python scripts/diagnose_failure.py --file event.json --output-json result.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engine.workflow.engine import MigrationFailureEngine
from engine.integrations.local.registry import build_local_registry
from persistence.repository import SRETrackerRepository
from persistence.knowledge import PostgresVectorKnowledgeRepository

REQUIRED_FIELDS = ("event_id", "event_type")


def make_query_embedding(base_url: str, model: str, prefix: str = "search_query: "):
    def query_embedding(text: str):
        try:
            r = requests.post(
                base_url.rstrip("/") + "/embeddings",
                json={"model": model, "input": prefix + text},
                timeout=30,
            )
            r.raise_for_status()
            return r.json()["data"][0]["embedding"]
        except Exception:
            return None
    return query_embedding


def parse_input(args: argparse.Namespace) -> dict[str, Any]:
    from datetime import datetime, timezone
    import uuid

    if args.message:
        event = {
            "event_id": f"evt-{uuid.uuid4().hex[:8]}",
            "event_type": "MigrationFailed",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "message": args.message.strip(),
            "cluster_id": args.cluster_id or "ocv-prod-a",
            "vm_id": args.vm_id or "vm-observed",
            "migration_id": args.migration_id or f"mig-{uuid.uuid4().hex[:6]}",
            "environment": {
                "target": {"cluster_id": args.cluster_id or "ocv-prod-a", "ocp_version": "4.19.23", "ocv_version": "4.19.23", "mtv_version": "2.11.0"},
                "source": {"provider": "vmware", "vcenter_version": "8.0"},
            },
        }
        return {"event": event, "failure_case_id": event["event_id"]}

    raw_text = None
    if args.file:
        raw_text = Path(args.file).read_text(encoding="utf-8")
    elif args.json:
        raw_text = args.json
    elif not sys.stdin.isatty():
        raw_text = sys.stdin.read().strip()

    if not raw_text:
        print("ERROR: No input provided. Specify --message, --file, --json, or pipe text via stdin.", file=sys.stderr)
        sys.exit(1)

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        # Plain text error message passed directly: auto-wrap into MigrationFailed event
        event = {
            "event_id": f"evt-{uuid.uuid4().hex[:8]}",
            "event_type": "MigrationFailed",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "message": raw_text.strip(),
            "cluster_id": args.cluster_id or "ocv-prod-a",
            "vm_id": args.vm_id or "vm-observed",
            "migration_id": args.migration_id or f"mig-{uuid.uuid4().hex[:6]}",
            "environment": {
                "target": {"cluster_id": args.cluster_id or "ocv-prod-a", "ocp_version": "4.19.23", "ocv_version": "4.19.23", "mtv_version": "2.11.0"},
                "source": {"provider": "vmware", "vcenter_version": "8.0"},
            },
        }
        return {"event": event, "failure_case_id": event["event_id"]}

    event = data.get("event", data)
    missing = [f for f in REQUIRED_FIELDS if not event.get(f)]
    if missing:
        print(f"ERROR: Missing required event fields: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    if event.get("event_type") != "MigrationFailed":
        print(f"ERROR: event_type must be 'MigrationFailed', got: {event.get('event_type')}", file=sys.stderr)
        sys.exit(1)

    return {"event": event, "failure_case_id": event.get("failure_case_id") or event.get("event_id")}


def print_diagnosis_report(state: Any) -> None:
    event = state.event
    print("\n" + "=" * 80)
    print("MIGRATION FAILURE AGENT — DIAGNOSTIC REPORT (V1 READ-ONLY)")
    print("=" * 80)

    print(f"\n[INCIDENT IDENTITY]")
    print(f"  Failure Case ID:  {state.failure_case_id}")
    print(f"  Event ID:         {event.get('event_id')}")
    print(f"  Migration ID:     {event.get('migration_id', 'N/A')}")
    print(f"  Virtual Machine:  {event.get('vm_id', 'N/A')}")
    print(f"  Target Cluster:   {event.get('cluster_id', 'N/A')}")
    print(f"  Phase:            {event.get('phase', 'N/A')}")
    print(f"  Failure Code:     {event.get('failure_code', 'N/A')}")
    print(f"  Message:          {event.get('message', 'N/A')}")

    print(f"\n[CLASSIFICATION & EPISODIC MEMORY]")
    print(f"  Domain:           {state.classification}")
    print(f"  Skill Activated:  {state.skill_id or 'None'}")
    rec = state.recurrence or {}
    print(f"  Recurrence:       {rec.get('recurrence_status', 'UNKNOWN')} (Occurrences: {rec.get('occurrence_count', 1)})")
    if rec.get("validated_solution_refs"):
        print(f"  Known Solutions:  {', '.join(rec.get('validated_solution_refs'))}")

    print(f"\n[EVIDENCE COLLECTED ({len(state.evidence)} facts)]")
    for ev in state.evidence:
        status_symbol = "✓" if ev.status.value == "SUCCESS" else "✗"
        print(f"  [{status_symbol}] {ev.fact} (Source: {ev.source}, Status: {ev.status.value})")

    print(f"\n[KNOWLEDGE RETRIEVAL (RHOKP RAG)]")
    for doc in state.knowledge_context[:3]:
        compat = doc.get("compatibility", {}).get("recommendation_status", "UNKNOWN")
        score = doc.get("score", 0.0)
        title = doc.get("title") or doc.get("id")
        print(f"  • [{score:.4f}] {str(title)[:65]} (Compat: {compat})")

    print(f"\n[DIAGNOSIS]")
    diag = state.diagnosis or {}
    print(f"  Mechanism:        {diag.get('mechanism', 'UNKNOWN')}")
    print(f"  Status:           {diag.get('status', 'INSUFFICIENT_EVIDENCE')}")
    print(f"  Confidence:       {diag.get('confidence', 0.0):.2f}")
    if diag.get("supporting_evidence"):
        print(f"  Evidence Facts:   {', '.join(diag.get('supporting_evidence'))}")

    if state.llm_advisory and state.llm_advisory.get("status") == "ADVISORY":
        print(f"\n[LLM ADVISORY — BASELINE INVESTIGATION PROBES]")
        print(f"  Summary:          {state.llm_advisory.get('summary')}")
        if state.llm_advisory.get("uncertainty"):
            print(f"  Uncertainty:      {state.llm_advisory.get('uncertainty')}")
        suggestions = state.llm_advisory.get("suggested_investigations", [])
        if suggestions:
            print("  Suggested Next Read-Only Checks:")
            for s in suggestions:
                print(f"    • Action: {s.get('action')} — {s.get('purpose')}")
                if s.get("parameters"):
                    print(f"      Signal: {s.get('parameters')}")
    elif state.llm_advisory and state.llm_advisory.get("status") not in {"NOT_REQUESTED", None}:
        print(f"\n[LLM ADVISORY]")
        print(f"  Status:           {state.llm_advisory.get('status')} ({state.llm_advisory.get('reason', '')})")

    print(f"\n[SAFE NEXT STEP (READ-ONLY)]")
    print(f"  Recommended Step: {state.next_step}")
    print(f"  Agent Status:     {state.status}")
    if state.decision_readiness:
        for action, details in list(state.decision_readiness.items())[:3]:
            r_val = details.get("readiness", "UNKNOWN")
            print(f"  Action '{action}': {r_val}")
    print(f"  Safety Boundary:  Diagnostic V1 read-only. No platform mutation executed.")
    print("=" * 80 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Diagnose a MigrationFailed JSON event with Migration Failure Agent")
    parser.add_argument("--message", "-m", help="Raw error message string to diagnose directly")
    parser.add_argument("--file", "-f", help="Path to JSON file containing MigrationFailed event")
    parser.add_argument("--json", "-j", help="Raw JSON string containing MigrationFailed event")
    parser.add_argument("--vm-id", help="Optional VM identifier")
    parser.add_argument("--cluster-id", help="Optional target cluster identifier")
    parser.add_argument("--migration-id", help="Optional migration plan identifier")
    parser.add_argument("--output-json", "-o", help="Optional path to write full diagnostic state JSON")
    parser.add_argument("--dsn", default=os.getenv("DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:5432/migration_agent"))
    parser.add_argument("--embed-url", default=os.getenv("LOCAL_EMBEDDING_URL", "http://127.0.0.1:11434/v1"))
    parser.add_argument("--embed-model", default=os.getenv("LOCAL_EMBEDDING_MODEL", "nomic-ai/nomic-embed-text-v1.5-GGUF:Q4_K_M"))
    parser.add_argument("--llm-provider", default=os.getenv("LLM_PROVIDER", "none"), help="LLM provider: 'local', 'ollama', 'openrouter', or 'none'")
    parser.add_argument("--llm-url", default=os.getenv("LOCAL_LLM_BASE_URL", "http://127.0.0.1:8080/v1"), help="Base URL for local LLM")
    parser.add_argument("--llm-model", default=os.getenv("LOCAL_LLM_MODEL", "unsloth/Phi-4-mini-reasoning-GGUF:Q4_K_M"), help="Model name for local LLM")
    args = parser.parse_args()

    request = parse_input(args)

    # Setup Persistence & Knowledge
    try:
        repo = SRETrackerRepository(args.dsn)
        repo.migrate()
        embed_fn = make_query_embedding(args.embed_url, args.embed_model)
        knowledge = PostgresVectorKnowledgeRepository(repo, embed=embed_fn)
    except Exception as exc:
        print(f"WARN: PostgreSQL/pgvector not connected ({exc}). Using in-memory fallback.", file=sys.stderr)
        repo = None
        knowledge = None

    # Capability Registry
    registry, fixture_tracker = build_local_registry()
    tracker = repo if repo is not None else fixture_tracker

    # LLM Provider
    llm = None
    if args.llm_provider in ("local", "ollama"):
        from engine.llm.local import LocalOpenAICompatibleProvider
        llm = LocalOpenAICompatibleProvider(model=args.llm_model, base_url=args.llm_url)
    elif args.llm_provider == "openrouter":
        from engine.llm.openrouter import OpenRouterProvider
        llm = OpenRouterProvider()

    # Run Agent
    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=knowledge, memory_mode="both", llm_provider=llm)
    state = engine.run(request)

    # Display Report
    print_diagnosis_report(state)

    if args.output_json:
        result_dict = state.to_dict() if hasattr(state, "to_dict") else vars(state)
        Path(args.output_json).write_text(json.dumps(result_dict, indent=2, default=str), encoding="utf-8")
        print(f"Full JSON state written to: {args.output_json}")


if __name__ == "__main__":
    main()
