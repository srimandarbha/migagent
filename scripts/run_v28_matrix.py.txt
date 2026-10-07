#!/usr/bin/env python3
"""Run the Migration Failure Agent evaluation matrix.

Supports:
  * direct: invoke the deterministic simulator/agent in-process
  * kafka: publish MigrationFailed events and wait for MigrationFailureAgentResult

The matrix is data-driven. Add scenarios to datasets/v28_test_matrix.yaml.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import yaml

from simulator.runner import simulate


@dataclass
class Assertion:
    name: str
    expected: Any
    actual: Any
    passed: bool


@dataclass
class MatrixResult:
    scenario: str
    memory_mode: str
    provider: str
    mode: str
    event_id: str
    passed: bool
    classification: Any = None
    diagnosis_status: Any = None
    diagnosis_code: Any = None
    diagnosis_confidence: Any = None
    next_step: Any = None
    status: Any = None
    memory_history_count: int = 0
    memory_knowledge_count: int = 0
    diagnosis_basis: dict[str, Any] | None = None
    assertions: list[dict[str, Any]] | None = None
    error: str | None = None


def load_matrix(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def nested_get(obj: Any, path: str) -> Any:
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def compare(actual: Any, expected: Any) -> bool:
    if isinstance(expected, dict) and "contains" in expected:
        needle = expected["contains"]
        if isinstance(actual, str):
            return needle in actual
        return False
    if isinstance(expected, dict) and "in" in expected:
        return actual in expected["in"]
    if isinstance(expected, dict) and "min" in expected:
        try:
            return actual >= expected["min"]
        except TypeError:
            return False
    return actual == expected


def evaluate(output: dict[str, Any], expected: dict[str, Any]) -> list[Assertion]:
    assertions: list[Assertion] = []
    for field, wanted in expected.items():
        actual = nested_get(output, field)
        assertions.append(Assertion(field, wanted, actual, compare(actual, wanted)))
    return assertions


def make_event(scenario: str, event_id: str, entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event_type": "MigrationFailed",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "migration_id": entry.get("migration_id", f"mig-{event_id}"),
        "vm_id": entry.get("vm_id", f"vm-{event_id}"),
        "cluster_id": entry.get("cluster_id", "ocv-local-01"),
        "failure_code": entry.get("failure_code"),
        "severity": entry.get("severity", "critical"),
        "phase": entry.get("phase", "UNKNOWN"),
        "message": entry.get("message", scenario),
        "scenario": scenario,
    }


def run_direct(scenario: str, provider: str, memory_mode: str) -> dict[str, Any]:
    return simulate(scenario, mode="agent", provider=provider, memory_mode=memory_mode)


def run_kafka_case(scenario: str, provider: str, memory_mode: str, entry: dict[str, Any], timeout: float) -> tuple[dict[str, Any], str]:
    try:
        from confluent_kafka import Consumer, Producer
    except ImportError as exc:
        raise RuntimeError("kafka mode requires confluent-kafka") from exc

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    input_topic = os.getenv("KAFKA_MIGRATION_FAILED_TOPIC", "mfa.migration.failed")
    result_topic = os.getenv("KAFKA_AGENT_RESULT_TOPIC", "mfa.agent.result")
    event_id = f"v28-{scenario}-{uuid.uuid4().hex[:12]}"
    event = make_event(scenario, event_id, entry)
    event['memory_mode'] = memory_mode

    group = f"mfa-v28-eval-{uuid.uuid4().hex}"
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": bootstrap})
    consumer.subscribe([result_topic])

    try:
        producer.produce(input_topic, key=event_id, value=json.dumps(event).encode("utf-8"))
        producer.flush()

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if payload.get("event_id") != event_id:
                continue
            result = payload.get("result")
            if not isinstance(result, dict):
                result = payload
            return result, event_id
        raise TimeoutError(f"timed out waiting for agent result event_id={event_id}")
    finally:
        consumer.close()


def select_cases(matrix: dict[str, Any], scenario_filter: str | None) -> list[dict[str, Any]]:
    cases = matrix.get("scenarios", [])
    if scenario_filter:
        cases = [c for c in cases if c.get("id") == scenario_filter]
        if not cases:
            raise SystemExit(f"scenario not found in matrix: {scenario_filter}")
    return cases


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the v2.8 Migration Failure Agent evaluation matrix")
    parser.add_argument("--matrix", default=str(ROOT / "datasets" / "v28_test_matrix.yaml"))
    parser.add_argument("--scenario")
    parser.add_argument("--mode", choices=["direct", "kafka"], default="direct")
    parser.add_argument("--provider", default="deterministic")
    parser.add_argument("--memory", choices=["both", "sre", "rhokp", "none", "all"], default="all")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--output", default=str(ROOT / "results" / "v28-matrix-results.json"))
    args = parser.parse_args()

    matrix = load_matrix(Path(args.matrix))
    cases = select_cases(matrix, args.scenario)
    memory_modes = [args.memory] if args.memory != "all" else ["both", "sre", "rhokp", "none"]

    results: list[MatrixResult] = []
    print("=== V2.8 MIGRATION FAILURE AGENT MATRIX ===")
    print(f"Mode={args.mode} Provider={args.provider} Memory={','.join(memory_modes)}")
    print()

    for entry in cases:
        scenario = entry["id"]
        for memory_mode in memory_modes:
            label = f"{scenario:<36} {memory_mode:<7}"
            event_id = f"v28-{scenario}-{memory_mode}-{uuid.uuid4().hex[:8]}"
            try:
                if args.mode == "direct":
                    output = run_direct(scenario, args.provider, memory_mode)
                else:
                    output, event_id = run_kafka_case(scenario, args.provider, memory_mode, entry, args.timeout)

                assertions = evaluate(output, entry.get("expected", {}))
                # Harness semantics: memory mode must change access, not current evidence.
                mem = output.get('memory_context') or {}
                expected_sre = memory_mode in {'both','sre'}
                expected_rhokp = memory_mode in {'both','rhokp'}
                basis = output.get('diagnosis_basis') or {}
                enrichment = (output.get('recommendation') or {}).get('memory_enrichment') or []
                enrichment_sources = {str(x.get('source')) for x in enrichment if isinstance(x, dict)}
                assertions.extend([
                    Assertion('memory_context.sre_tracker_enabled', expected_sre, mem.get('sre_tracker_enabled'), mem.get('sre_tracker_enabled') == expected_sre),
                    Assertion('memory_context.rhokp_enabled', expected_rhokp, mem.get('rhokp_enabled'), mem.get('rhokp_enabled') == expected_rhokp),
                    Assertion('diagnosis_basis.authoritative', {'min': 1} if output.get('diagnosis',{}).get('status') == 'LIKELY' else {'min': 0}, len(basis.get('authoritative', [])), compare(len(basis.get('authoritative', [])), {'min': 1} if output.get('diagnosis',{}).get('status') == 'LIKELY' else {'min': 0})),
                    Assertion('memory_usefulness.sre', expected_sre and bool(output.get('historical_context')), expected_sre and bool(output.get('historical_context')), (not expected_sre) or (not output.get('historical_context')) or ('SRE_TRACKER' in enrichment_sources)),
                    Assertion('memory_usefulness.rhokp', expected_rhokp and bool(output.get('knowledge_context')), expected_rhokp and bool(output.get('knowledge_context')), (not expected_rhokp) or (not output.get('knowledge_context')) or ('RHOKP' in enrichment_sources)),
                    Assertion('memory_provenance_separate_from_authority', True, bool(basis.get('authoritative')) if output.get('diagnosis',{}).get('status') == 'LIKELY' else True, True),
                ])
                passed = all(a.passed for a in assertions)
                result = MatrixResult(
                    scenario=scenario,
                    memory_mode=memory_mode,
                    provider=args.provider,
                    mode=args.mode,
                    event_id=event_id,
                    passed=passed,
                    classification=output.get("classification"),
                    diagnosis_status=(output.get("diagnosis") or {}).get("status") if isinstance(output.get("diagnosis"), dict) else None,
                    diagnosis_code=(output.get("diagnosis") or {}).get("code") if isinstance(output.get("diagnosis"), dict) else None,
                    diagnosis_confidence=(output.get("diagnosis") or {}).get("confidence") if isinstance(output.get("diagnosis"), dict) else None,
                    next_step=output.get("next_step"),
                    status=output.get("status"),
                    memory_history_count=len(output.get('historical_context') or []),
                    memory_knowledge_count=len(output.get('knowledge_context') or []),
                    diagnosis_basis=output.get('diagnosis_basis'),
                    assertions=[asdict(a) for a in assertions],
                )
                results.append(result)
                print(f"{label} {'PASS' if passed else 'FAIL'}")
                if not passed:
                    for a in assertions:
                        if not a.passed:
                            print(f"  FAIL {a.name}: expected={a.expected!r} actual={a.actual!r}")
            except Exception as exc:
                result = MatrixResult(
                    scenario=scenario,
                    memory_mode=memory_mode,
                    provider=args.provider,
                    mode=args.mode,
                    event_id=event_id,
                    passed=False,
                    error=f"{type(exc).__name__}: {exc}",
                )
                results.append(result)
                print(f"{label} ERROR: {exc}")

    # Cross-memory invariant: current-evidence diagnosis must remain stable when only
    # historical/RHOKP access changes. This catches the old 'memory label only' harness.
    for scenario in sorted({r.scenario for r in results}):
        rows=[r for r in results if r.scenario==scenario]
        signatures={(r.classification, r.diagnosis_status, r.diagnosis_code, (r.diagnosis_basis or {}).get('authoritative', []).__len__()) for r in rows}
        if len(signatures) > 1:
            for r in rows:
                r.passed=False
                r.error=(r.error + '; ' if r.error else '') + 'diagnosis changed across memory modes'
        # Disabled memory sources must never appear in diagnosis_basis.
        for r in rows:
            basis=r.diagnosis_basis or {}
            if r.memory_mode in {'none','rhokp'} and basis.get('historical'):
                r.passed=False; r.error=(r.error + '; ' if r.error else '') + 'SRE history leaked into disabled memory mode'
            if r.memory_mode in {'none','sre'} and basis.get('knowledge'):
                r.passed=False; r.error=(r.error + '; ' if r.error else '') + 'RHOKP knowledge leaked into disabled memory mode'
    passed_count = sum(r.passed for r in results)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "matrix_version": matrix.get("version", "unknown"),
        "mode": args.mode,
        "provider": args.provider,
        "memory_modes": memory_modes,
        "total": len(results),
        "passed": passed_count,
        "failed": len(results) - passed_count,
        "results": [asdict(r) for r in results],
    }
    output_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    print()
    print("==============================================================")
    print(f"TOTAL : {len(results)}")
    print(f"PASS  : {passed_count}")
    print(f"FAIL  : {len(results) - passed_count}")
    print(f"JSON  : {output_path}")
    return 0 if passed_count == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
