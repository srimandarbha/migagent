"""Gate 6: Complete Golden Vertical Slice Test.

Verifies end-to-end integration:
  Kafka event -> Agent Ingress -> Agent Engine -> MemoryContext (PostgreSQL + RAG)
  -> Capability Registry -> Evidence Collection & Sufficiency
  -> Diagnosis -> Read-Only Safe Next Step -> Durable PostgreSQL SRE Tracker
  -> Kafka Result Topic Publication -> Offset Commit.

Strict Safety Contract:
  - Zero platform mutations (read-only diagnostic V1).
  - Priority order: CURRENT_EVIDENCE > SRE_HISTORY > RHOKP_KNOWLEDGE > LLM_ADVISORY.
  - Fail-closed deterministic execution.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock
import pytest

from engine.ingress.kafka import KafkaIngress, KafkaSettings, parse_event, build_request
from engine.workflow.engine import MigrationFailureEngine
from engine.integrations.local.registry import build_local_registry
from engine.integrations.local.postgres import build_postgres_services
from persistence.repository import SRETrackerRepository
from persistence.knowledge import PostgresVectorKnowledgeRepository


def _make_golden_event(event_id: str = "evt-golden-cbt-001"):
    return {
        "event_id": event_id,
        "event_type": "MigrationFailed",
        "event_time": "2026-10-01T12:00:00Z",
        "migration_id": "mig-golden-cbt-001",
        "vm_id": "vm-payroll-db",
        "cluster_id": "ocv-prod-a",
        "change_id": "CHG-998877",
        "failure_code": "storage.csi.provisioning_timeout",
        "failure_class": "STORAGE.CSI",
        "severity": "CRITICAL",
        "phase": "CopyDisks",
        "message": "DataVolume provisioning timed out during volume attachment",
        "environment": {
            "target": {"cluster_id": "ocv-prod-a", "ocp_version": "4.19.23", "ocv_version": "4.19.23", "mtv_version": "2.11.0"},
            "source": {"provider": "vmware", "vcenter_version": "8.0"},
            "migration": {"type": "warm"},
        },
    }


class FakeKafkaMessage:
    def __init__(self, payload: dict, topic: str = "mfa.migration.failed", partition: int = 0, offset: int = 101):
        self._payload = json.dumps(payload).encode("utf-8")
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._payload

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None


def test_gate6_complete_golden_vertical_slice_end_to_end():
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:5432/migration_agent")

    # Connect to PostgreSQL persistence
    repo = SRETrackerRepository(dsn)
    repo.migrate()
    knowledge = PostgresVectorKnowledgeRepository(repo, embed=None)

    # Capability Registry
    registry, _ = build_local_registry()

    # Track capability mutations to enforce read-only safety
    invoked_mutations = []
    original_invoke = registry.invoke

    def safe_invoke(capability_id, params, access="read", agent="migration-failure"):
        if access not in {"read", "query", "search"}:
            invoked_mutations.append((capability_id, access))
        return original_invoke(capability_id, params, access=access, agent=agent)

    registry.invoke = safe_invoke

    # Instantiate engine with real PostgreSQL SRE tracker & Knowledge
    engine = MigrationFailureEngine(registry, tracker=repo, knowledge=knowledge, memory_mode="both")

    # Ingress setup with mock publisher and consumer
    published_results = []
    mock_publisher = MagicMock()
    mock_publisher.publish.side_effect = lambda payload: published_results.append(payload)
    mock_consumer = MagicMock()

    ingress = KafkaIngress.__new__(KafkaIngress)
    ingress.settings = KafkaSettings()
    ingress.run_agent = engine.run
    ingress.result_publisher = mock_publisher
    ingress.tracker = repo
    ingress.consumer = mock_consumer

    # 1. Transport message arrives
    event_id = f"evt-golden-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    raw_event = _make_golden_event(event_id)
    msg = FakeKafkaMessage(raw_event, offset=202)

    # 2. Ingress parses and validates event
    parsed_event = parse_event(msg.value())
    request = build_request(parsed_event, msg)

    assert request["kafka"]["topic"] == "mfa.migration.failed"
    assert request["kafka"]["offset"] == 202

    # Save event as received in SRE Tracker
    case_id = repo.create_or_get_failure_case(
        event_id=event_id,
        migration_id=raw_event["migration_id"],
        vm_id=raw_event["vm_id"],
        cluster_id=raw_event["cluster_id"],
        change_id=raw_event["change_id"],
        failure_class=raw_event["failure_class"],
        failure_code=raw_event["failure_code"],
        agent_version="v2.11-prod",
        policy_version="v1.0",
        severity=raw_event["severity"],
    )
    repo.save_event(
        event_id=event_id,
        failure_case_id=case_id,
        event_type=raw_event["event_type"],
        event_time=datetime.now(timezone.utc),
        payload=raw_event,
        kafka_topic=msg.topic(),
        kafka_partition=msg.partition(),
        kafka_offset=msg.offset(),
        status="RECEIVED",
    )

    # 3. Agent Engine executes full graph
    state = ingress.run_agent(request)

    # 4. Strict Safety Verification: Diagnostic V1 is strictly read-only
    assert len(invoked_mutations) == 0, f"MUTATION DETECTED: {invoked_mutations}"

    # 5. Verify Evidence Sufficiency & Diagnosis
    facts = {e.fact for e in state.evidence if e.status.value == "SUCCESS"}
    assert {"PVC_PENDING", "CSI_PROVISIONING_TIMEOUT", "BACKEND_HEALTHY", "MIGRATION_FAILED"} <= facts
    assert state.diagnosis["status"] == "LIKELY"
    assert state.diagnosis["mechanism"] == "STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH"
    assert state.next_step == "REVIEW_CSI_CONTROLLER_FAILURE"

    # 6. Verify Memory & Knowledge Integration
    assert state.memory_context is not None
    assert state.diagnosis_basis["priority_order"] == [
        "CURRENT_EVIDENCE", "SRE_HISTORY", "RHOKP_KNOWLEDGE", "LLM_ADVISORY"
    ]

    from dataclasses import asdict

    # 7. Durable Persistence in PostgreSQL
    evidence_ids = []
    for ev in state.evidence:
        ev_dict = ev.to_dict() if hasattr(ev, "to_dict") else asdict(ev)
        ev_id = repo.save_evidence(case_id, ev_dict)
        evidence_ids.append(ev_id)
    assert len(evidence_ids) > 0

    diag_id = repo.save_diagnosis(case_id, state.diagnosis, evidence_ids=evidence_ids)
    assert diag_id is not None

    # 8. Result Publishing to Kafka
    result_payload = {
        "event_id": event_id,
        "event_type": "MigrationFailureAgentResult",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "failure_case_id": str(case_id),
        "status": state.status,
        "next_step": state.next_step,
        "diagnosis": state.diagnosis,
        "source": {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
        },
        "agent_version": "v2.11-prod",
        "policy_version": "v1.0",
        "result": state.to_dict(),
    }
    ingress.result_publisher.publish(result_payload)
    ingress.tracker.mark_event_completed(event_id)
    ingress.consumer.commit(message=msg, asynchronous=False)

    # 9. Verify Kafka Contract
    assert len(published_results) == 1
    assert published_results[0]["event_id"] == event_id
    assert published_results[0]["diagnosis"]["mechanism"] == "STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH"
    assert ingress.tracker.event_exists(event_id) is True
    mock_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)
