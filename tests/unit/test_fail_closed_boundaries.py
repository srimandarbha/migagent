"""Tests for fail-closed boundaries, access enforcement, and contract integrity (C5, C6, C7, H1, H2, H3)."""
import json
from unittest.mock import MagicMock
from uuid import UUID, uuid4
import pytest

from engine.contracts.models import _parse_evidence_status, EvidenceStatus, AgentState
from engine.integrations.contracts import GLOBAL_CONTRACT_REGISTRY
from engine.integrations.registry import InMemoryCapabilityRegistry, CapabilityNotFound, RegistryError
from engine.llm.advisory import parse_advisory
from engine.memory.action_ontology import ActionPlan
from engine.memory.dynamic_knowledge_store import DynamicKnowledgeStore, FailureSignature, KnownSolution
from engine.workflow.engine import MigrationFailureEngine
from persistence.repository import SRETrackerRepository, _repo_record_resolution


def test_c5_site1_evidence_status_parsing_fails_closed():
    """C5 Site 1: Empty or None evidence status parses to UNKNOWN, not SUCCESS."""
    assert _parse_evidence_status(None) == EvidenceStatus.UNKNOWN
    assert _parse_evidence_status("") == EvidenceStatus.UNKNOWN
    assert _parse_evidence_status("UNKNOWN") == EvidenceStatus.UNKNOWN
    assert _parse_evidence_status("SUCCESS") == EvidenceStatus.SUCCESS
    assert _parse_evidence_status("VERIFIED") == EvidenceStatus.SUCCESS
    assert _parse_evidence_status("NONEXISTENT_TAXONOMY") == EvidenceStatus.UNKNOWN


def test_c5_site2_registry_supports_contract_fails_closed():
    """C5 Site 2: Registry supports_contract returns False for unknown capabilities or invalid signals."""
    reg = InMemoryCapabilityRegistry({
        "observability.search": lambda p: {"evidence": []},
    })
    # Valid domain & signal against GLOBAL_CONTRACT_REGISTRY
    assert reg.supports_contract("observability.search", "ocv", "pvc_state") is True
    # Invalid signal for observability.search fails closed
    assert reg.supports_contract("observability.search", "ocv", "nonexistent_signal") is False
    # Invalid domain for observability.search fails closed
    assert reg.supports_contract("observability.search", "bogus_domain", "pvc_state") is False
    # Completely unregistered capability fails closed
    assert reg.supports_contract("arbitrary.executor", "ocv", "pvc_state") is False


def test_c5_site3_contract_registry_validate_fails_closed_on_unknown():
    """C5 Site 3: GLOBAL_CONTRACT_REGISTRY.validate returns (False, ...) for unmanaged capabilities."""
    is_valid, err = GLOBAL_CONTRACT_REGISTRY.validate("unregistered.capability.xyz", {})
    assert is_valid is False
    assert "not under contract management" in err


def test_c5_site4_parse_advisory_rejects_unregistered_capabilities():
    """C5 Site 4: Advisory parsing marks unregistered capabilities as validated: False."""
    registry = InMemoryCapabilityRegistry({
        "observability.search": lambda p: {"evidence": []},
    })
    payload = {
        "summary": "Exploratory test",
        "suggested_investigations": [
            {
                "action": "QUERY_STORAGE",
                "purpose": "Check storage array",
                "capability": "storage.direct_array_api",
                "parameters": {"domain": "storage", "signal": "san_stats"},
            }
        ],
        "uncertainty": "None",
    }
    result = parse_advisory(json.dumps(payload), registry=registry)
    assert result["status"] == "ADVISORY"
    inv = result["suggested_investigations"][0]
    assert inv["validated"] is False
    assert "not registered" in inv["validation_error"].lower()


def test_c7_registry_invoke_denies_mutation_access():
    """C7: Calling registry.invoke with access != 'read' raises RegistryError."""
    reg = InMemoryCapabilityRegistry({
        "observability.search": lambda p: {"evidence": []},
    })
    with pytest.raises(RegistryError, match="not permitted"):
        reg.invoke("observability.search", {}, access="write")

    with pytest.raises(RegistryError, match="not permitted"):
        reg.invoke("observability.search", {}, access="mutate")

    # read access succeeds
    res = reg.invoke("observability.search", {}, access="read")
    assert res == {"evidence": []}


def test_c6_record_resolution_defaults_to_unverified():
    """C6: Omitting verification_status defaults to UNVERIFIED, preventing accidental knowledge promotion."""
    from engine.integrations.local.tracker import FixtureSRETrackerAdapter

    tracker = FixtureSRETrackerAdapter()
    case_id = tracker.create_or_get_failure_case(event_id="evt-c6", migration_id="mig-c6")
    tracker.record_resolution(
        failure_case_id=case_id,
        resolution_code="REMEDY-01",
        description="Manual intervention",
        outcome_status="RESOLVED",
    )
    assert len(tracker.cases[0]["resolutions"]) == 1
    res = tracker.cases[0]["resolutions"][0]
    assert res["verification_status"] == "UNVERIFIED"
    assert tracker.cases[0]["status"] == "RETAINED"  # NOT 'VERIFIED'


def test_h1_save_evidence_deterministic_per_case_isolation():
    """H1: save_evidence namespaces evidence IDs per case, preventing cross-case collision."""
    repo = SRETrackerRepository.__new__(SRETrackerRepository)
    repo._connection_pool = MagicMock()

    executed_inserts = []
    mock_conn = MagicMock()

    def capture_execute(sql, params=None):
        if "INSERT INTO sre.evidence" in sql:
            executed_inserts.append(params)
        return MagicMock()

    mock_conn.execute.side_effect = capture_execute
    repo.connection = MagicMock()
    repo.connection.return_value.__enter__.return_value = mock_conn

    case_1 = UUID("11111111-1111-1111-1111-111111111111")
    case_2 = UUID("22222222-2222-2222-2222-222222222222")

    raw_ev = {"id": "ev-common-001", "fact": "MIGRATION_FAILED"}

    id_1 = repo.save_evidence(case_1, raw_ev)
    id_2 = repo.save_evidence(case_2, raw_ev)

    # Cross-case evidence IDs must be distinct despite identical raw evidence id
    assert id_1 != id_2
    assert len(executed_inserts) == 2
    assert executed_inserts[0][0] == id_1
    assert executed_inserts[1][0] == id_2

    # Reprocessing the SAME case must deterministically yield the SAME evidence ID
    id_1_repeat = repo.save_evidence(case_1, raw_ev)
    assert id_1_repeat == id_1


def test_h2_diagnose_possible_evidence_does_not_inflate_to_likely():
    """H2: When dynamic signature evidence is only POSSIBLE, diagnosis is POSSIBLE with discounted confidence."""
    registry = InMemoryCapabilityRegistry({})
    store = DynamicKnowledgeStore(auto_seed=False)

    sig = FailureSignature(
        signature_id="SIG-TEST-PARTIAL",
        canonical_pattern="partial_match",
        domain="storage",
        description="Partial match pattern",
        mechanism="STORAGE.PARTIAL_DEGRADATION",
        required_evidence=["FACT_A", "FACT_B"],
        confidence_base=0.90,
    )
    store.register_signature(sig)

    engine = MigrationFailureEngine(registry=registry, knowledge_store=store)

    state = AgentState(
        failure_case_id="case-partial-01",
        event={"event_id": "evt-01", "message": "error"},
        evidence_round=1,
    )
    state.evidence_evaluation = {"diagnosis_status": "SUFFICIENT"}
    state.failure_signature = "SIG-TEST-PARTIAL"

    # Only 1 out of 2 required facts is present -> evidence is POSSIBLE
    from engine.contracts import Evidence
    state.evidence = [
        Evidence(id="e1", source="splunk", fact="FACT_A", status=EvidenceStatus.SUCCESS),
    ]

    diag = engine._diagnose(state)
    assert diag["status"] == "PATTERN_MATCH_POSSIBLE"
    assert diag["confidence"] == round(0.90 * 0.6, 3)
    assert "partially consistent" in diag["statement"]


def test_h3_classify_dynamic_override_gated_on_high_confidence():
    """H3: Low-confidence dynamic matches (< 0.6) cannot override deterministic classification."""
    registry = InMemoryCapabilityRegistry({})
    store = DynamicKnowledgeStore(auto_seed=False)

    # Signature with low confidence
    sig_low = FailureSignature(
        signature_id="SIG-WEAK-01",
        canonical_pattern="fuzzy_keyword",
        domain="storage",
        description="Weak keyword match",
        mechanism="STORAGE.WEAK_HIT",
        confidence_base=0.40,
    )
    store.register_signature(sig_low)

    engine = MigrationFailureEngine(registry=registry, knowledge_store=store)

    event = {
        "event_id": "evt-weak-01",
        "event_type": "MigrationFailed",
        "message": "fuzzy_keyword hit with unclassified failure",
    }
    state = AgentState(failure_case_id="case-weak-01", event=event)
    engine._classify(state)

    # Classification must remain UNKNOWN because dynamic match confidence was 0.40 (< 0.6)
    assert state.classification == "UNKNOWN"
    assert state.classification_confidence == 0.1
