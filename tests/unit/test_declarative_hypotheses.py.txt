import pytest
from engine.contracts import AgentState, Evidence, EvidenceStatus, Hypothesis
from engine.workflow.engine import MigrationFailureEngine
from engine.integrations.registry import InMemoryCapabilityRegistry


def test_declarative_hypotheses_evaluation_supported():
    engine = MigrationFailureEngine(InMemoryCapabilityRegistry({}))
    state = AgentState(failure_case_id="case-001", event={"event_id": "evt-001"})
    state.classification = "VMWARE.CBT"
    state.evidence = [
        Evidence(id="ev-1", source="observability.search", fact="CBT_FAILED", status=EvidenceStatus.SUCCESS),
    ]

    hypotheses = engine._hypotheses(state)
    assert len(hypotheses) == 1
    h = hypotheses[0]
    assert h.id == "H-CBT"
    assert h.code == "VMWARE.CBT_STATE"
    assert h.status == "SUPPORTED"
    assert h.supporting == ["ev-1"]
    assert h.contradicting == []


def test_declarative_hypotheses_evaluation_contradicted():
    engine = MigrationFailureEngine(InMemoryCapabilityRegistry({}))
    state = AgentState(failure_case_id="case-002", event={"event_id": "evt-002"})
    state.classification = "STORAGE.CSI.PROVISIONING_TIMEOUT"
    state.evidence = [
        Evidence(id="ev-1", source="observability.search", fact="BACKEND_HEALTHY", status=EvidenceStatus.SUCCESS),
        Evidence(id="ev-2", source="observability.search", fact="PVC_PENDING", status=EvidenceStatus.SUCCESS),
        Evidence(id="ev-3", source="observability.search", fact="CSI_PROVISIONING_TIMEOUT", status=EvidenceStatus.SUCCESS),
    ]

    hypotheses = engine._hypotheses(state)
    by_id = {h.id: h for h in hypotheses}

    # H-STORAGE-BACKEND is contradicted by BACKEND_HEALTHY
    h_storage = by_id["H-STORAGE-BACKEND"]
    assert h_storage.status == "CONTRADICTED"
    assert "ev-1" in h_storage.contradicting

    # H-CSI-PATH-FALLBACK is supported
    h_fallback = by_id["H-CSI-PATH-FALLBACK"]
    assert h_fallback.status == "SUPPORTED"
    assert set(h_fallback.supporting) == {"ev-1", "ev-2", "ev-3"}

    # Diagnosis basis should exclude STORAGE.BACKEND_DEGRADED
    state.hypotheses = hypotheses
    state.diagnosis = {"mechanism": "STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH", "status": "LIKELY"}
    basis = engine._diagnosis_basis(state)

    assert any(x["mechanism"] == "STORAGE.BACKEND_DEGRADED" for x in basis["excluded_mechanisms"])
    excluded_entry = next(x for x in basis["excluded_mechanisms"] if x["mechanism"] == "STORAGE.BACKEND_DEGRADED")
    assert "ev-1" in excluded_entry["evidence_ids"]
    assert "BACKEND_HEALTHY" in excluded_entry["reason"]


def test_declarative_hypotheses_evaluation_untested():
    engine = MigrationFailureEngine(InMemoryCapabilityRegistry({}))
    state = AgentState(failure_case_id="case-003", event={"event_id": "evt-003"})
    state.classification = "GUEST.WINDOWS.VSS"
    # No matching VSS facts present
    state.evidence = [
        Evidence(id="ev-1", source="observability.search", fact="UNRELATED_FACT", status=EvidenceStatus.SUCCESS),
    ]

    hypotheses = engine._hypotheses(state)
    assert len(hypotheses) == 1
    h = hypotheses[0]
    assert h.status == "UNTESTED"
    assert h.supporting == []
    assert h.contradicting == []


def test_multiple_evidence_for_same_fact_preserves_all_ids():
    engine = MigrationFailureEngine(InMemoryCapabilityRegistry({}))
    state = AgentState(failure_case_id="case-004", event={"event_id": "evt-004"})
    state.classification = "NETWORK.NAD.MISSING"
    # Three separate observations reporting NAD_MISSING
    state.evidence = [
        Evidence(id="ev-1", source="observability.search", fact="NAD_MISSING", status=EvidenceStatus.SUCCESS),
        Evidence(id="ev-2", source="observability.search", fact="NAD_MISSING", status=EvidenceStatus.SUCCESS),
        Evidence(id="ev-3", source="observability.search", fact="NAD_MISSING", status=EvidenceStatus.SUCCESS),
    ]

    hypotheses = engine._hypotheses(state)
    assert len(hypotheses) == 1
    h = hypotheses[0]
    assert h.status == "SUPPORTED"
    # All three evidence IDs must be preserved in supporting, not just the last one
    assert set(h.supporting) == {"ev-1", "ev-2", "ev-3"}

