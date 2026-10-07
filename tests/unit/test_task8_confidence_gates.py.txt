"""Acceptance tests for Task 8: Diagnosis and classification confidence gates."""
from engine.contracts import AgentState, Evidence, EvidenceStatus
from engine.integrations.registry import InMemoryCapabilityRegistry
from engine.memory.dynamic_knowledge_store import DynamicKnowledgeStore, FailureSignature
from engine.workflow.engine import MigrationFailureEngine, DYNAMIC_OVERRIDE_MIN_CONFIDENCE


def test_possible_signature_not_likely():
    """Dynamic signature with only POSSIBLE evidence yields status PATTERN_MATCH_POSSIBLE, not LIKELY."""
    registry = InMemoryCapabilityRegistry({})
    store = DynamicKnowledgeStore(auto_seed=False)

    sig = FailureSignature(
        signature_id="SIG-POSSIBLE-TEST",
        canonical_pattern="test_possible",
        domain="storage",
        description="Testing partial evidence",
        mechanism="STORAGE.PARTIAL_EVIDENCE",
        required_evidence=["FACT_A", "FACT_B"],
        confidence_base=0.85,
    )
    store.register_signature(sig)

    engine = MigrationFailureEngine(registry=registry, knowledge_store=store)
    state = AgentState(
        failure_case_id="case-pos-01",
        event={"event_id": "evt-01", "message": "error"},
        evidence_round=1,
    )
    state.evidence_evaluation = {"diagnosis_status": "SUFFICIENT"}
    state.failure_signature = "SIG-POSSIBLE-TEST"
    state.evidence = [
        Evidence(id="e1", source="splunk", fact="FACT_A", status=EvidenceStatus.SUCCESS),
    ]

    diag = engine._diagnose(state)
    assert diag["status"] != "LIKELY"
    assert diag["status"] == "PATTERN_MATCH_POSSIBLE"
    assert diag["confidence"] == round(0.85 * 0.6, 3)


def test_dynamic_override_requires_threshold():
    """Dynamic match confidence must be >= DYNAMIC_OVERRIDE_MIN_CONFIDENCE (0.7) to override classification."""
    assert DYNAMIC_OVERRIDE_MIN_CONFIDENCE == 0.7

    registry = InMemoryCapabilityRegistry({})
    store = DynamicKnowledgeStore(auto_seed=False)

    # 1. Signature with 0.60 match confidence does NOT override UNKNOWN classification
    sig_60 = FailureSignature(
        signature_id="SIG-60",
        canonical_pattern="pattern_60",
        domain="network",
        description="60 percent match",
        mechanism="NETWORK.OVERRIDE_60",
        confidence_base=0.60,
    )
    store.register_signature(sig_60)

    engine = MigrationFailureEngine(registry=registry, knowledge_store=store)
    state_60 = AgentState(
        failure_case_id="case-60",
        event={"event_id": "e60", "message": "pattern_60 occurred during migration"},
    )
    engine._classify(state_60)
    assert state_60.classification == "UNKNOWN"

    # 2. Signature with 0.75 match confidence DOES override UNKNOWN classification
    sig_75 = FailureSignature(
        signature_id="SIG-75",
        canonical_pattern="pattern_75",
        domain="network",
        description="75 percent match",
        mechanism="NETWORK.OVERRIDE_75",
        confidence_base=0.75,
    )
    store.register_signature(sig_75)

    state_75 = AgentState(
        failure_case_id="case-75",
        event={"event_id": "e75", "message": "pattern_75 occurred during migration"},
    )
    engine._classify(state_75)
    assert state_75.classification == "NETWORK.OVERRIDE_75"
    assert state_75.classification_confidence == 0.75
