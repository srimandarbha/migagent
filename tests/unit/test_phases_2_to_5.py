"""Unit tests for Phases 2, 3, 4, and 5:
- Phase 2: Evidence-weighted dynamic hypothesis scoring.
- Phase 3: Multi-hypothesis evaluation (EXCLUSIVE, DOMINANT, CO_OCCURRING, AMBIGUOUS).
- Phase 4: Causal chain reasoning (symptom -> mechanism -> contributing_factors -> root_cause).
- Phase 5: Recovery feasibility decoupling (failure permanence and maintenance window constraints).
"""
import pytest
from engine.contracts import AgentState, Evidence, EvidenceStatus, Hypothesis, Readiness
from engine.rules.hypothesis_scorer import (
    evaluate_hypothesis_score,
    resolve_multi_hypothesis_topology,
    HypothesisTopology,
)
from engine.rules.causal_chains import build_causal_chain
from engine.rules.recovery_feasibility import (
    evaluate_recovery_feasibility,
    FailurePermanence,
)
from engine.rules.safety_gates import gate_recovery
from engine.workflow.engine import MigrationFailureEngine
from engine.integrations.registry import InMemoryCapabilityRegistry


# --- Phase 2: Evidence-Weighted Dynamic Scoring ---

def test_phase2_dynamic_scoring_with_freshness_decay():
    item = {
        "id": "H-TEST",
        "mechanism": "STORAGE.DEGRADED",
        "score": 0.85,
        "supporting": {"all": ["BACKEND_UNHEALTHY"]},
    }

    # Fresh evidence (within 300s) -> high confidence
    fresh_ev = Evidence(
        id="ev-1", source="splunk", fact="BACKEND_UNHEALTHY", status=EvidenceStatus.SUCCESS,
        freshness_seconds=60.0, reliability=0.98,
    )
    sh_fresh = evaluate_hypothesis_score(item, {"BACKEND_UNHEALTHY": [fresh_ev]}, "STORAGE.CSI.PROVISIONING_TIMEOUT")
    assert sh_fresh.hypothesis.status == "SUPPORTED"
    assert sh_fresh.dynamic_score >= 0.85

    # Stale evidence (7200s old) -> lower dynamic confidence
    stale_ev = Evidence(
        id="ev-2", source="splunk", fact="BACKEND_UNHEALTHY", status=EvidenceStatus.SUCCESS,
        freshness_seconds=7200.0, reliability=0.98,
    )
    sh_stale = evaluate_hypothesis_score(item, {"BACKEND_UNHEALTHY": [stale_ev]}, "STORAGE.CSI.PROVISIONING_TIMEOUT")
    assert sh_stale.freshness_factor < sh_fresh.freshness_factor
    assert sh_stale.supporting_quality < sh_fresh.supporting_quality


def test_phase2_contradiction_penalties():
    item = {
        "id": "H-TEST-CONTRA",
        "mechanism": "STORAGE.DEGRADED",
        "score": 0.90,
        "supporting": {"all": ["PVC_PENDING"]},
        "contradicting": {"any": ["BACKEND_HEALTHY"]},
    }
    # Strong contradiction
    ev_sup = Evidence(id="ev-1", source="splunk", fact="PVC_PENDING", status=EvidenceStatus.SUCCESS)
    ev_contra = Evidence(id="ev-2", source="splunk", fact="BACKEND_HEALTHY", status=EvidenceStatus.SUCCESS, reliability=0.95)

    sh = evaluate_hypothesis_score(item, {"PVC_PENDING": [ev_sup], "BACKEND_HEALTHY": [ev_contra]}, "STORAGE.CSI.PROVISIONING_TIMEOUT")
    assert sh.hypothesis.status == "CONTRADICTED"
    assert sh.dynamic_score < 0.50
    assert "ev-2" in sh.hypothesis.contradicting


# --- Phase 3: Multi-Hypothesis Evaluation ---

def test_phase3_multi_hypothesis_topology_dominant_vs_co_occurring():
    h1 = Hypothesis(id="H-1", code="MECH-1", description="", score=0.92, status="SUPPORTED")
    h2 = Hypothesis(id="H-2", code="MECH-2", description="", score=0.70, status="SUPPORTED")

    from engine.rules.hypothesis_scorer import ScoredHypothesis
    sh1 = ScoredHypothesis(hypothesis=h1, topology=HypothesisTopology.SUPPORTED, base_prior=0.9, dynamic_score=0.92, supporting_quality=1.0, freshness_factor=1.0, contradiction_penalty=0.0)
    sh2 = ScoredHypothesis(hypothesis=h2, topology=HypothesisTopology.SUPPORTED, base_prior=0.7, dynamic_score=0.70, supporting_quality=1.0, freshness_factor=1.0, contradiction_penalty=0.0)

    scored, landscape = resolve_multi_hypothesis_topology([sh1, sh2])
    assert landscape["topology_status"] == "DOMINANT"
    assert landscape["leading_hypothesis"] == "H-1"
    assert landscape["margin"] == 0.22


def test_phase3_multi_hypothesis_ambiguous_topology():
    h1 = Hypothesis(id="H-1", code="MECH-1", description="", score=0.85, status="SUPPORTED")
    h2 = Hypothesis(id="H-2", code="MECH-2", description="", score=0.83, status="SUPPORTED")

    from engine.rules.hypothesis_scorer import ScoredHypothesis
    sh1 = ScoredHypothesis(hypothesis=h1, topology=HypothesisTopology.SUPPORTED, base_prior=0.85, dynamic_score=0.85, supporting_quality=1.0, freshness_factor=1.0, contradiction_penalty=0.0)
    sh2 = ScoredHypothesis(hypothesis=h2, topology=HypothesisTopology.SUPPORTED, base_prior=0.85, dynamic_score=0.83, supporting_quality=1.0, freshness_factor=1.0, contradiction_penalty=0.0)

    scored, landscape = resolve_multi_hypothesis_topology([sh1, sh2])
    assert landscape["topology_status"] == "AMBIGUOUS"
    assert landscape["differentiator_needed"] is True
    assert set(landscape["competing_hypotheses"]) == {"H-1", "H-2"}


# --- Phase 4: Causal Chain Reasoning ---

def test_phase4_causal_chain_populates_real_root_cause():
    facts = {"BACKEND_UNHEALTHY", "PVC_PENDING", "NODE_MEMORY_PRESSURE"}
    eids = {"BACKEND_UNHEALTHY": ["e1"], "PVC_PENDING": ["e2"], "NODE_MEMORY_PRESSURE": ["e3"]}

    chain = build_causal_chain("STORAGE.CSI.PROVISIONING_TIMEOUT", "STORAGE.BACKEND_DEGRADED", facts, eids)

    # Must NOT be 'UNKNOWN'
    assert chain.root_cause == "STORAGE_BACKEND_CLUSTER_DEGRADATION"
    assert "Ceph/ODF" in chain.causal_explanation
    assert "Worker node experiencing memory pressure" in chain.contributing_factors
    assert chain.mechanism == "STORAGE.BACKEND_DEGRADED"


def test_phase4_diagnosis_contains_causal_chain_in_engine():
    engine = MigrationFailureEngine(InMemoryCapabilityRegistry({}))
    state = AgentState(failure_case_id="case-causal-1", event={"event_id": "evt-1"})
    state.classification = "NETWORK.NAD.MISSING"
    state.evidence = [
        Evidence(id="ev-1", source="splunk", fact="NAD_NOT_FOUND", status=EvidenceStatus.SUCCESS),
    ]
    state.evidence_evaluation = {"diagnosis_status": "SUFFICIENT"}
    state.hypotheses = engine._hypotheses(state)

    diag = engine._diagnose(state)
    assert diag["status"] == "LIKELY"
    assert diag["root_cause"] == "UNKNOWN"
    assert diag["root_cause_candidate"] == "NETWORK_ATTACHMENT_DEFINITION_MISSING_IN_TARGET_NAMESPACE"
    assert diag["symptom"] != ""
    assert "causal_chain" in diag
    assert diag["causal_chain"]["root_cause"] == "NETWORK_ATTACHMENT_DEFINITION_MISSING_IN_TARGET_NAMESPACE"


# --- Phase 5: Recovery Feasibility Decoupling ---

def test_phase5_permanent_failure_blocks_retry():
    state = AgentState(failure_case_id="case-p5-perm", event={"event_id": "evt-perm"})
    state.classification = "GUEST.WINDOWS.BITLOCKER"

    feasibility = evaluate_recovery_feasibility(state)
    assert feasibility.permanence == FailurePermanence.PERMANENT_SPECIFICATION
    assert feasibility.is_operationally_feasible is False
    assert feasibility.recommended_action == "ROLLBACK"


def test_phase5_maintenance_window_overrun_blocks_retry():
    # 500 GB VM, 40 MB/s rate -> ~12,800 seconds (~213 minutes)
    # Remaining window: 60 minutes
    state = AgentState(
        failure_case_id="case-p5-window",
        event={
            "event_id": "evt-win",
            "disk_size_gb": 500.0,
            "transfer_rate_mb_s": 40.0,
            "maintenance_window_remaining_minutes": 60.0,
        },
    )
    state.classification = "VMWARE.CBT"  # Normally transient

    feasibility = evaluate_recovery_feasibility(state)
    assert feasibility.permanence == FailurePermanence.TRANSIENT
    assert feasibility.is_operationally_feasible is False
    assert feasibility.recommended_action == "ROLLBACK"
    assert any("exceeds remaining change window" in n for n in feasibility.feasibility_notes)


def test_phase5_gate_recovery_enforces_maintenance_window():
    state = AgentState(
        failure_case_id="case-p5-gate",
        event={
            "event_id": "evt-win",
            "disk_size_gb": 1000.0,
            "transfer_rate_mb_s": 20.0,
            "maintenance_window_remaining_minutes": 30.0,  # 30m window vs 14 hours needed!
        },
    )
    state.classification = "VMWARE.CBT"
    state.evidence_evaluation = {"diagnosis_status": "SUFFICIENT"}
    # Preconditions satisfied according to policies/vmware.cbt.yaml
    state.evidence = [
        Evidence(id="e1", source="splunk", fact="CBT_READY", domain="vmware", signal="cbt_state", status=EvidenceStatus.SUCCESS),
        Evidence(id="e2", source="splunk", fact="VM_ACCESSIBLE", domain="vmware", signal="task_state", status=EvidenceStatus.SUCCESS),
        Evidence(id="e3", source="splunk", fact="TRANSFER_NORMAL", domain="mtv", signal="transfer_errors", status=EvidenceStatus.SUCCESS),
    ]

    readiness, blockers = gate_recovery(state, "RETRY")
    assert readiness == Readiness.NOT_READY
    assert any("change window" in b for b in blockers)
