"""Unit tests for Phase 2 telemetry hardening, causal differentiation, Splunk sanitization, and closed-loop advisory."""
import os
import pytest
from engine.contracts import AgentState, Evidence, EvidenceStatus, Hypothesis
from engine.rules.causal_chains import build_causal_chain
from engine.rules.hypothesis_scorer import (
    calculate_freshness_factor,
    evaluate_hypothesis_score,
    HypothesisTopology,
)
from engine.integrations.dmz.splunk_queries import SplunkQueryCatalog, sanitize_identifier
from engine.workflow.engine import MigrationFailureEngine
from engine.integrations.registry import InMemoryCapabilityRegistry
from engine.workflow.nodes.implementation import MigrationFailureNodes


def test_causal_chain_storage_backends_differentiation():
    eids = {
        "BACKEND_DELL_DEGRADED": ["e-dell"],
        "BACKEND_PURE_DEGRADED": ["e-pure"],
        "BACKEND_PORTWORX_DEGRADED": ["e-px"],
        "BACKEND_TRIDENT_DEGRADED": ["e-tri"],
        "BACKEND_CEPH_DEGRADED": ["e-ceph"],
        "BACKEND_HEALTH_UNKNOWN": ["e-unk"],
    }

    # 1. Dell backend fact
    chain_dell = build_causal_chain("STORAGE.CSI.PROVISIONING_TIMEOUT", "STORAGE.DEGRADED", {"BACKEND_DELL_DEGRADED"}, eids)
    assert chain_dell.root_cause == "STORAGE_BACKEND_DELL_POWERSTORE_DEGRADED"
    assert "Dell PowerStore" in chain_dell.causal_explanation

    # 2. Pure backend fact
    chain_pure = build_causal_chain("STORAGE.CSI.PROVISIONING_TIMEOUT", "STORAGE.DEGRADED", {"BACKEND_PURE_DEGRADED"}, eids)
    assert chain_pure.root_cause == "STORAGE_BACKEND_PURE_STORAGE_DEGRADED"
    assert "Pure Storage" in chain_pure.causal_explanation

    # 3. Portworx backend fact
    chain_px = build_causal_chain("STORAGE.CSI.PROVISIONING_TIMEOUT", "STORAGE.DEGRADED", {"BACKEND_PORTWORX_DEGRADED"}, eids)
    assert chain_px.root_cause == "STORAGE_BACKEND_PORTWORX_DEGRADED"
    assert "Portworx" in chain_px.causal_explanation

    # 4. Trident backend fact
    chain_tri = build_causal_chain("STORAGE.CSI.PROVISIONING_TIMEOUT", "STORAGE.DEGRADED", {"BACKEND_TRIDENT_DEGRADED"}, eids)
    assert chain_tri.root_cause == "STORAGE_BACKEND_NETAPP_TRIDENT_DEGRADED"
    assert "Trident" in chain_tri.causal_explanation

    # 5. Ceph backend fact
    chain_ceph = build_causal_chain("STORAGE.CSI.PROVISIONING_TIMEOUT", "STORAGE.DEGRADED", {"BACKEND_CEPH_DEGRADED"}, eids)
    assert chain_ceph.root_cause == "STORAGE_BACKEND_CEPH_ODF_DEGRADED"
    assert "Ceph/ODF" in chain_ceph.causal_explanation

    # 6. Generic BACKEND_UNHEALTHY refined with context
    chain_ctx_dell = build_causal_chain(
        "STORAGE.CSI.PROVISIONING_TIMEOUT",
        "STORAGE.DEGRADED",
        {"BACKEND_UNHEALTHY"},
        {"BACKEND_UNHEALTHY": ["e1"]},
        context={"storage_backend": "dell-powerstore"},
    )
    assert chain_ctx_dell.root_cause == "STORAGE_BACKEND_DELL_POWERSTORE_DEGRADED"

    # 7. BACKEND_HEALTH_UNKNOWN contributing factor
    chain_unk = build_causal_chain("STORAGE.CSI.PROVISIONING_TIMEOUT", "STORAGE.DEGRADED", {"BACKEND_HEALTH_UNKNOWN"}, eids)
    assert any("could not be verified" in f for f in chain_unk.contributing_factors)


def test_splunk_queries_sanitization_and_injection_prevention():
    catalog = SplunkQueryCatalog()

    # Test SPL injection attempts are sanitized
    malicious_cluster = 'prod-c1" OR 1=1 | eval exploited="true"'
    query = catalog.build_query("virt_v2v_logs", {
        "cluster_id": malicious_cluster,
        "vm_id": 'vm-123; rm -rf /',
    })

    # Quotes, pipe, and semicolons must be stripped
    assert '"prod-c1" OR 1=1 |' not in query
    assert "prod-c1OR11evalexploitedtrue" in query
    assert "vm-123rm-rf" in query
    assert "| head 500" in query

    # Fallback template includes table projection and head 500
    fallback = catalog.build_query("unknown_custom_query", {
        "cluster_id": "c1",
        "vm_id": "v1",
        "migration_id": "m1",
    })
    assert "| head 500" in fallback
    assert "table _time, pod_name, message" in fallback


def test_hypothesis_scorer_conservative_freshness_and_dynamic_weight():
    # 1. Unmeasured freshness is 0.50 (fail-closed / conservative, not 0.90)
    assert calculate_freshness_factor(None) == 0.50
    assert calculate_freshness_factor(60.0) == 1.0
    assert calculate_freshness_factor(7200.0) == 0.60

    item = {
        "id": "H-TEST",
        "mechanism": "STORAGE.DEGRADED",
        "score": 0.90,
        "supporting": {"all": ["BACKEND_DELL_DEGRADED"]},
    }

    # High quality fresh evidence (1.0 reliability, 60s freshness)
    ev_fresh = Evidence(
        id="e-fresh",
        source="splunk",
        fact="BACKEND_DELL_DEGRADED",
        status=EvidenceStatus.SUCCESS,
        freshness_seconds=60.0,
        reliability=1.0,
    )
    sh_fresh = evaluate_hypothesis_score(item, {"BACKEND_DELL_DEGRADED": [ev_fresh]}, "STORAGE.CSI.PROVISIONING_TIMEOUT")
    assert sh_fresh.hypothesis.status == "SUPPORTED"
    assert sh_fresh.dynamic_score == 0.90

    # Degraded evidence (0.7 reliability, 7200s freshness)
    ev_degraded = Evidence(
        id="e-deg",
        source="splunk",
        fact="BACKEND_DELL_DEGRADED",
        status=EvidenceStatus.SUCCESS,
        freshness_seconds=7200.0,
        reliability=0.70,
    )
    sh_deg = evaluate_hypothesis_score(item, {"BACKEND_DELL_DEGRADED": [ev_degraded]}, "STORAGE.CSI.PROVISIONING_TIMEOUT")
    assert sh_deg.hypothesis.status == "SUPPORTED"
    # 0.90 * (0.70 * 0.60) = 0.90 * 0.42 = 0.378 -> clamped to max(0.40, ...) = 0.40
    assert sh_deg.dynamic_score < sh_fresh.dynamic_score
    assert sh_deg.dynamic_score == 0.40


def test_closed_loop_llm_advisory_re_evaluation_routing():
    engine = MigrationFailureEngine(InMemoryCapabilityRegistry({}))
    nodes = MigrationFailureNodes(engine)

    # Initial graph state without exploratory evidence
    graph_state = {"agent_state": AgentState(failure_case_id="case-1", event={"event_id": "evt-1"})}
    assert nodes.route_after_llm_advisory(graph_state) == "readiness"

    # State with exploratory evidence gathered (round 1)
    state_with_advisory = AgentState(failure_case_id="case-2", event={"event_id": "evt-2"})
    state_with_advisory.exploratory_round = 1
    state_with_advisory.llm_advisory = {
        "status": "ADVISORY",
        "evidence_gathered": [{"id": "E-EXPLORE-1", "fact": "BACKEND_DELL_DEGRADED"}],
    }
    graph_state2 = {"agent_state": state_with_advisory}

    # First exploratory round -> routes to re_evaluate
    route1 = nodes.route_after_llm_advisory(graph_state2)
    assert route1 == "re_evaluate"

    # Second check (round > 1) -> bound enforced, routes to readiness
    state_with_advisory.exploratory_round = 2
    route2 = nodes.route_after_llm_advisory(graph_state2)
    assert route2 == "readiness"


def test_production_gate_fails_fast_when_graph_unavailable(monkeypatch):
    monkeypatch.setenv("MFA_ENV", "production")
    engine = MigrationFailureEngine(InMemoryCapabilityRegistry({}))
    # Simulate environment where LangGraph could not be compiled
    engine._get_graph = lambda: None

    with pytest.raises(RuntimeError, match="LangGraph is required in production"):
        engine.run({"event_id": "evt-prod", "event_type": "MigrationFailed"})
