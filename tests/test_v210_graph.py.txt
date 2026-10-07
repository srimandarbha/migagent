import importlib.util

import pytest

from engine.workflow.engine import MigrationFailureEngine
from engine.workflow.nodes.implementation import MigrationFailureNodes
from simulator.runner import make_world


def test_graph_node_sequence_storage_adaptive_loop():
    scenario, registry, tracker, knowledge = make_world('storage-csi-backend-healthy')
    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=knowledge, memory_mode='both')
    nodes = MigrationFailureNodes(engine)
    request = {'failure_case_id': 'graph-001', 'memory_mode': 'both', 'event': {
        'event_id': 'graph-001', 'event_type': 'MigrationFailed',
        'scenario': 'storage-csi-backend-healthy', 'phase': scenario['phase'],
        'message': scenario['message'], 'cluster_id': 'ocv-prod-a'
    }}
    gs = {'request': request}
    for node in (nodes.create_case, nodes.classify, nodes.load_context, nodes.build_evidence_plan, nodes.collect_evidence):
        gs.update(node(gs))
    gs.update(nodes.evaluate_evidence(gs))
    assert nodes.route_after_evidence(gs) == 'investigate'
    gs.update(nodes.plan_next_evidence(gs))
    assert nodes.route_after_plan(gs) == 'collect'
    gs.update(nodes.collect_evidence(gs))
    gs.update(nodes.evaluate_evidence(gs))
    assert nodes.route_after_evidence(gs) in {'investigate', 'sufficient'}


def test_graph_nodes_do_not_send_llm_advisory_for_deterministic_diagnosis():
    scenario, registry, tracker, knowledge = make_world('storage-csi-backend-healthy')
    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=knowledge, memory_mode='both')
    nodes = MigrationFailureNodes(engine)
    state = engine.run({'failure_case_id': 'graph-002', 'event': {
        'event_id': 'graph-002', 'event_type': 'MigrationFailed',
        'scenario': 'storage-csi-backend-healthy', 'phase': scenario['phase'],
        'message': scenario['message'], 'cluster_id': 'ocv-prod-a'
    }})
    assert state.diagnosis['status'] == 'LIKELY'
    assert state.llm_advisory['status'] == 'NOT_REQUESTED'


def test_graph_routing_terminates_when_adaptive_limit_reached():
    scenario, registry, tracker, knowledge = make_world('storage-csi-timeout')
    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=knowledge, memory_mode='none')
    nodes = MigrationFailureNodes(engine)
    state = engine._create_state({'failure_case_id': 'graph-003', 'event': {
        'event_id': 'graph-003', 'event_type': 'MigrationFailed',
        'scenario': 'storage-csi-timeout', 'phase': scenario['phase'],
        'message': scenario['message']
    }})
    state.classification = 'STORAGE.CSI.PROVISIONING_TIMEOUT'
    state.evidence_round = 3
    state.evidence_evaluation = {'diagnosis_status': 'INSUFFICIENT', 'missing_evidence': [{'capability':'observability.search','parameters':{'domain':'ocv','signal':'csi_latency'}}]}
    state.next_evidence_requests = state.evidence_evaluation['missing_evidence']
    assert nodes.route_after_evidence({'agent_state': state}) == 'terminate'


def test_compiled_langgraph_preserves_request_across_nodes():
    """Regression test for graph-level request state propagation.

    The compiled LangGraph must preserve the original request after create_case,
    otherwise persist_case/load_context fail with KeyError("request").
    """
    from engine.workflow.graph import build_graph

    scenario, registry, tracker, knowledge = make_world('storage-csi-backend-healthy')
    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=knowledge, memory_mode='both')
    graph = build_graph(engine)
    if graph is None:
        pytest.skip('LangGraph is not installed in this test environment')
    request = {'failure_case_id': 'graph-state-001', 'memory_mode': 'both', 'event': {
        'event_id': 'graph-state-001', 'event_type': 'MigrationFailed',
        'scenario': 'storage-csi-backend-healthy', 'phase': scenario['phase'],
        'message': scenario['message'], 'cluster_id': 'ocv-prod-a'
    }}

    result = graph.invoke({'request': request})

    assert result['request'] == request
    assert 'agent_state' in result
    # failure_case_id is the persisted SRE Tracker identifier. The request
    # correlation key remains graph-state-001 and is asserted above.
    assert result['agent_state']['failure_case_id']
    assert result['agent_state']['failure_case_id'] != ''


def test_node_pack_preserves_request_state_without_langgraph_runtime():
    """The node boundary itself must preserve graph-level request metadata."""
    scenario, registry, tracker, knowledge = make_world('storage-csi-backend-healthy')
    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=knowledge, memory_mode='both')
    nodes = MigrationFailureNodes(engine)
    request = {'failure_case_id': 'graph-state-002', 'memory_mode': 'both', 'event': {
        'event_id': 'graph-state-002', 'event_type': 'MigrationFailed',
        'scenario': 'storage-csi-backend-healthy', 'phase': scenario['phase'],
        'message': scenario['message'], 'cluster_id': 'ocv-prod-a'
    }}

    created = nodes.create_case({'request': request})
    assert created['request'] == request
    classified = nodes.classify(created)
    assert classified['request'] == request
    persisted = nodes.persist_case(classified)
    assert persisted['request'] == request
    loaded = nodes.load_context(persisted)
    assert loaded['request'] == request
