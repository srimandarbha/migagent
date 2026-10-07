"""LangGraph orchestration for the Migration Failure Agent.

Transport remains outside this graph.  The graph receives a normalized request and
returns the final AgentState.  Deterministic domain services remain the source of
truth for evidence, diagnosis and safety.
"""
from __future__ import annotations

from typing import Any, Dict

try:
    from langgraph.graph import StateGraph, START, END
except ImportError:  # pragma: no cover - exercised only when optional dependency is absent
    StateGraph = START = END = None

from .nodes.implementation import MigrationFailureNodes


def build_graph(engine):
    if StateGraph is None:
        return None

    nodes = MigrationFailureNodes(engine)
    graph = StateGraph(dict)

    graph.add_node("create_case", nodes.create_case)
    graph.add_node("classify", nodes.classify)
    graph.add_node("persist_case", nodes.persist_case)
    graph.add_node("load_context", nodes.load_context)
    graph.add_node("load_environment", nodes.load_environment)
    graph.add_node("check_knowledge_compatibility", nodes.check_knowledge_compatibility)
    graph.add_node("correlate_recurrence", nodes.correlate_recurrence)
    graph.add_node("judge_knowledge_applicability", nodes.judge_knowledge_applicability)
    graph.add_node("build_evidence_plan", nodes.build_evidence_plan)
    graph.add_node("collect_evidence", nodes.collect_evidence)
    graph.add_node("evaluate_evidence", nodes.evaluate_evidence)
    graph.add_node("plan_next_evidence", nodes.plan_next_evidence)
    graph.add_node("collect_optional", nodes.collect_optional)
    graph.add_node("evaluate_hypotheses", nodes.evaluate_hypotheses)
    graph.add_node("diagnose", nodes.diagnose)
    graph.add_node("llm_advisory", nodes.llm_advisory)
    graph.add_node("calculate_readiness", nodes.calculate_readiness)
    graph.add_node("recommend", nodes.recommend)
    graph.add_node("persist", nodes.persist)
    graph.add_node("finalize", nodes.finalize)

    graph.add_edge(START, "create_case")
    graph.add_edge("create_case", "classify")
    graph.add_edge("classify", "persist_case")
    graph.add_edge("persist_case", "load_context")
    graph.add_edge("load_context", "load_environment")
    graph.add_edge("load_environment", "check_knowledge_compatibility")
    graph.add_conditional_edges(
        "check_knowledge_compatibility",
        nodes.route_after_knowledge_compatibility,
        {"judge": "judge_knowledge_applicability", "continue": "correlate_recurrence"},
    )
    graph.add_edge("judge_knowledge_applicability", "correlate_recurrence")
    graph.add_edge("correlate_recurrence", "build_evidence_plan")
    graph.add_edge("build_evidence_plan", "collect_evidence")
    graph.add_edge("collect_evidence", "evaluate_evidence")

    graph.add_conditional_edges(
        "evaluate_evidence",
        nodes.route_after_evidence,
        {
            "sufficient": "collect_optional",
            "investigate": "plan_next_evidence",
            "terminate": "evaluate_hypotheses",
        },
    )
    graph.add_conditional_edges(
        "plan_next_evidence",
        nodes.route_after_plan,
        {"collect": "collect_evidence", "terminate": "evaluate_hypotheses"},
    )
    graph.add_edge("collect_optional", "evaluate_hypotheses")
    graph.add_edge("evaluate_hypotheses", "diagnose")
    graph.add_conditional_edges(
        "diagnose",
        nodes.route_after_diagnosis,
        {"advisory": "llm_advisory", "readiness": "calculate_readiness"},
    )
    graph.add_conditional_edges(
        "llm_advisory",
        nodes.route_after_llm_advisory,
        {"re_evaluate": "evaluate_evidence", "readiness": "calculate_readiness"},
    )
    graph.add_edge("calculate_readiness", "recommend")
    graph.add_edge("recommend", "persist")
    graph.add_edge("persist", "finalize")
    graph.add_edge("finalize", END)
    return graph.compile()

