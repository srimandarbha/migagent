"""Explicit workflow nodes for the Migration Failure Agent.

The nodes orchestrate existing deterministic domain services.  They do not talk to
Kafka or platform APIs directly.  LangGraph owns routing; MigrationFailureEngine
owns evidence, diagnosis, safety, memory and persistence semantics.
"""
from __future__ import annotations

from typing import Any, Dict

from ...contracts import AgentState, RecoveryOption, Readiness
from ...memory.context import MemoryContext
from ...rules.evidence_policy import adaptive_for, optional_for
from ...tools.investigation import InvestigationTool


class MigrationFailureNodes:
    """Node implementation layer used by the LangGraph orchestration graph."""

    def __init__(self, engine):
        self.engine = engine
        self.tool = InvestigationTool(engine.registry, engine.tracker, engine.knowledge)

    @staticmethod
    def _state(graph_state: Dict[str, Any]) -> AgentState:
        state = graph_state.get("agent_state")
        if isinstance(state, AgentState):
            return state
        if isinstance(state, dict):
            return AgentState.from_dict(state)
        raise TypeError("graph state must contain AgentState-compatible data under 'agent_state'")

    @staticmethod
    def _pack(
        state: AgentState,
        graph_state: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Return the updated domain state without dropping graph-level state.

        LangGraph carries both the serialized AgentState and orchestration
        metadata such as the original request and adaptive collection inputs.
        Dropping the latter causes downstream nodes to fail when they access
        graph_state["request"].
        """
        return {
            **graph_state,
            "agent_state": state.to_dict(),
        }

    def create_case(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        request = graph_state["request"]
        state = self.engine._create_state(request)
        return self._pack(state, graph_state)

    def classify(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        self.engine._classify(state)
        return self._pack(state, graph_state)

    def persist_case(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        self.engine._persist_case_event(state, graph_state["request"])
        return self._pack(state, graph_state)

    def load_context(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        self.engine._load_context(state, graph_state["request"])
        return self._pack(state, graph_state)

    def load_environment(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        self.engine._load_environment(state)
        return self._pack(state, graph_state)

    def check_knowledge_compatibility(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        self.engine._check_knowledge_compatibility(state)
        return self._pack(state, graph_state)

    def judge_knowledge_applicability(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        state.knowledge_judge = self.engine._judge_knowledge_applicability(state)
        state.trace.append(f"knowledge judge status={state.knowledge_judge.get('status')}")
        return self._pack(state, graph_state)

    def correlate_recurrence(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        self.engine._correlate_recurrence(state)
        return self._pack(state, graph_state)

    def build_evidence_plan(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        self.engine._initialize_evidence_plan(state)
        state.next_evidence_requests = []
        state.trace.append("evidence plan initialized")
        return self._pack(state, graph_state)

    def collect_evidence(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        requirements = graph_state.get("collection_requirements")
        if requirements is None:
            self.engine._collect_round(state, self.tool, required_only=True)
        else:
            self.engine._collect_round(state, self.tool, requirements=requirements)
        return {**self._pack(state, graph_state), "collection_requirements": None}

    def evaluate_evidence(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        evaluation = self.engine._evaluate_evidence(state)
        state.evidence_evaluation = evaluation
        state.trace.append(f"evidence evaluation={evaluation['status']}")
        return self._pack(state, graph_state)

    def plan_next_evidence(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        requests = self.engine._select_next_evidence(state)
        state.next_evidence_requests = requests
        if not requests:
            state.trace.append("adaptive investigation stopped: no approved next evidence")
            return {**self._pack(state, graph_state), "collection_requirements": None}

        state.evidence_round += 1
        state.investigation_history.append({"round": state.evidence_round, "requests": requests})
        state.trace.append(f"adaptive evidence round={state.evidence_round} requests={len(requests)}")
        return {**self._pack(state, graph_state), "collection_requirements": requests}

    def collect_optional(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        if state.evidence_evaluation.get("diagnosis_status") == "SUFFICIENT":
            for req in optional_for(state.classification)[:1]:
                self.engine._collect_round(state, self.tool, requirements=[req])
        return self._pack(state, graph_state)

    def evaluate_hypotheses(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        state.hypotheses = self.engine._hypotheses(state)
        state.trace.append(f"hypotheses evaluated={len(state.hypotheses)}")
        return self._pack(state, graph_state)

    def diagnose(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        state.diagnosis = self.engine._diagnose(state)
        state.mechanism = state.diagnosis.get("mechanism")
        state.diagnosis_basis = self.engine._diagnosis_basis(state)
        return self._pack(state, graph_state)

    def llm_advisory(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        state.llm_advisory = self.engine._llm_advisory(state)
        return self._pack(state, graph_state)

    def calculate_readiness(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        state.evidence_evaluation["diagnosis_status"] = (
            "SUFFICIENT" if state.diagnosis.get("status") != "INSUFFICIENT_EVIDENCE" else "INSUFFICIENT"
        )
        state.recovery.clear()
        for action in ["CONTINUE_MONITOR", "RETRY", "FIX_FORWARD", "ROLLBACK", "ESCALATE"]:
            readiness, blockers = self.engine._gate_recovery(state, action)
            state.recovery.append(
                RecoveryOption(action, readiness, blockers, approval_required=action != "CONTINUE_MONITOR")
            )
        state.decision_readiness = self.engine._decision_readiness(state)
        return self._pack(state, graph_state)

    def recommend(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        state.status = "COMPLETED" if state.diagnosis.get("status") != "INSUFFICIENT_EVIDENCE" else "INSUFFICIENT_EVIDENCE"
        state.next_step = self.engine._next_step(state)
        state.investigation_package = self.engine._investigation_package(state)
        state.recommendation = self.engine._recommendation(state)
        return self._pack(state, graph_state)

    def persist(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        self.engine._persist_results(state)
        return self._pack(state, graph_state)

    def finalize(self, graph_state: Dict[str, Any]) -> Dict[str, Any]:
        state = self._state(graph_state)
        state.trace.append("workflow finalized")
        return self._pack(state, graph_state)

    def route_after_knowledge_compatibility(self, graph_state: Dict[str, Any]) -> str:
        state = self._state(graph_state)
        environment = state.environment.to_dict() if state.environment else {}
        failure = {
            'failure_code': state.event.get('failure_code') or state.event.get('error_code'),
            'classification': state.classification,
            'failure_class': state.event.get('failure_class'),
        }
        from ...knowledge_compatibility import judge_eligibility
        return 'judge' if judge_eligibility(environment, failure, state.knowledge_candidates) else 'continue'

    def route_after_evidence(self, graph_state: Dict[str, Any]) -> str:
        state = self._state(graph_state)
        evaluation = state.evidence_evaluation
        if evaluation.get("diagnosis_status") == "SUFFICIENT":
            return "sufficient"

        adaptive = adaptive_for(state.classification)
        max_rounds = int(adaptive.get("max_rounds", 0) or 0)
        if (
            adaptive.get("enabled", False)
            and state.evidence_round < max_rounds
            and bool(state.next_evidence_requests or evaluation.get("missing_evidence"))
            and state.iteration < self.engine.max_iterations
        ):
            return "investigate"
        return "terminate"

    def route_after_plan(self, graph_state: Dict[str, Any]) -> str:
        state = self._state(graph_state)
        return "collect" if state.next_evidence_requests and state.iteration < self.engine.max_iterations else "terminate"

    def route_after_diagnosis(self, graph_state: Dict[str, Any]) -> str:
        state = self._state(graph_state)
        return "advisory" if state.diagnosis.get("status") == "INSUFFICIENT_EVIDENCE" else "readiness"
