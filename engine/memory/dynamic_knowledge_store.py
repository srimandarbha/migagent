"""Dynamic Failure Knowledge Store.

Maintains operational failure signatures and multi-solution remediation plans
without hardcoded Python classification regexes or static code modification.

Key Architectural Guarantees:
1. Dynamic Database / Memory Model: Signatures and solutions live as structured data.
2. 1 Signature : N Solutions: A single root mechanism or signature can have multiple
   applicable solutions depending on cluster context, storage type, or severity.
3. Current Evidence Overrides Knowledge: A database match is only a candidate hypothesis.
   Current platform observations deterministically decide CONFIRMED, POSSIBLE, REJECTED,
   or INSUFFICIENT_EVIDENCE.
4. Action Ontology & Safety Gating: Every solution carries a structured ActionPlan
   validated against the ActionValidator.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from .action_ontology import (
    ActionCategory,
    ActionDefinition,
    ActionPlan,
    ActionType,
    ActionValidator,
    RiskLevel,
)
from .normalizer import FailureNormalizer, NormalizedSignature


class EvidenceEvaluationStatus(str, Enum):
    CONFIRMED = "CONFIRMED"
    POSSIBLE = "POSSIBLE"
    REJECTED = "REJECTED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


@dataclass
class KnownSolution:
    solution_id: str
    signature_id: str
    title: str
    action_summary: str
    recommended_action: str
    action_plan: ActionPlan
    automation_system: str = "MANUAL"  # MANUAL, AAP, EDA, COMMAND_CENTER
    risk_level: RiskLevel = RiskLevel.MEDIUM
    requires_approval: bool = True
    approval_role: Optional[str] = "SRE"
    success_count: int = 0
    failure_count: int = 0
    external_ref: Optional[str] = None
    source: str = "PRE_SEEDED"  # PRE_SEEDED, SRE_VALIDATED, LLM_CANDIDATE
    applicability_constraints: Dict[str, Any] = field(default_factory=dict)

    @property
    def total_executions(self) -> int:
        return self.success_count + self.failure_count

    @property
    def success_rate(self) -> float:
        if self.total_executions == 0:
            return 1.0 if self.source != "LLM_CANDIDATE" else 0.5
        return self.success_count / self.total_executions

    def record_outcome(self, success: bool) -> None:
        if success:
            self.success_count += 1
        else:
            self.failure_count += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "solution_id": self.solution_id,
            "signature_id": self.signature_id,
            "title": self.title,
            "action_summary": self.action_summary,
            "recommended_action": self.recommended_action,
            "action_plan": self.action_plan.to_dict(),
            "automation_system": self.automation_system,
            "risk_level": self.risk_level.value if isinstance(self.risk_level, RiskLevel) else str(self.risk_level),
            "requires_approval": self.requires_approval,
            "approval_role": self.approval_role,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "success_rate": round(self.success_rate, 3),
            "external_ref": self.external_ref,
            "source": self.source,
            "applicability_constraints": self.applicability_constraints,
        }


@dataclass
class FailureSignature:
    signature_id: str
    canonical_pattern: str  # Regex string or exact normalized signature
    domain: str             # storage, conversion, guest_os, network, vmware, platform, bootloader, security
    mechanism: str
    description: str
    raw_sample: Optional[str] = None
    applicability_rules: Dict[str, Any] = field(default_factory=dict)
    required_evidence: List[str] = field(default_factory=list)
    contraindicated_evidence: List[str] = field(default_factory=list)
    confidence_base: float = 0.9
    status: str = "ACTIVE"  # ACTIVE, DEPRECATED, CANDIDATE
    frequency: int = 1
    solutions: List[KnownSolution] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    _compiled_regex: Optional[re.Pattern] = field(default=None, init=False, repr=False)

    def __post_init__(self):
        try:
            self._compiled_regex = re.compile(self.canonical_pattern, re.IGNORECASE)
        except re.error:
            # Fall back to escaping if not a valid regex
            self._compiled_regex = re.compile(re.escape(self.canonical_pattern), re.IGNORECASE)

    def matches(self, raw_text: str, normalized_text: str) -> bool:
        if not self._compiled_regex:
            return False
        return bool(self._compiled_regex.search(raw_text) or self._compiled_regex.search(normalized_text))

    def evaluate_applicability(self, context: Optional[Dict[str, Any]]) -> bool:
        if not self.applicability_rules or not context:
            return True
        for k, required_val in self.applicability_rules.items():
            actual_val = context.get(k)
            if actual_val is None:
                continue
            if isinstance(required_val, list):
                if actual_val not in required_val:
                    return False
            elif actual_val != required_val:
                return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signature_id": self.signature_id,
            "canonical_pattern": self.canonical_pattern,
            "domain": self.domain,
            "mechanism": self.mechanism,
            "description": self.description,
            "raw_sample": self.raw_sample,
            "applicability_rules": self.applicability_rules,
            "required_evidence": self.required_evidence,
            "contraindicated_evidence": self.contraindicated_evidence,
            "confidence_base": self.confidence_base,
            "status": self.status,
            "frequency": self.frequency,
            "solutions": [s.to_dict() for s in self.solutions],
            "metadata": self.metadata,
        }


class DynamicKnowledgeStore:
    """In-memory and persistent repository for FailureSignatures and KnownSolutions."""

    def __init__(self, tracker: Optional[Any] = None, auto_seed: bool = True):
        self.tracker = tracker
        self._signatures: Dict[str, FailureSignature] = {}
        if auto_seed:
            self._load_seed_data()
        self._load_from_tracker()

    def _load_seed_data(self) -> None:
        try:
            from .seed_data import get_seed_signatures
            for sig in get_seed_signatures():
                self.register_signature(sig)
        except ImportError:
            pass

    def _load_from_tracker(self) -> None:
        """Attempt loading dynamic records from SRE Tracker if supported."""
        if not self.tracker:
            return
        if hasattr(self.tracker, "get_known_issues"):
            try:
                records = self.tracker.get_known_issues()
                for rec in records:
                    self._ingest_tracker_record(rec)
            except Exception:
                pass

    def _ingest_tracker_record(self, rec: Dict[str, Any]) -> None:
        sig_id = rec.get("signature_id") or rec.get("pattern_id") or f"TRACKER-{rec.get('id', 'UNK')}"
        if sig_id in self._signatures:
            # Update frequency/stats
            sig = self._signatures[sig_id]
            sig.frequency = max(sig.frequency, rec.get("occurrence_count", sig.frequency))
            return

        pattern = rec.get("pattern") or rec.get("canonical_signature") or ""
        if not pattern:
            return

        sig = FailureSignature(
            signature_id=sig_id,
            canonical_pattern=pattern,
            domain=rec.get("domain", "general"),
            mechanism=rec.get("mechanism", "UNKNOWN"),
            description=rec.get("issue_summary", rec.get("description", "")),
            frequency=rec.get("occurrence_count", 1),
            status=rec.get("status", "ACTIVE"),
        )
        if rec.get("solutions"):
            for s_data in rec["solutions"]:
                if isinstance(s_data, KnownSolution):
                    sig.solutions.append(s_data)
                elif isinstance(s_data, dict):
                    step = ActionDefinition(
                        action_id=f"{s_data.get('solution_id', sig_id)}-STEP-1",
                        action_type=ActionType.RETRY,
                        title=s_data.get("title", "Action"),
                        description=s_data.get("action_summary", ""),
                        requires_approval=s_data.get("requires_approval", True),
                        approval_role="SRE",
                    )
                    plan = ActionPlan(
                        plan_id=f"{s_data.get('solution_id', sig_id)}-PLAN-1",
                        failure_signature=sig_id,
                        steps=[step],
                        rationale=s_data.get("title", ""),
                    )
                    sol = KnownSolution(
                        solution_id=s_data.get("solution_id", f"{sig_id}-SOL"),
                        signature_id=sig_id,
                        title=s_data.get("title", "SRE Tracker Solution"),
                        action_summary=s_data.get("action_summary", ""),
                        recommended_action=s_data.get("recommended_action", s_data.get("action_summary", "")),
                        action_plan=plan,
                        automation_system=s_data.get("automation_system", "MANUAL"),
                        risk_level=RiskLevel.LOW,
                        requires_approval=s_data.get("requires_approval", True),
                        success_count=s_data.get("success_count", 0),
                        failure_count=s_data.get("failure_count", 0),
                        source="SRE_VALIDATED",
                    )
                    sig.solutions.append(sol)
        elif rec.get("recommended_action"):
            step = ActionDefinition(
                action_id=f"{sig_id}-STEP-1",
                action_type=ActionType(rec.get("action_type", "RETRY")) if rec.get("action_type") in ActionType.__members__ else ActionType.RETRY,
                title=rec.get("action_summary", "Recommended Action"),
                description=rec.get("recommended_action"),
                requires_approval=True,
                approval_role="SRE",
            )
            plan = ActionPlan(
                plan_id=f"{sig_id}-PLAN-1",
                failure_signature=sig_id,
                steps=[step],
                rationale=rec.get("issue_summary", ""),
            )
            sol = KnownSolution(
                solution_id=f"{sig_id}-SOL-1",
                signature_id=sig_id,
                title=rec.get("action_summary", "SRE Tracker Solution"),
                action_summary=rec.get("action_summary", rec.get("recommended_action", "")),
                recommended_action=rec.get("recommended_action", ""),
                action_plan=plan,
                source="SRE_VALIDATED",
            )
            sig.solutions.append(sol)
        self.register_signature(sig)

    def register_signature(self, signature: FailureSignature) -> None:
        self._signatures[signature.signature_id] = signature

    def register_solution(self, signature_id: str, solution: KnownSolution) -> None:
        if signature_id in self._signatures:
            sig = self._signatures[signature_id]
            # Replace existing if ID matches, else append
            for i, existing in enumerate(sig.solutions):
                if existing.solution_id == solution.solution_id:
                    sig.solutions[i] = solution
                    return
            sig.solutions.append(solution)

    def get_signature(self, signature_id: str) -> Optional[FailureSignature]:
        return self._signatures.get(signature_id)

    def all_signatures(self) -> List[FailureSignature]:
        return list(self._signatures.values())

    def match_failure(
        self,
        raw_text: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Tuple[FailureSignature, float]]:
        """Matches raw logs or error messages against registered signatures."""
        if not raw_text:
            return []

        norm = FailureNormalizer.normalize(raw_text)
        matches: List[Tuple[FailureSignature, float]] = []

        for sig in self._signatures.values():
            if sig.status not in ("ACTIVE", "VALIDATED"):
                continue
            if not sig.evaluate_applicability(context):
                continue

            if sig.matches(raw_text, norm.normalized_text):
                score = sig.confidence_base
                # Prioritize signatures with higher frequency
                frequency_bonus = min(0.08, (sig.frequency - 1) * 0.01)
                score = min(0.99, score + frequency_bonus)
                matches.append((sig, round(score, 3)))

        # Sort by confidence descending
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches

    def evaluate_signature_evidence(
        self,
        signature: FailureSignature,
        facts: Set[str],
    ) -> EvidenceEvaluationStatus:
        """Deterministically evaluates whether live observations corroborate the signature.
        
        Live evidence is the ultimate authority. Even a high-confidence DB signature match
        is REJECTED if contradictory evidence is observed.
        """
        # 1. Contradiction check: fail-closed
        if signature.contraindicated_evidence:
            contradicted = set(signature.contraindicated_evidence) & facts
            if contradicted:
                return EvidenceEvaluationStatus.REJECTED

        # 2. Required evidence check
        if not signature.required_evidence:
            return EvidenceEvaluationStatus.POSSIBLE

        required_set = set(signature.required_evidence)
        present = required_set & facts

        if present == required_set:
            return EvidenceEvaluationStatus.CONFIRMED
        elif present:
            return EvidenceEvaluationStatus.POSSIBLE
        else:
            return EvidenceEvaluationStatus.INSUFFICIENT_EVIDENCE

    def get_best_solution(
        self,
        signature: FailureSignature,
        facts: Optional[Set[str]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[KnownSolution]:
        """Selects the highest-rated viable solution for a given signature.
        
        Validates every candidate solution's ActionPlan through ActionValidator.
        """
        if not signature.solutions:
            return None

        viable_solutions: List[KnownSolution] = []
        for sol in signature.solutions:
            # Check context constraints
            if sol.applicability_constraints and context:
                mismatch = False
                for k, v in sol.applicability_constraints.items():
                    if context.get(k) is not None and context.get(k) != v:
                        mismatch = True
                        break
                if mismatch:
                    continue

            # Deterministic plan validation
            val_res = ActionValidator.validate_plan(sol.action_plan, facts)
            if not val_res.valid:
                continue

            viable_solutions.append(sol)

        if not viable_solutions:
            return None

        # Sort by success rate descending, then total executions descending, then risk level ascending
        risk_weights = {RiskLevel.LOW: 0, RiskLevel.MEDIUM: 1, RiskLevel.HIGH: 2, RiskLevel.CRITICAL: 3}
        viable_solutions.sort(
            key=lambda s: (
                s.success_rate,
                s.total_executions,
                -risk_weights.get(s.risk_level, 1),
            ),
            reverse=True,
        )
        return viable_solutions[0]
