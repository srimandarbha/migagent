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
    ApplicabilityStatus,
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
    verification_contract: Dict[str, Any] = field(default_factory=dict)
    provenance: Dict[str, Any] = field(default_factory=dict)

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

    def evaluate_applicability(self, context: Optional[Dict[str, Any]]) -> ApplicabilityStatus:
        """Evaluates three-state applicability for this solution.
        
        Fail-closed semantics:
        - If no constraints: APPLICABLE
        - If constraints exist but context is missing: UNKNOWN (cannot confirm)
        - If any constraint is mismatched: INAPPLICABLE
        - If any required key is missing: UNKNOWN
        - If all constraints match: APPLICABLE
        """
        if not self.applicability_constraints:
            return ApplicabilityStatus.APPLICABLE
        if not context:
            return ApplicabilityStatus.UNKNOWN
        for k, required_val in self.applicability_constraints.items():
            actual_val = context.get(k)
            if actual_val is None:
                return ApplicabilityStatus.UNKNOWN
            if isinstance(required_val, (list, tuple, set)):
                if actual_val not in required_val:
                    return ApplicabilityStatus.INAPPLICABLE
            elif actual_val != required_val:
                return ApplicabilityStatus.INAPPLICABLE
        return ApplicabilityStatus.APPLICABLE

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
            "verification_contract": self.verification_contract,
            "provenance": self.provenance,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "KnownSolution":
        plan_data = d.get("action_plan")
        if isinstance(plan_data, dict):
            plan = ActionPlan.from_dict(plan_data)
        elif isinstance(plan_data, ActionPlan):
            plan = plan_data
        else:
            plan = ActionPlan(
                plan_id=f"{d.get('solution_id', 'SOL')}-PLAN",
                failure_signature=d.get("signature_id", "UNK"),
                rationale=d.get("title", ""),
            )
        risk = RiskLevel(d.get("risk_level", "LOW")) if d.get("risk_level") in RiskLevel.__members__ else RiskLevel.LOW
        return cls(
            solution_id=d.get("solution_id", "SOL-UNK"),
            signature_id=d.get("signature_id", "UNK"),
            title=d.get("title", "Solution"),
            action_summary=d.get("action_summary", ""),
            recommended_action=d.get("recommended_action", d.get("action_summary", "")),
            action_plan=plan,
            automation_system=d.get("automation_system", "MANUAL"),
            risk_level=risk,
            requires_approval=bool(d.get("requires_approval", True)),
            approval_role=d.get("approval_role", "SRE"),
            success_count=int(d.get("success_count", 0)),
            failure_count=int(d.get("failure_count", 0)),
            external_ref=d.get("external_ref"),
            source=d.get("source", "MANUAL"),
            applicability_constraints=dict(d.get("applicability_constraints", {})),
            verification_contract=dict(d.get("verification_contract", {})),
            provenance=dict(d.get("provenance", {})),
        )


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

    def evaluate_applicability(self, context: Optional[Dict[str, Any]]) -> ApplicabilityStatus:
        """Evaluates three-state applicability for this failure signature.
        
        Fail-closed semantics:
        - If no applicability rules: APPLICABLE
        - If rules exist but context is missing: UNKNOWN (cannot confirm)
        - If any rule is mismatched: INAPPLICABLE
        - If any required key is missing: UNKNOWN
        - If all rules match: APPLICABLE
        """
        if not self.applicability_rules:
            return ApplicabilityStatus.APPLICABLE
        if not context:
            return ApplicabilityStatus.UNKNOWN
        for k, required_val in self.applicability_rules.items():
            actual_val = context.get(k)
            if actual_val is None:
                return ApplicabilityStatus.UNKNOWN
            if isinstance(required_val, (list, tuple, set)):
                if actual_val not in required_val:
                    return ApplicabilityStatus.INAPPLICABLE
            elif actual_val != required_val:
                return ApplicabilityStatus.INAPPLICABLE
        return ApplicabilityStatus.APPLICABLE

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

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FailureSignature":
        solutions = [KnownSolution.from_dict(s) if isinstance(s, dict) else s for s in d.get("solutions", [])]
        return cls(
            signature_id=d.get("signature_id", "SIG-UNK"),
            canonical_pattern=d.get("canonical_pattern", d.get("pattern", "")),
            domain=d.get("domain", "general"),
            mechanism=d.get("mechanism", "UNKNOWN"),
            description=d.get("description", d.get("issue_summary", "")),
            raw_sample=d.get("raw_sample"),
            applicability_rules=dict(d.get("applicability_rules", {})),
            required_evidence=list(d.get("required_evidence", [])),
            contraindicated_evidence=list(d.get("contraindicated_evidence", [])),
            confidence_base=float(d.get("confidence_base", 0.9)),
            status=d.get("status", "ACTIVE"),
            frequency=int(d.get("frequency", d.get("occurrence_count", 1))),
            solutions=solutions,
            metadata=dict(d.get("metadata", {})),
        )


class DynamicKnowledgeStore:
    """In-memory and persistent repository for FailureSignatures and KnownSolutions."""

    def __init__(self, tracker: Optional[Any] = None, auto_seed: Optional[bool] = None):
        self.tracker = tracker
        self._signatures: Dict[str, FailureSignature] = {}
        # DB-first discipline: If tracker is provided, PostgreSQL is the authoritative truth.
        # Auto-seed is disabled by default when tracker is present, enabled when tracker is None.
        if auto_seed is None:
            auto_seed = (tracker is None)
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
            # Live DB records override Python seed data
            sig = self._signatures[sig_id]
            if rec.get("status"):
                sig.status = rec["status"]
            if rec.get("mechanism"):
                sig.mechanism = rec["mechanism"]
            if rec.get("domain"):
                sig.domain = rec["domain"]
            if rec.get("pattern") or rec.get("canonical_signature"):
                sig.canonical_pattern = rec.get("pattern") or rec.get("canonical_signature")
                sig.__post_init__()
            if rec.get("issue_summary") or rec.get("description"):
                sig.description = rec.get("issue_summary") or rec.get("description")
            if "applicability_rules" in rec and rec["applicability_rules"] is not None:
                sig.applicability_rules = dict(rec["applicability_rules"])
            if "required_evidence" in rec and rec["required_evidence"] is not None:
                sig.required_evidence = list(rec["required_evidence"])
            if "contraindicated_evidence" in rec and rec["contraindicated_evidence"] is not None:
                sig.contraindicated_evidence = list(rec["contraindicated_evidence"])
            if "metadata" in rec and rec["metadata"] is not None:
                sig.metadata = dict(rec["metadata"])
            sig.frequency = max(sig.frequency, rec.get("occurrence_count", sig.frequency))

            # Update / replace solutions if provided in DB record
            if rec.get("solutions"):
                updated_solutions = []
                for s_data in rec["solutions"]:
                    if isinstance(s_data, KnownSolution):
                        updated_solutions.append(s_data)
                    elif isinstance(s_data, dict):
                        updated_solutions.append(KnownSolution.from_dict(s_data))
                if updated_solutions:
                    sig.solutions = updated_solutions
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
            applicability_rules=dict(rec.get("applicability_rules", {})),
            required_evidence=list(rec.get("required_evidence", [])),
            contraindicated_evidence=list(rec.get("contraindicated_evidence", [])),
            confidence_base=float(rec.get("confidence_base", 0.9)),
            status=rec.get("status", "ACTIVE"),
            metadata=dict(rec.get("metadata", {})),
        )
        if rec.get("solutions"):
            for s_data in rec["solutions"]:
                if isinstance(s_data, KnownSolution):
                    sig.solutions.append(s_data)
                elif isinstance(s_data, dict):
                    sig.solutions.append(KnownSolution.from_dict(s_data))
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
            app_status = sig.evaluate_applicability(context)
            if app_status == ApplicabilityStatus.INAPPLICABLE:
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
            # Check context constraints via three-state evaluation
            sol_app = sol.evaluate_applicability(context)
            if sol_app == ApplicabilityStatus.INAPPLICABLE:
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
