"""Learning Candidate Pipeline.

Operational Learning Architecture:
1. Candidate Isolation: Unknown failures, LLM-generated suggestions, and one-off retry
   successes do NOT directly modify the active trusted knowledge base.
2. Learning Candidates: New failure patterns and suggested actions are captured as
   `LearningCandidate` records with status `PENDING_VALIDATION`.
3. SRE Gatekeeper: Promotion to active operational knowledge (`FailureSignature` and
   `KnownSolution`) requires explicit human SRE validation with a recorded rationale.
4. Frequency & Metric Tracking: Occurrence counts, success counts, and failure rates
   are maintained dynamically so trusted solutions gain confidence over time.
"""
from __future__ import annotations

import datetime
import hashlib
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from .action_ontology import (
    ActionDefinition,
    ActionPlan,
    ActionType,
    ActionValidator,
    RiskLevel,
)
from .dynamic_knowledge_store import DynamicKnowledgeStore, FailureSignature, KnownSolution
from .normalizer import FailureNormalizer


@dataclass
class LearningCandidate:
    candidate_id: str
    signature_hash: str
    normalized_pattern: str
    raw_log_sample: str
    suggested_domain: str
    suggested_mechanism: str
    suggested_action: str
    suggested_action_type: str = "RETRY"
    status: str = "PENDING_VALIDATION"  # PENDING_VALIDATION, VALIDATED, REJECTED
    occurrence_count: int = 1
    success_count: int = 0
    failure_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat())
    validated_by: Optional[str] = None
    validation_reason: Optional[str] = None
    labels: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.5
        return self.success_count / total

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class LearningPipeline:
    """Manages the lifecycle of operational learning candidates."""

    def __init__(self, store: DynamicKnowledgeStore, tracker: Optional[Any] = None):
        self.store = store
        self.tracker = tracker
        self._candidates: Dict[str, LearningCandidate] = {}
        self._load_from_tracker()

    def _load_from_tracker(self) -> None:
        """P0-4: Rehydrate learning candidates from SRE Tracker so candidates survive process restarts."""
        if not self.tracker or not hasattr(self.tracker, "get_learning_candidates"):
            return
        try:
            records = self.tracker.get_learning_candidates(status="PENDING_VALIDATION")
            if not records:
                records = self.tracker.get_learning_candidates()
            for r in records:
                shash = r.get("signature_hash") or r.get("failure_signature") or ""
                cid = r.get("candidate_id") or (f"CAND-{shash[:8].upper()}" if shash else None)
                if not cid:
                    continue
                self._candidates[cid] = LearningCandidate(
                    candidate_id=cid,
                    signature_hash=shash,
                    normalized_pattern=r.get("normalized_pattern") or r.get("description", ""),
                    raw_log_sample=r.get("raw_log_sample") or r.get("description", ""),
                    suggested_domain=r.get("suggested_domain") or r.get("failure_class", "general"),
                    suggested_mechanism=r.get("suggested_mechanism") or r.get("diagnosis_code") or "UNKNOWN",
                    suggested_action=r.get("suggested_action") or r.get("resolution_code") or "SRE_REVIEW",
                    suggested_action_type=r.get("suggested_action_type", "RETRY"),
                    status=r.get("status", "PENDING_VALIDATION"),
                    occurrence_count=r.get("occurrence_count", r.get("verified_success_count", 1)),
                    metadata=dict(r.get("metadata") or r.get("applicability", {})),
                )
        except Exception:
            pass

    def get_candidate(self, candidate_id: str) -> Optional[LearningCandidate]:
        return self._candidates.get(candidate_id)

    def list_candidates(self, status: Optional[str] = None) -> List[LearningCandidate]:
        if status:
            return [c for c in self._candidates.values() if c.status == status]
        return list(self._candidates.values())

    def ingest_unknown_failure(
        self,
        raw_log: str,
        suggested_mechanism: str = "UNKNOWN",
        suggested_action: str = "SRE_REVIEW",
        suggested_action_type: str = "RETRY",
        suggested_domain: str = "general",
        labels: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> LearningCandidate:
        """Ingests an unclassified or newly observed failure pattern as a learning candidate.
        
        Does NOT alter active FailureSignatures until validated by an SRE.
        """
        norm = FailureNormalizer.normalize(raw_log)
        sig_hash = norm.signature_hash

        # Check if candidate already exists
        for c in self._candidates.values():
            if c.signature_hash == sig_hash:
                c.occurrence_count += 1
                c.updated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
                if self.tracker and hasattr(self.tracker, "save_learning_candidate"):
                    try:
                        self.tracker.save_learning_candidate(c.to_dict())
                    except Exception:
                        pass
                return c

        candidate_id = f"CAND-{sig_hash[:8].upper()}"
        candidate = LearningCandidate(
            candidate_id=candidate_id,
            signature_hash=sig_hash,
            normalized_pattern=norm.normalized_text,
            raw_log_sample=raw_log[:500],
            suggested_domain=suggested_domain,
            suggested_mechanism=suggested_mechanism,
            suggested_action=suggested_action,
            suggested_action_type=suggested_action_type,
            labels=labels or [],
            metadata=metadata or {},
        )
        self._candidates[candidate_id] = candidate

        # P0-4: Durably persist candidate to tracker/persistence so it survives pod restarts
        if self.tracker and hasattr(self.tracker, "save_learning_candidate"):
            try:
                db_cid = self.tracker.save_learning_candidate(candidate.to_dict())
                if db_cid:
                    candidate.metadata["db_candidate_id"] = str(db_cid)
            except Exception:
                pass

        return candidate

    def record_action_outcome(
        self,
        signature_id: str,
        solution_id: Optional[str] = None,
        success: bool = True,
    ) -> None:
        """Records an execution outcome (success or failure) against a solution or candidate."""
        sig = self.store.get_signature(signature_id)
        if sig:
            sig.frequency += 1
            if solution_id:
                for sol in sig.solutions:
                    if sol.solution_id == solution_id:
                        sol.record_outcome(success)
                        break

        # Also check candidates
        for c in self._candidates.values():
            if c.candidate_id == signature_id or c.signature_hash in signature_id:
                if success:
                    c.success_count += 1
                else:
                    c.failure_count += 1
                c.updated_at = datetime.datetime.utcnow().isoformat() + "Z"
                break

    def promote_candidate(
        self,
        candidate_id: str,
        validated_by: str,
        validation_reason: str,
        risk_level: RiskLevel = RiskLevel.MEDIUM,
        automation_system: str = "MANUAL",
        approval_role: str = "SRE",
    ) -> FailureSignature:
        """Promotes a validated learning candidate into the active DynamicKnowledgeStore.
        
        Fail-closed: requires non-empty validated_by and validation_reason.
        """
        candidate = self._candidates.get(candidate_id)
        if not candidate:
            raise KeyError(f"Candidate {candidate_id} not found")

        if not validated_by or not validation_reason:
            raise ValueError("SRE validation requires explicit validated_by and validation_reason")

        candidate.status = "VALIDATED"
        candidate.validated_by = validated_by
        candidate.validation_reason = validation_reason
        candidate.updated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Build ActionPlan
        action_type = ActionType(candidate.suggested_action_type) if candidate.suggested_action_type in ActionType.__members__ else ActionType.RETRY
        action_def = ActionDefinition(
            action_id=f"{candidate_id}-ACT-1",
            action_type=action_type,
            title=f"Validated Action for {candidate.suggested_mechanism}",
            description=candidate.suggested_action,
            risk_level=risk_level,
            requires_approval=True,
            approval_role=approval_role,
        )
        plan = ActionPlan(
            plan_id=f"{candidate_id}-PLAN-1",
            failure_signature=candidate.signature_hash,
            steps=[action_def],
            rationale=validation_reason,
            confidence=0.9,
            overall_risk=risk_level,
            requires_human_approval=True,
        )

        # Validate through ActionValidator
        val_res = ActionValidator.validate_plan(plan)
        if not val_res.valid:
            raise ValueError(f"Plan validation failed for candidate {candidate_id}: {val_res.errors}")

        sol_id = f"SOL-{candidate_id}"
        solution = KnownSolution(
            solution_id=sol_id,
            signature_id=candidate_id,
            title=f"SRE Solution for {candidate.suggested_mechanism}",
            action_summary=candidate.suggested_action,
            recommended_action=candidate.suggested_action,
            action_plan=plan,
            automation_system=automation_system,
            risk_level=risk_level,
            requires_approval=True,
            approval_role=approval_role,
            success_count=candidate.success_count,
            failure_count=candidate.failure_count,
            source="SRE_VALIDATED",
        )

        sig = FailureSignature(
            signature_id=candidate_id,
            canonical_pattern=candidate.normalized_pattern,
            domain=candidate.suggested_domain,
            mechanism=candidate.suggested_mechanism,
            description=candidate.suggested_mechanism,
            raw_sample=candidate.raw_log_sample,
            frequency=candidate.occurrence_count,
            status="ACTIVE",
            solutions=[solution],
            metadata={"promoted_from": candidate_id, "validated_by": validated_by, "reason": validation_reason},
        )

        # Register in knowledge store
        self.store.register_signature(sig)

        # Mark candidate validated in SRE Tracker if available
        if self.tracker and hasattr(self.tracker, "validate_learning_candidate"):
            try:
                cid_to_val = candidate.metadata.get("db_candidate_id") or candidate.signature_hash or candidate_id
                self.tracker.validate_learning_candidate(
                    candidate_id=cid_to_val,
                    validated_by=validated_by,
                    validation_reason=validation_reason,
                )
            except Exception:
                pass

        # Persist to SRE Tracker if available
        if self.tracker and hasattr(self.tracker, "save_known_issue"):
            try:
                self.tracker.save_known_issue(sig.to_dict())
            except Exception:
                pass

        return sig
