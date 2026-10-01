"""Post-incident resolution and knowledge-candidate lifecycle."""
from __future__ import annotations

from typing import Any

from .rules.learning import evaluate_learning


class LearningLifecycle:
    """Records what actually happened after an SRE action.

    This service is intentionally separate from the diagnostic graph. The initial
    agent is read-only; this lifecycle consumes human/SRE outcome records and can
    promote only verified, validated knowledge.
    """
    def __init__(self, tracker):
        self.tracker = tracker

    def record_resolution(self, *, failure_case_id: str, resolution_code: str | None,
                          description: str | None, outcome_status: str,
                          verification_status: str, recorded_by: str | None = None,
                          action_id: str | None = None, evidence_ids: list[str] | None = None,
                          validated_by: str | None = None, validation_reason: str | None = None,
                          failure_signature: str | None = None, failure_class: str | None = None,
                          failure_code: str | None = None, diagnosis_code: str | None = None,
                          action: dict[str, Any] | None = None, expected_state: dict[str, Any] | None = None,
                          observed_state: dict[str, Any] | None = None, environment_context: dict[str, Any] | None = None):
        decision = evaluate_learning(
            resolution_code=resolution_code,
            outcome_status=outcome_status,
            verification_status=verification_status,
            human_validated=bool(validated_by),
        )
        if not self.tracker:
            raise RuntimeError("SRE Tracker is required to record operational resolution")
        resolution_id = self.tracker.record_resolution(
            failure_case_id=failure_case_id,
            resolution_code=resolution_code,
            description=description,
            action_id=action_id,
            outcome_status=outcome_status,
            verification_status=verification_status,
            recorded_by=recorded_by,
            evidence_ids=evidence_ids,
            validation_status=decision.status,
            validated_by=validated_by,
            validation_reason=validation_reason,
            action=action,
            expected_state=expected_state,
            observed_state=observed_state,
            environment_context=environment_context,
        )
        candidate_id = None
        if decision.status in {"CANDIDATE", "VALIDATED"} and failure_signature:
            candidate_id = self.tracker.upsert_learning_candidate(
                failure_signature=failure_signature,
                failure_class=failure_class,
                failure_code=failure_code,
                diagnosis_code=diagnosis_code,
                resolution_code=resolution_code,
                description=description,
                evidence_case_ids=[failure_case_id],
                verified_success_count=1,
                verified_failure_count=0,
                applicability=environment_context,
            )
            if decision.status == "VALIDATED":
                self.tracker.validate_learning_candidate(
                    candidate_id=candidate_id,
                    validated_by=validated_by,
                    validation_reason=validation_reason,
                )
        return {
            "resolution_id": str(resolution_id),
            "learning_status": decision.status,
            "learning_reason": decision.reason,
            "candidate_id": str(candidate_id) if candidate_id else None,
        }
