"""Deterministic operational learning policy.

This module never invents a resolution.  A resolution becomes reusable knowledge
only after an observed outcome, verification evidence, and explicit validation.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


FIRST_SEEN = "FIRST_SEEN"
RECURRING_UNKNOWN = "RECURRING_UNKNOWN"
RECURRING_UNRESOLVED = "RECURRING_UNRESOLVED"
RECURRING_RESOLVED = "RECURRING_RESOLVED"
KNOWN_ISSUE = "KNOWN_ISSUE"


@dataclass(frozen=True)
class LearningDecision:
    status: str
    reason: str


def evaluate_learning(*, resolution_code: str | None,
                      outcome_status: str | None,
                      verification_status: str | None,
                      human_validated: bool = False) -> LearningDecision:
    """Determine whether an outcome is learnable.

    Human validation is the final promotion gate.  A successful migration alone
    does not prove which action caused the recovery.
    """
    if not resolution_code:
        return LearningDecision("NOT_LEARNABLE", "The actual resolution/action was not recorded.")
    if str(verification_status or "").upper() != "PASSED":
        return LearningDecision("NOT_LEARNABLE", "The proposed resolution has no passed verification.")
    if str(outcome_status or "").upper() != "RESOLVED":
        return LearningDecision("NOT_LEARNABLE", "The case outcome is not verified as resolved.")
    if human_validated:
        return LearningDecision("VALIDATED", "The resolution is verified and explicitly human validated.")
    return LearningDecision("CANDIDATE", "The resolution is verified but has not been human validated.")


def recurrence_status(*, occurrence_count: int,
                      validated_solution_exists: bool,
                      resolved_previous_count: int,
                      unresolved_previous_count: int) -> str:
    if validated_solution_exists:
        return KNOWN_ISSUE
    if occurrence_count <= 1:
        return FIRST_SEEN
    if resolved_previous_count > 0 and unresolved_previous_count == 0:
        return RECURRING_RESOLVED
    if unresolved_previous_count > 0:
        return RECURRING_UNRESOLVED
    return RECURRING_UNKNOWN
