"""Failure recurrence correlation and deterministic signature generation."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from ..rules.learning import recurrence_status


_VOLATILE = re.compile(r"\b(?:[0-9a-f]{8,}|mig[-_][\w-]+|vm[-_][\w-]+|evt[-_][\w-]+)\b", re.I)


def _migration_type(event: dict[str, Any]) -> str | None:
    env = event.get("environment") or {}
    migration = env.get("migration") or {}
    return event.get("migration_type") or (event.get("migration") or {}).get("type") or migration.get("type")


def build_base_signature(event: dict[str, Any], classification: str | None) -> str:
    """Build a stable pre-evidence signature.

    Message text is normalized but is deliberately not the primary key.  Exact
    failure code/classification plus environment dimensions are the first-level
    recurrence identity.
    """
    from ..environment import from_event
    env = from_event(event)
    payload = {
        "classification": classification or "UNKNOWN",
        "failure_code": str(event.get("failure_code") or event.get("error_code") or "").strip().lower(),
        "phase": str(event.get("phase") or "").strip().lower(),
        "migration_type": str(env.migration_type or "").strip().lower(),
        "source_provider": str(env.source_provider or "").strip().lower(),
        "mtv_version": str(env.mtv_version or "").strip(),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_evidence_signature(classification: str | None, mechanism: str | None, facts: list[str]) -> str:
    payload = {
        "classification": classification or "UNKNOWN",
        "mechanism": mechanism or "UNKNOWN",
        "facts": sorted(set(str(x) for x in facts)),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def correlate(tracker, *, signature: str, failure_case_id: str | None = None,
              classification: str | None = None, cluster_id: str | None = None) -> dict[str, Any]:
    """Correlate a current failure against historical operational cases.

    Missing Tracker data is a normal NO_DATA result, never an exception that
    converts the failure into an infrastructure error.
    """
    if tracker is None:
        return {
            "status": "DISABLED",
            "recurrence_status": "FIRST_SEEN",
            "occurrence_count": 1,
            "previous_case_ids": [],
            "validated_solution_refs": [],
            "historical_actions": [],
            "previous_outcomes": [],
        }

    try:
        if hasattr(tracker, "correlate_failure_signature"):
            return tracker.correlate_failure_signature(
                signature=signature,
                failure_case_id=failure_case_id,
                classification=classification,
                cluster_id=cluster_id,
            )

        rows = tracker.search_failure_history(
            failure_class=classification,
            cluster_id=cluster_id,
            limit=100,
        )
        previous = [r for r in rows if str(r.get("failure_case_id")) != str(failure_case_id)]
        return {
            "status": "SUCCESS" if previous else "NO_DATA",
            "recurrence_status": recurrence_status(
                occurrence_count=len(previous) + 1,
                validated_solution_exists=any(r.get("validated_solution") for r in previous),
                resolved_previous_count=sum(1 for r in previous if str(r.get("status", "")).upper() in {"RESOLVED", "VERIFIED"}),
                unresolved_previous_count=sum(1 for r in previous if str(r.get("status", "")).upper() not in {"RESOLVED", "VERIFIED"}),
            ),
            "occurrence_count": len(previous) + 1,
            "previous_case_ids": [str(r.get("failure_case_id")) for r in previous],
            "validated_solution_refs": [],
            "historical_actions": [],
            "previous_outcomes": previous,
        }
    except Exception as exc:
        return {
            "status": "ERROR",
            "recurrence_status": "FIRST_SEEN",
            "occurrence_count": 1,
            "previous_case_ids": [],
            "validated_solution_refs": [],
            "historical_actions": [],
            "previous_outcomes": [],
            "error": str(exc),
        }
