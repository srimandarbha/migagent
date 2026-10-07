from __future__ import annotations

"""Deterministic knowledge applicability and error-to-solution checks.

RAG/LLM identifies candidate knowledge. This module decides whether a candidate
is version-applicable and whether its structured failure signature matches the
current failure. It never infers applicability from semantic similarity alone.
"""

from typing import Any
from packaging.version import InvalidVersion, Version

APPLICABILITY = {
    "SUPPORTED_EXACT", "SUPPORTED_RANGE", "VERSION_UNKNOWN", "VERSION_MISMATCH",
    "HISTORICAL", "RETIRED", "CONFLICTING", "UNVERIFIED",
}
MATCH_STATUS = {"MATCH", "NO_MATCH", "UNKNOWN"}


def _version(value: str | None) -> Version | None:
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    # Red Hat metadata sometimes uses a leading v. Strip only that decoration.
    if raw.lower().startswith("v"):
        raw = raw[1:]
    try:
        return Version(raw)
    except InvalidVersion:
        return None


def version_tuple(value: str | None):
    """Backward-compatible helper returning a normalized 3-part tuple."""
    v = _version(value)
    if v is None:
        return None
    release = list(v.release[:3])
    release += [0] * (3 - len(release))
    return tuple(release)


def _version_matches(actual: str | None, spec: dict[str, Any]) -> bool | None:
    """Evaluate exact/list/range version constraints.

    Missing or malformed actual versions return None. An explicit empty spec is
    also unknown, never implicitly compatible.
    """
    actual_v = _version(actual)
    if actual_v is None:
        return None

    versions = spec.get("versions") or []
    parsed_versions = [_version(v) for v in versions]
    parsed_versions = [v for v in parsed_versions if v is not None]
    if versions and not parsed_versions:
        return None
    if parsed_versions:
        for raw, candidate in zip(versions, parsed_versions):
            # A two-part constraint such as 4.19 means the supported product
            # line 4.19.x. A three-part constraint is an exact patch release.
            raw_parts = str(raw).lstrip('vV').split('.')
            if len(raw_parts) <= 2 and actual_v.release[:len(raw_parts)] == candidate.release[:len(raw_parts)]:
                return True
            if len(raw_parts) >= 3 and actual_v == candidate:
                return True
        return False

    minimum = _version(spec.get("min"))
    maximum = _version(spec.get("max"))
    if spec.get("min") and minimum is None:
        return None
    if spec.get("max") and maximum is None:
        return None
    if minimum is None and maximum is None:
        return None
    if minimum is not None and actual_v < minimum:
        return False
    if maximum is not None and actual_v > maximum:
        return False
    return True


def _normalize_code(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", ".")


def _candidate_codes(document: dict[str, Any]) -> set[str]:
    values: list[Any] = []
    for key in ("failure_code", "failure_codes", "error_code", "error_codes", "failure_signature", "error_signature", "error_signatures"):
        value = document.get(key)
        if isinstance(value, list):
            values.extend(value)
        elif value:
            values.append(value)
    metadata = document.get("metadata") or {}
    for key in ("failure_code", "failure_codes", "error_code", "error_codes", "failure_signature", "error_signatures"):
        value = metadata.get(key)
        if isinstance(value, list):
            values.extend(value)
        elif value:
            values.append(value)
    return {_normalize_code(v) for v in values if _normalize_code(v)}


def _failure_codes(failure: dict[str, Any]) -> set[str]:
    values: list[Any] = []
    for key in ("failure_code", "failure_codes", "error_code", "error_codes", "classification", "failure_class"):
        value = failure.get(key)
        if isinstance(value, list):
            values.extend(value)
        elif value:
            values.append(value)
    return {_normalize_code(v) for v in values if _normalize_code(v)}


def evaluate_error_solution_match(failure: dict[str, Any] | None, document: dict[str, Any]) -> dict[str, Any]:
    """Deterministically assess a structured failure-to-knowledge match.

    Exact structured codes are authoritative. If the candidate contains no
    structured error/failure codes, the result is UNKNOWN rather than MATCH.
    Semantic RAG relevance remains useful, but it is not a deterministic match.
    """
    failure = failure or {}
    actual = _failure_codes(failure)
    candidate = _candidate_codes(document)
    if not actual or not candidate:
        return {
            "status": "UNKNOWN",
            "reason": "Structured failure/error codes are missing on one or both sides.",
            "failure_codes": sorted(actual),
            "candidate_codes": sorted(candidate),
        }
    overlap = sorted(actual & candidate)
    if overlap:
        return {
            "status": "MATCH",
            "reason": "A structured failure/error code matches the knowledge candidate.",
            "matched_codes": overlap,
            "failure_codes": sorted(actual),
            "candidate_codes": sorted(candidate),
        }
    return {
        "status": "NO_MATCH",
        "reason": "No structured failure/error code matches the knowledge candidate.",
        "failure_codes": sorted(actual),
        "candidate_codes": sorted(candidate),
    }


def _version_checks(environment: dict[str, Any], meta: dict[str, Any]) -> list[dict[str, Any]]:
    checks = []
    for field, actual_key in (
        ("ocv", "ocv_version"),
        ("ocp", "ocp_version"),
        ("mtv", "mtv_version"),
        ("vmware", "source_provider_version"),
    ):
        spec = meta.get(field, {}) or {}
        if not spec:
            continue
        actual = environment.get(actual_key)
        result = _version_matches(actual, spec)
        checks.append({"product": field, "actual": actual, "match": result, "supported": spec})
    return checks


def evaluate(environment: dict[str, Any], document: dict[str, Any]) -> dict[str, Any]:
    """Evaluate deterministic environment applicability for one knowledge item."""
    meta = document.get("applicability", {}) or {}
    status = str(meta.get("status", "")).upper()
    if status in {"RETIRED", "CONFLICTING"}:
        return {"status": status, "reason": f"Knowledge source is marked {status.lower()}.", "checks": []}

    checks = _version_checks(environment, meta)
    known = [r for r in checks if r["match"] is not None]
    if not known:
        return {
            "status": "VERSION_UNKNOWN",
            "reason": "The source does not contain structured applicability for the observed environment.",
            "checks": checks,
        }
    if any(r["match"] is False for r in known):
        return {
            "status": "VERSION_MISMATCH",
            "reason": "At least one documented product/version constraint does not match the current environment.",
            "checks": checks,
        }

    exact = all(bool(r["supported"].get("versions")) for r in known)
    return {
        "status": "SUPPORTED_EXACT" if exact else "SUPPORTED_RANGE",
        "reason": "All known structured version constraints match the current environment.",
        "checks": checks,
    }


def evaluate_candidate(
    environment: dict[str, Any],
    failure: dict[str, Any] | None,
    document: dict[str, Any],
) -> dict[str, Any]:
    """Combine deterministic error matching and deterministic version checks."""
    error_match = evaluate_error_solution_match(failure, document)
    version = evaluate(environment, document)

    failure_codes = _failure_codes(failure or {})
    candidate_codes = _candidate_codes(document)
    if error_match["status"] == "NO_MATCH":
        recommendation_status = "REJECT_ERROR_MISMATCH"
    elif version["status"] in {"VERSION_MISMATCH", "RETIRED", "CONFLICTING"}:
        recommendation_status = "REJECT_VERSION"
    elif version["status"] == "VERSION_UNKNOWN":
        recommendation_status = "NEEDS_VALIDATION"
    elif error_match["status"] == "UNKNOWN" and (failure_codes or candidate_codes):
        # A structured current failure exists, but this candidate has no
        # deterministic signature to prove that it addresses that failure.
        recommendation_status = "NEEDS_VALIDATION"
    else:
        # If no structured failure code is available yet, version-valid
        # knowledge may still be retained as contextual knowledge. The later
        # evidence/diagnosis stages decide whether it is usable for the case.
        recommendation_status = "ELIGIBLE"

    return {
        "error_match": error_match,
        "version_applicability": version,
        "recommendation_status": recommendation_status,
        "eligible": recommendation_status == "ELIGIBLE",
    }


def annotate(
    environment: dict[str, Any],
    documents: list[dict[str, Any]],
    failure: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    result = []
    for doc in documents:
        d = dict(doc)
        app = evaluate(environment, d)
        d["applicability"] = {**(d.get("applicability") or {}), **app}
        d["compatibility"] = evaluate_candidate(environment, failure, d)
        result.append(d)
    return result


def resolve_applicable(
    environment: dict[str, Any],
    documents: list[dict[str, Any]],
    failure: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    annotated = annotate(environment, documents, failure)
    supported = [
        d for d in annotated
        if d["compatibility"]["recommendation_status"] == "ELIGIBLE"
    ]
    background = [
        d for d in annotated
        if d["compatibility"]["recommendation_status"] == "NEEDS_VALIDATION"
        or d["applicability"]["status"] in {"VERSION_UNKNOWN", "HISTORICAL"}
    ]
    excluded = [
        d for d in annotated
        if d["compatibility"]["recommendation_status"] in {"REJECT_VERSION", "REJECT_ERROR_MISMATCH"}
        or d["applicability"]["status"] in {"VERSION_MISMATCH", "RETIRED", "CONFLICTING"}
    ]
    return {"supported": supported, "background": background, "excluded": excluded, "all": annotated}


def judge_eligibility(environment: dict[str, Any], failure: dict[str, Any], candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return only candidates that are ambiguous, not explicitly rejected.

    LLM judging is permitted only for UNKNOWN applicability/error matching. Explicit
    version mismatch, retired/conflicting knowledge, and explicit error mismatch remain hard rejects.
    """
    result = []
    for candidate in candidates:
        decision = candidate.get("compatibility") or evaluate_candidate(environment, failure, candidate)
        status = decision.get("recommendation_status")
        if status == "NEEDS_VALIDATION":
            result.append(candidate)
    return result


def apply_judge_verdict(candidates: list[dict[str, Any]], verdict: dict[str, Any]) -> list[dict[str, Any]]:
    """Apply an LLM advisory without allowing it to override deterministic hard rejects."""
    if verdict.get("status") != "JUDGED":
        return candidates
    judged = verdict.get("verdict")
    for candidate in candidates:
        decision = candidate.get("compatibility", {})
        # Only ambiguous candidates can be promoted.
        if decision.get("recommendation_status") != "NEEDS_VALIDATION":
            continue
        if decision.get("version_applicability", {}).get("status") in {"VERSION_MISMATCH", "RETIRED", "CONFLICTING"}:
            continue
        if decision.get("error_match", {}).get("status") == "NO_MATCH":
            continue
        if judged == "LIKELY_APPLICABLE":
            decision["recommendation_status"] = "CANDIDATE_APPLICABLE"
        elif judged == "LIKELY_NOT_APPLICABLE":
            decision["recommendation_status"] = "CANDIDATE_NOT_APPLICABLE"
        else:
            decision["recommendation_status"] = "NEEDS_VALIDATION"
    return candidates
