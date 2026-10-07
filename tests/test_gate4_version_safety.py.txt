"""Gate 4: Version-Aware Knowledge Applicability & Deterministic Safety Boundary Tests.

Verifies:
  1. SUPPORTED (exact or range, e.g. MTV 2.8+ or 2.10-2.12 in MTV 2.11 environment) -> ELIGIBLE
  2. NEEDS_VALIDATION (applicability unknown) -> eligible for LLM judge
  3. VERSION_MISMATCH (MTV 2.12+ in MTV 2.11 environment) -> deterministic hard reject REJECT_VERSION
  4. ERROR_MISMATCH (mismatched failure code) -> deterministic hard reject REJECT_ERROR_MISMATCH
  5. Deterministic firewall: LLM judge CANNOT override hard rejects even with maximum confidence.
"""
from __future__ import annotations

import os
import pytest
from pathlib import Path

from engine.knowledge_compatibility import (
    evaluate,
    evaluate_candidate,
    judge_eligibility,
    apply_judge_verdict,
    resolve_applicable,
)
from persistence.repository import SRETrackerRepository
from persistence.knowledge import PostgresVectorKnowledgeRepository

ROOT = Path(__file__).resolve().parents[1]


def get_current_env():
    return {
        "cluster_id": "ocv-prod-01",
        "ocp_version": "4.19.23",
        "ocv_version": "4.19.23",
        "mtv_version": "2.11.0",
        "source_provider": "vmware",
        "source_provider_version": "8.0",
    }


def test_gate4_supported_knowledge_in_range():
    env = get_current_env()
    doc = {
        "id": "RH-DOC-MTV-CBT-RETRY-LIMIT",
        "title": "Troubleshooting VMware CBT snapshot retry limit",
        "failure_code": "vmware.cbt.retry_limit",
        "applicability": {
            "mtv": {"min": "2.10", "max": "2.12", "versions": ["2.10", "2.11", "2.12"]},
            "ocv": {"versions": ["4.19", "4.20"]},
        },
    }
    failure = {"failure_code": "vmware.cbt.retry_limit"}
    decision = evaluate_candidate(env, failure, doc)

    assert decision["version_applicability"]["status"] in {"SUPPORTED_EXACT", "SUPPORTED_RANGE"}
    assert decision["error_match"]["status"] == "MATCH"
    assert decision["recommendation_status"] == "ELIGIBLE"
    assert decision["eligible"] is True


def test_gate4_unknown_version_needs_validation():
    env = get_current_env()
    doc = {
        "id": "RH-DOC-GENERIC-UNSPECIFIED-VERSION",
        "title": "General MTV Overview",
        "failure_code": "vmware.cbt.retry_limit",
        "applicability": {},  # Unspecified applicability
    }
    failure = {"failure_code": "vmware.cbt.retry_limit"}
    decision = evaluate_candidate(env, failure, doc)

    assert decision["version_applicability"]["status"] == "VERSION_UNKNOWN"
    assert decision["error_match"]["status"] == "MATCH"
    assert decision["recommendation_status"] == "NEEDS_VALIDATION"
    assert decision["eligible"] is False

    # Verify it IS eligible for the LLM judge
    doc["compatibility"] = decision
    judge_candidates = judge_eligibility(env, failure, [doc])
    assert len(judge_candidates) == 1
    assert judge_candidates[0]["id"] == doc["id"]


def test_gate4_version_mismatch_is_hard_rejected():
    env = get_current_env()  # mtv_version = 2.11.0
    doc = {
        "id": "RH-DOC-MTV-VERSION-2-12-ONLY",
        "title": "MTV 2.12 Feature Guide",
        "failure_code": "vmware.cbt.retry_limit",
        "applicability": {
            "mtv": {"min": "2.12", "max": "2.13", "versions": ["2.12", "2.13"]},
            "ocv": {"versions": ["4.20", "4.21"]},
        },
    }
    failure = {"failure_code": "vmware.cbt.retry_limit"}
    decision = evaluate_candidate(env, failure, doc)

    assert decision["version_applicability"]["status"] == "VERSION_MISMATCH"
    assert decision["recommendation_status"] == "REJECT_VERSION"
    assert decision["eligible"] is False

    # CRITICAL: Verify hard reject is BLOCKED from LLM judge
    doc["compatibility"] = decision
    judge_candidates = judge_eligibility(env, failure, [doc])
    assert len(judge_candidates) == 0


def test_gate4_error_mismatch_is_hard_rejected():
    env = get_current_env()
    doc = {
        "id": "RH-SOL-STORAGE-CSI-PROVISIONING-TIMEOUT",
        "title": "CSI Provisioning Timeout",
        "failure_code": "storage.csi.provisioning_timeout",
        "applicability": {
            "mtv": {"min": "2.8", "max": "2.12"},
            "ocv": {"versions": ["4.19", "4.20"]},
        },
    }
    failure = {"failure_code": "vmware.cbt.retry_limit"}
    decision = evaluate_candidate(env, failure, doc)

    assert decision["version_applicability"]["status"] == "SUPPORTED_RANGE"
    assert decision["error_match"]["status"] == "NO_MATCH"
    assert decision["recommendation_status"] == "REJECT_ERROR_MISMATCH"
    assert decision["eligible"] is False

    # CRITICAL: Verify error mismatch is BLOCKED from LLM judge
    doc["compatibility"] = decision
    judge_candidates = judge_eligibility(env, failure, [doc])
    assert len(judge_candidates) == 0


def test_gate4_adversarial_judge_verdict_cannot_override_hard_rejects():
    """Fail-closed safety: LLM judge cannot promote VERSION_MISMATCH or REJECT_ERROR_MISMATCH."""
    env = get_current_env()
    failure = {"failure_code": "vmware.cbt.retry_limit"}

    doc_version_mismatch = {
        "id": "RH-DOC-MTV-VERSION-2-12-ONLY",
        "failure_code": "vmware.cbt.retry_limit",
        "applicability": {"mtv": {"min": "2.12"}},
    }
    doc_version_mismatch["compatibility"] = evaluate_candidate(env, failure, doc_version_mismatch)

    doc_error_mismatch = {
        "id": "RH-SOL-STORAGE-CSI-PROVISIONING-TIMEOUT",
        "failure_code": "storage.csi.provisioning_timeout",
        "applicability": {"mtv": {"min": "2.8"}},
    }
    doc_error_mismatch["compatibility"] = evaluate_candidate(env, failure, doc_error_mismatch)

    # Adversarial hallucinated verdict from an LLM
    adversarial_verdict = {
        "status": "JUDGED",
        "verdict": "LIKELY_APPLICABLE",
        "confidence": 1.0,
        "basis": ["Hallucinated assertion that version 2.12 applies to 2.11"],
    }

    results = apply_judge_verdict([doc_version_mismatch, doc_error_mismatch], adversarial_verdict)

    # Verify both remain hard rejects
    assert results[0]["compatibility"]["recommendation_status"] == "REJECT_VERSION"
    assert results[1]["compatibility"]["recommendation_status"] == "REJECT_ERROR_MISMATCH"


def test_gate4_live_database_documents_respect_boundaries():
    """Verify live documents from PostgreSQL match expected compatibility when queried."""
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:5432/migration_agent")
    try:
        repo = SRETrackerRepository(dsn)
        kr = PostgresVectorKnowledgeRepository(repo, embed=None)
        res = kr.search("CBT retry limit", top_k=20)
    except Exception:
        pytest.skip("PostgreSQL database not available")

    docs = res.get("documents", res) if isinstance(res, dict) else res
    assert len(docs) > 0

    env = get_current_env()
    resolved = resolve_applicable(env, docs, failure={"failure_code": "vmware.cbt.retry_limit"})

    # Check categorized partitions
    assert "supported" in resolved
    assert "background" in resolved
    assert "excluded" in resolved

    # Verify excluded contains any version mismatch or error mismatch documents
    for d in resolved["excluded"]:
        assert d["compatibility"]["recommendation_status"] in {"REJECT_VERSION", "REJECT_ERROR_MISMATCH"}
