"""Dynamic Evidence-Weighted Hypothesis Scorer and Multi-Hypothesis Evaluator.

Implements Phase 2 (evidence-weighted scoring based on reliability, freshness,
and proportionate contradiction penalties) and Phase 3 (multi-hypothesis
topology: EXCLUSIVE, DOMINANT, CO_OCCURRING, AMBIGUOUS).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from ..contracts import Evidence, Hypothesis


class HypothesisTopology(str, Enum):
    EXCLUSIVE = "EXCLUSIVE"
    DOMINANT = "DOMINANT"
    SUPPORTED = "SUPPORTED"
    CO_OCCURRING = "CO_OCCURRING"
    AMBIGUOUS = "AMBIGUOUS"
    CONTRADICTED = "CONTRADICTED"
    UNTESTED = "UNTESTED"


@dataclass
class ScoredHypothesis:
    hypothesis: Hypothesis
    topology: HypothesisTopology
    base_prior: float
    dynamic_score: float
    supporting_quality: float
    freshness_factor: float
    contradiction_penalty: float
    co_occurring_with: List[str] = field(default_factory=list)


def calculate_freshness_factor(freshness_seconds: Optional[float]) -> float:
    """Calculates temporal decay factor: 1.0 within 5m, decaying to 0.60 after 2h."""
    if freshness_seconds is None:
        return 0.50  # Conservative default for unmeasured freshness
    if freshness_seconds <= 300.0:
        return 1.0
    if freshness_seconds >= 7200.0:
        return 0.60
    # Linear decay between 300s and 7200s
    return round(1.0 - 0.40 * ((freshness_seconds - 300.0) / (7200.0 - 300.0)), 3)


def evaluate_hypothesis_score(
    item: Dict[str, Any],
    evidence_by_fact: Dict[str, List[Evidence]],
    classification: str,
) -> ScoredHypothesis:
    """Evaluates dynamic evidence weight and contradiction penalties for a hypothesis."""
    hid = item.get("id", "")
    code = item.get("mechanism", item.get("code", classification))
    desc = item.get("description", "")
    base_prior = float(item.get("score", 0.85))

    sup_cfg = item.get("supporting", {})
    contra_cfg = item.get("contradicting", {})

    sup_all = set(sup_cfg.get("all", []))
    sup_any = set(sup_cfg.get("any", []))
    contra_all = set(contra_cfg.get("all", []))
    contra_any = set(contra_cfg.get("any", []))

    observed_facts = set(evidence_by_fact.keys())

    # 1. Contradiction evaluation
    contra_matched: Set[str] = set()
    if contra_any and (contra_any & observed_facts):
        contra_matched |= (contra_any & observed_facts)
    if contra_all and contra_all.issubset(observed_facts):
        contra_matched |= contra_all

    contradicting_refs: List[str] = []
    contra_quality_scores: List[float] = []
    for f in contra_matched:
        for ev in evidence_by_fact.get(f, []):
            contradicting_refs.append(ev.id)
            ff = calculate_freshness_factor(ev.freshness_seconds)
            contra_quality_scores.append(ev.reliability * ff)

    avg_contra_quality = (sum(contra_quality_scores) / len(contra_quality_scores)) if contra_quality_scores else 0.0
    contradiction_penalty = round(avg_contra_quality * 0.45, 3) if contra_matched else 0.0

    # 2. Supporting evaluation
    sup_matched: Set[str] = set()
    is_supported = True

    if sup_all:
        if sup_all.issubset(observed_facts):
            sup_matched |= sup_all
        else:
            is_supported = False

    if sup_any:
        matched_any = sup_any & observed_facts
        if matched_any:
            sup_matched |= matched_any
        else:
            is_supported = False

    if not sup_all and not sup_any:
        is_supported = False

    supporting_refs: List[str] = []
    sup_quality_scores: List[float] = []
    freshness_factors: List[float] = []
    for f in sup_matched:
        for ev in evidence_by_fact.get(f, []):
            supporting_refs.append(ev.id)
            ff = calculate_freshness_factor(ev.freshness_seconds)
            freshness_factors.append(ff)
            sup_quality_scores.append(ev.reliability * ff)

    avg_sup_quality = (sum(sup_quality_scores) / len(sup_quality_scores)) if sup_quality_scores else 0.85
    avg_freshness = (sum(freshness_factors) / len(freshness_factors)) if freshness_factors else 0.50

    # 3. Dynamic Score Calculation
    if contra_matched and avg_contra_quality >= 0.40:
        # Contradiction drops score and forces status CONTRADICTED
        status_str = "CONTRADICTED"
        topology = HypothesisTopology.CONTRADICTED
        dynamic_score = max(0.05, round(base_prior * (1.0 - contradiction_penalty) - 0.40, 2))
    elif is_supported:
        status_str = "SUPPORTED"
        topology = HypothesisTopology.SUPPORTED
        raw_score = base_prior * avg_sup_quality - contradiction_penalty
        dynamic_score = max(0.40, min(1.0, round(raw_score, 2)))
    else:
        status_str = "UNTESTED"
        topology = HypothesisTopology.UNTESTED
        dynamic_score = round(base_prior * 0.50, 2)

    hyp = Hypothesis(
        id=hid,
        code=code,
        description=desc,
        score=dynamic_score,
        status=status_str,
        supporting=supporting_refs,
        contradicting=contradicting_refs,
    )

    return ScoredHypothesis(
        hypothesis=hyp,
        topology=topology,
        base_prior=base_prior,
        dynamic_score=dynamic_score,
        supporting_quality=avg_sup_quality,
        freshness_factor=avg_freshness,
        contradiction_penalty=contradiction_penalty,
    )


def resolve_multi_hypothesis_topology(
    scored_hypotheses: List[ScoredHypothesis],
) -> Tuple[List[ScoredHypothesis], Dict[str, Any]]:
    """Analyzes competing vs co-occurring hypotheses across the hypothesis set.

    Determines if a supported hypothesis is EXCLUSIVE, DOMINANT, CO_OCCURRING,
    or AMBIGUOUS.
    """
    supported = [sh for sh in scored_hypotheses if sh.hypothesis.status == "SUPPORTED"]

    landscape: Dict[str, Any] = {
        "total_hypotheses": len(scored_hypotheses),
        "supported_count": len(supported),
        "contradicted_count": sum(1 for sh in scored_hypotheses if sh.hypothesis.status == "CONTRADICTED"),
        "untested_count": sum(1 for sh in scored_hypotheses if sh.hypothesis.status == "UNTESTED"),
        "topology_status": "NONE",
        "differentiator_needed": False,
    }

    if not supported:
        landscape["topology_status"] = "NO_SUPPORTED_HYPOTHESIS"
        return scored_hypotheses, landscape

    # Sort supported by dynamic score descending
    supported_sorted = sorted(supported, key=lambda sh: sh.dynamic_score, reverse=True)
    top = supported_sorted[0]

    if len(supported_sorted) == 1:
        # Sole supported hypothesis
        top.topology = HypothesisTopology.EXCLUSIVE
        top.hypothesis.status = "SUPPORTED"
        landscape["topology_status"] = "EXCLUSIVE"
        landscape["leading_hypothesis"] = top.hypothesis.id
        return scored_hypotheses, landscape

    # Multiple supported hypotheses: check score margin
    runner_up = supported_sorted[1]
    margin = round(top.dynamic_score - runner_up.dynamic_score, 2)

    if margin >= 0.15:
        top.topology = HypothesisTopology.DOMINANT
        runner_up.topology = HypothesisTopology.SUPPORTED
        landscape["topology_status"] = "DOMINANT"
        landscape["leading_hypothesis"] = top.hypothesis.id
        landscape["runner_up_hypothesis"] = runner_up.hypothesis.id
        landscape["margin"] = margin
    elif margin < 0.08:
        # Competing hypotheses with almost identical scores
        top.topology = HypothesisTopology.AMBIGUOUS
        runner_up.topology = HypothesisTopology.AMBIGUOUS
        landscape["topology_status"] = "AMBIGUOUS"
        landscape["competing_hypotheses"] = [sh.hypothesis.id for sh in supported_sorted[:2]]
        landscape["differentiator_needed"] = True
    else:
        # Co-occurring hypotheses
        top.topology = HypothesisTopology.DOMINANT
        for other in supported_sorted[1:]:
            other.topology = HypothesisTopology.CO_OCCURRING
            top.co_occurring_with.append(other.hypothesis.id)
        landscape["topology_status"] = "CO_OCCURRING"
        landscape["leading_hypothesis"] = top.hypothesis.id
        landscape["co_occurring_hypotheses"] = [other.hypothesis.id for other in supported_sorted[1:]]

    return scored_hypotheses, landscape
