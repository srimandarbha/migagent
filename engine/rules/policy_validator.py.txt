"""Deterministic policy compiler and validator.

Validates that investigation policy YAML files conform to schema,
capability parameter contracts, fact ontology, and skill documentation.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml

from .facts import FACT_REGISTRY
from ..integrations.contracts import GLOBAL_CONTRACT_REGISTRY


class PolicyValidationError(ValueError):
    pass


def validate_policy_dict(policy: Dict[str, Any], skill_root: Optional[Path] = None) -> List[str]:
    """Validate a loaded policy dictionary and return a list of error messages."""
    errors: List[str] = []

    if not isinstance(policy, dict):
        return ["Policy root must be a YAML dictionary"]

    policy_id = policy.get("id")
    if not policy_id or not isinstance(policy_id, str):
        errors.append("Policy missing non-empty 'id' string")

    version = policy.get("version")
    if not version:
        errors.append(f"Policy '{policy_id}' missing 'version'")

    skill = policy.get("skill")
    if not skill or not isinstance(skill, str):
        errors.append(f"Policy '{policy_id}' missing non-empty 'skill' path")
    elif skill_root is not None:
        skill_path = Path(skill_root) / skill / "skill.md"
        if not skill_path.exists():
            errors.append(f"Policy '{policy_id}' references nonexistent skill file: {skill_path}")

    # Validate required evidence
    req_evidence = policy.get("required_evidence")
    if req_evidence is None or not isinstance(req_evidence, list):
        errors.append(f"Policy '{policy_id}' must define 'required_evidence' list")
    else:
        for idx, item in enumerate(req_evidence):
            _validate_evidence_requirement(f"required_evidence[{idx}]", item, errors)

    # Validate optional evidence
    for idx, item in enumerate(policy.get("optional_evidence", []) or []):
        _validate_evidence_requirement(f"optional_evidence[{idx}]", item, errors)

    # Validate adaptive investigation
    adaptive = policy.get("adaptive_investigation")
    if adaptive and isinstance(adaptive, dict) and adaptive.get("enabled"):
        rules = adaptive.get("rules", [])
        if not isinstance(rules, list):
            errors.append(f"Policy '{policy_id}' adaptive rules must be a list")
        else:
            for r_idx, rule in enumerate(rules):
                prefix = f"adaptive_investigation.rules[{r_idx}]"
                if not rule.get("id"):
                    errors.append(f"Policy '{policy_id}' {prefix} missing 'id'")
                when = rule.get("when", {})
                for f in when.get("all_facts", []):
                    if not FACT_REGISTRY.is_valid(f):
                        errors.append(f"Policy '{policy_id}' {prefix}.when.all_facts contains unknown fact '{f}'")
                for f in when.get("any_facts", []):
                    if not FACT_REGISTRY.is_valid(f):
                        errors.append(f"Policy '{policy_id}' {prefix}.when.any_facts contains unknown fact '{f}'")
                for f in rule.get("unless_any_facts", []):
                    if not FACT_REGISTRY.is_valid(f):
                        errors.append(f"Policy '{policy_id}' {prefix}.unless_any_facts contains unknown fact '{f}'")
                for inv_idx, inv in enumerate(rule.get("investigate", [])):
                    _validate_evidence_requirement(f"{prefix}.investigate[{inv_idx}]", inv, errors)

    # Validate hypotheses
    hypotheses = policy.get("hypotheses", [])
    if hypotheses is not None and not isinstance(hypotheses, list):
        errors.append(f"Policy '{policy_id}' 'hypotheses' must be a list")
    elif hypotheses:
        for h_idx, h in enumerate(hypotheses):
            prefix = f"hypotheses[{h_idx}]"
            if not h.get("id"):
                errors.append(f"Policy '{policy_id}' {prefix} missing 'id'")
            if not h.get("mechanism"):
                errors.append(f"Policy '{policy_id}' {prefix} missing 'mechanism'")
            score = h.get("score")
            if score is None or not isinstance(score, (int, float)) or not (0.0 <= score <= 1.0):
                errors.append(f"Policy '{policy_id}' {prefix} score must be float in [0.0, 1.0], got {score}")

            sup = h.get("supporting", {})
            for f in sup.get("all", []):
                if not FACT_REGISTRY.is_valid(f):
                    errors.append(f"Policy '{policy_id}' {prefix}.supporting.all contains unknown fact '{f}'")
            for f in sup.get("any", []):
                if not FACT_REGISTRY.is_valid(f):
                    errors.append(f"Policy '{policy_id}' {prefix}.supporting.any contains unknown fact '{f}'")

            contra = h.get("contradicting", {})
            for f in contra.get("all", []):
                if not FACT_REGISTRY.is_valid(f):
                    errors.append(f"Policy '{policy_id}' {prefix}.contradicting.all contains unknown fact '{f}'")
            for f in contra.get("any", []):
                if not FACT_REGISTRY.is_valid(f):
                    errors.append(f"Policy '{policy_id}' {prefix}.contradicting.any contains unknown fact '{f}'")

    # Validate decision readiness
    readiness = policy.get("decision_readiness", {})
    if not isinstance(readiness, dict):
        errors.append(f"Policy '{policy_id}' 'decision_readiness' must be a dict")
    else:
        for act, cfg in readiness.items():
            if not isinstance(cfg, dict):
                errors.append(f"Policy '{policy_id}' decision_readiness['{act}'] must be a dict")
                continue
            req_facts = cfg.get("required_facts", [])
            for f in req_facts:
                if not FACT_REGISTRY.is_valid(f):
                    errors.append(f"Policy '{policy_id}' decision_readiness['{act}'].required_facts contains unknown fact '{f}'")

    return errors


def _validate_evidence_requirement(loc: str, item: Any, errors: List[str]) -> None:
    if not isinstance(item, dict):
        errors.append(f"{loc}: requirement must be a dictionary")
        return
    cap = item.get("capability")
    if not cap:
        errors.append(f"{loc}: missing 'capability'")
        return
    params = item.get("parameters", {})
    is_valid, err = GLOBAL_CONTRACT_REGISTRY.validate(cap, params)
    if not is_valid:
        errors.append(f"{loc}: parameter contract violation for '{cap}': {err}")


def validate_policy_file(path: Path, skill_root: Optional[Path] = None) -> List[str]:
    try:
        content = path.read_text(encoding="utf-8")
        data = yaml.safe_load(content)
    except Exception as exc:
        return [f"Failed to load YAML from {path}: {exc}"]
    return validate_policy_dict(data, skill_root=skill_root)


def validate_all_policies(policy_dir: Path, skill_root: Optional[Path] = None) -> Dict[str, List[str]]:
    results: Dict[str, List[str]] = {}
    for p in sorted(Path(policy_dir).glob("*.yaml")):
        errs = validate_policy_file(p, skill_root=skill_root)
        if errs:
            results[p.name] = errs
    return results
