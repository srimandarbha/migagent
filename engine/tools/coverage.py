"""Capability coverage checks for investigation policies.

Coverage is deliberately capability-level, not backend-level.  A capability being
registered only means the agent can address that evidence requirement; runtime
availability is still reported separately by InvestigationTool.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

from ..rules.evidence_policy import load_policy

COVERAGE_READY = "READY"
COVERAGE_PARTIAL = "PARTIAL"
COVERAGE_BLOCKED = "BLOCKED"
COVERAGE_UNKNOWN = "UNKNOWN"

NOT_REGISTERED = "NOT_REGISTERED"
REGISTERED = "REGISTERED"
REGISTRY_UNINSPECTABLE = "REGISTRY_UNINSPECTABLE"


def _capability_id(item: Any) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        return str(item.get("capability", ""))
    return ""


def policy_capabilities(policy: dict[str, Any]) -> dict[str, list[str]]:
    required = [_capability_id(x) for x in policy.get("required_evidence", [])]
    optional = [_capability_id(x) for x in policy.get("optional_evidence", [])]
    contextual: list[str] = []
    adaptive: list[str] = []
    for rule in policy.get("adaptive_investigation", {}).get("rules", []) or []:
        adaptive.extend(_capability_id(x) for x in rule.get("investigate", []) or [])
    return {
        "required": sorted({x for x in required if x}),
        "optional": sorted({x for x in optional if x}),
        "contextual": sorted({x for x in contextual if x}),
        "adaptive": sorted({x for x in adaptive if x}),
    }


def discover_registered_capabilities(registry: Any) -> tuple[set[str], bool]:
    """Return registered capabilities and whether discovery was reliable."""
    if registry is None:
        return set(), False
    for method_name in ("list_capabilities", "available_capabilities"):
        method = getattr(registry, method_name, None)
        if callable(method):
            try:
                return {str(x) for x in method()}, True
            except Exception:
                return set(), False
    for attr in ("capabilities", "_capabilities"):
        value = getattr(registry, attr, None)
        if isinstance(value, dict):
            return {str(x) for x in value}, True
        if isinstance(value, (set, list, tuple)):
            return {str(x) for x in value}, True
    return set(), False


@dataclass
class CapabilityCoverage:
    issue_tag: str
    policy: str
    registered_capabilities: list[str]
    required: list[str]
    optional: list[str]
    contextual: list[str]
    knowledge: list[str]
    missing_required: list[str]
    missing_optional: list[str]
    missing_contextual: list[str]
    missing_knowledge: list[str]
    status: str
    required_coverage: str
    registry_inspectable: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def calculate_coverage(issue_tag: str, registry: Any) -> CapabilityCoverage:
    policy = load_policy(issue_tag)
    caps = policy_capabilities(policy)
    registered, inspectable = discover_registered_capabilities(registry)
    if not inspectable:
        return CapabilityCoverage(
            issue_tag=issue_tag,
            policy=str(policy.get("id", issue_tag)),
            registered_capabilities=[],
            required=caps["required"],
            optional=caps["optional"],
            contextual=caps["contextual"],
            knowledge=[],
            missing_required=[],
            missing_optional=[],
            missing_contextual=[],
            missing_knowledge=[],
            status=COVERAGE_UNKNOWN,
            required_coverage=COVERAGE_UNKNOWN,
            registry_inspectable=False,
        )

    missing_required = sorted(set(caps["required"]) - registered)
    missing_optional = sorted(set(caps["optional"]) - registered)
    missing_contextual = sorted(set(caps["contextual"]) - registered)
    adaptive_missing = sorted(set(caps["adaptive"]) - registered)
    missing_optional = sorted(set(missing_optional) | set(adaptive_missing))

    status = COVERAGE_BLOCKED if missing_required else (
        COVERAGE_PARTIAL if missing_optional or missing_contextual else COVERAGE_READY
    )
    return CapabilityCoverage(
        issue_tag=issue_tag,
        policy=str(policy.get("id", issue_tag)),
        registered_capabilities=sorted(registered),
        required=caps["required"],
        optional=caps["optional"],
        contextual=caps["contextual"],
        knowledge=[],
        missing_required=missing_required,
        missing_optional=missing_optional,
        missing_contextual=missing_contextual,
        missing_knowledge=[],
        status=status,
        required_coverage=COVERAGE_BLOCKED if missing_required else COVERAGE_READY,
        registry_inspectable=True,
    )
