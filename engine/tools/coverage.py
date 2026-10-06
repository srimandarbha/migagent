"""Capability coverage checks for investigation policies.

Validates coverage at both the capability level and the parameter contract level
(domain, signal, required parameters) against registry capabilities.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Set, Tuple

from ..rules.evidence_policy import load_policy
from ..integrations.contracts import GLOBAL_CONTRACT_REGISTRY

COVERAGE_READY = "READY"
COVERAGE_PARTIAL = "PARTIAL"
COVERAGE_BLOCKED = "BLOCKED"
COVERAGE_UNKNOWN = "UNKNOWN"

NOT_REGISTERED = "NOT_REGISTERED"
REGISTERED = "REGISTERED"
INVALID_SCHEMA = "INVALID_SCHEMA"
REGISTRY_UNINSPECTABLE = "REGISTRY_UNINSPECTABLE"


def _capability_id(item: Any) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        return str(item.get("capability", ""))
    return ""


def _extract_contract_item(req: Any) -> Dict[str, Any]:
    if isinstance(req, str):
        return {"capability": req, "domain": None, "signal": None, "parameters": {}}
    params = dict(req.get("parameters", {})) if isinstance(req.get("parameters"), dict) else {}
    return {
        "capability": str(req.get("capability", "")),
        "domain": params.get("domain"),
        "signal": params.get("signal"),
        "parameters": params,
        "purpose": req.get("purpose", ""),
    }


def policy_contracts(policy: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    required = [_extract_contract_item(x) for x in policy.get("required_evidence", [])]
    optional = [_extract_contract_item(x) for x in policy.get("optional_evidence", [])]
    contextual: list[dict[str, Any]] = []
    adaptive: list[dict[str, Any]] = []
    for rule in policy.get("adaptive_investigation", {}).get("rules", []) or []:
        adaptive.extend(_extract_contract_item(x) for x in rule.get("investigate", []) or [])
    return {
        "required": required,
        "optional": optional,
        "contextual": contextual,
        "adaptive": adaptive,
    }


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
    contract_details: list[dict[str, Any]] = field(default_factory=list)
    missing_required_contracts: list[dict[str, Any]] = field(default_factory=list)
    missing_optional_contracts: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def calculate_coverage(issue_tag: str, registry: Any) -> CapabilityCoverage:
    policy = load_policy(issue_tag)
    caps = policy_capabilities(policy)
    contracts = policy_contracts(policy)
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
            contract_details=[],
            missing_required_contracts=[],
            missing_optional_contracts=[],
        )

    supports_contract_fn = getattr(registry, "supports_contract", None)

    contract_details: list[dict[str, Any]] = []
    missing_required_contracts: list[dict[str, Any]] = []
    missing_required_caps: set[str] = set()

    for item in contracts["required"]:
        cid = item["capability"]
        domain = item.get("domain")
        signal = item.get("signal")
        params = item.get("parameters", {})

        is_valid_schema, schema_err = GLOBAL_CONTRACT_REGISTRY.validate(cid, params)

        if not is_valid_schema:
            status = INVALID_SCHEMA
            contract_entry = {**item, "status": status, "error": schema_err}
            contract_details.append(contract_entry)
            missing_required_contracts.append(contract_entry)
            missing_required_caps.add(cid)
        elif cid not in registered:
            status = NOT_REGISTERED
            contract_entry = {**item, "status": status, "error": f"Capability '{cid}' not registered"}
            contract_details.append(contract_entry)
            missing_required_contracts.append(contract_entry)
            missing_required_caps.add(cid)
        elif callable(supports_contract_fn) and not supports_contract_fn(cid, domain, signal):
            status = NOT_REGISTERED
            contract_entry = {**item, "status": status, "error": f"Contract [{domain}.{signal}] not supported by {cid}"}
            contract_details.append(contract_entry)
            missing_required_contracts.append(contract_entry)
            missing_required_caps.add(cid)
        else:
            status = REGISTERED
            contract_details.append({**item, "status": status})

    missing_optional_contracts: list[dict[str, Any]] = []
    missing_optional_caps: set[str] = set()

    for item in contracts["optional"] + contracts["adaptive"]:
        cid = item["capability"]
        domain = item.get("domain")
        signal = item.get("signal")
        params = item.get("parameters", {})

        is_valid_schema, schema_err = GLOBAL_CONTRACT_REGISTRY.validate(cid, params)
        if not is_valid_schema:
            status = INVALID_SCHEMA
            contract_entry = {**item, "status": status, "error": schema_err}
            contract_details.append(contract_entry)
            missing_optional_contracts.append(contract_entry)
            missing_optional_caps.add(cid)
        elif cid not in registered:
            status = NOT_REGISTERED
            contract_entry = {**item, "status": status, "error": f"Capability '{cid}' not registered"}
            contract_details.append(contract_entry)
            missing_optional_contracts.append(contract_entry)
            missing_optional_caps.add(cid)
        elif callable(supports_contract_fn) and not supports_contract_fn(cid, domain, signal):
            status = NOT_REGISTERED
            contract_entry = {**item, "status": status, "error": f"Contract [{domain}.{signal}] not supported by {cid}"}
            contract_details.append(contract_entry)
            missing_optional_contracts.append(contract_entry)
            missing_optional_caps.add(cid)
        else:
            status = REGISTERED
            contract_details.append({**item, "status": status})

    missing_required = sorted(missing_required_caps | (set(caps["required"]) - registered))
    missing_optional = sorted(missing_optional_caps | (set(caps["optional"]) - registered))
    missing_contextual = sorted(set(caps["contextual"]) - registered)

    status = COVERAGE_BLOCKED if (missing_required or missing_required_contracts) else (
        COVERAGE_PARTIAL if (missing_optional or missing_contextual or missing_optional_contracts) else COVERAGE_READY
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
        required_coverage=COVERAGE_BLOCKED if (missing_required or missing_required_contracts) else COVERAGE_READY,
        registry_inspectable=True,
        contract_details=contract_details,
        missing_required_contracts=missing_required_contracts,
        missing_optional_contracts=missing_optional_contracts,
    )
