"""Acceptance tests for Task 5: Inverting five fail-open defaults."""
import json
import pytest

from engine.contracts.models import _parse_evidence_status, EvidenceStatus
from engine.integrations.contracts import GLOBAL_CONTRACT_REGISTRY
from engine.integrations.registry import InMemoryCapabilityRegistry, RegistryError
from engine.llm.advisory import parse_advisory


def test_fail_closed_empty_evidence_status():
    """Site 1: empty or None evidence status must parse to EvidenceStatus.UNKNOWN."""
    assert _parse_evidence_status(None) == EvidenceStatus.UNKNOWN
    assert _parse_evidence_status("") == EvidenceStatus.UNKNOWN
    assert _parse_evidence_status("UNKNOWN") == EvidenceStatus.UNKNOWN


def test_fail_closed_metadata_less_adapter():
    """Site 2: An unmanaged metadata-less adapter must return False from supports_contract."""
    reg = InMemoryCapabilityRegistry({
        "unmanaged.custom_tool": lambda p: {"data": "ok"},
    })
    assert reg.supports_contract("unmanaged.custom_tool", "storage", "backend_health") is False
    assert reg.supports_contract("unmanaged.custom_tool") is False


def test_fail_closed_unknown_capability_contract():
    """Site 3: GLOBAL_CONTRACT_REGISTRY.validate returns (False, f"capability '{cid}' is not under contract management")."""
    is_valid, msg = GLOBAL_CONTRACT_REGISTRY.validate("completely.unknown.capability", {})
    assert is_valid is False
    assert "capability 'completely.unknown.capability' is not under contract management" in msg


def test_fail_closed_registry_introspection_failure():
    """Site 4: When a registry is provided but listing raises, mark suggestions validated=False with validation_error='registry introspection failed'."""
    class BrokenRegistry:
        def list_capabilities(self):
            raise RuntimeError("Registry introspection exploded")

    raw_advisory = json.dumps({
        "summary": "Try this",
        "suggested_investigations": [
            {
                "action": "CHECK_LOGS",
                "purpose": "Find root cause",
                "capability": "observability.search",
                "parameters": {"domain": "ocv", "signal": "pvc_state"},
            }
        ],
    })
    parsed = parse_advisory(raw_advisory, registry=BrokenRegistry())
    assert parsed["status"] == "ADVISORY"
    inv = parsed["suggested_investigations"][0]
    assert inv["validated"] is False
    assert inv["validation_error"] == "registry introspection failed"


def test_fail_closed_invoke_write_access():
    """Site 5: invoke() rejects non-read access when not allowed by contract, raising RegistryError."""
    reg = InMemoryCapabilityRegistry({
        "observability.search": lambda p: {"evidence": []},
    })
    with pytest.raises(RegistryError, match="not permitted"):
        reg.invoke("observability.search", {}, access="write")
