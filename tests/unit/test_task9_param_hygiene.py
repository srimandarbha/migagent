"""Acceptance tests for Task 9: Capability parameter hygiene and deterministic contract validation."""
from engine.contracts import AgentState
from engine.integrations.registry import InMemoryCapabilityRegistry
from engine.tools.investigation import InvestigationTool


def test_event_fields_not_spread_into_params():
    """Event fields other than the allowed 7 are not spread into capability params."""
    received_params = {}

    def mock_search(params):
        received_params.update(params)
        return {"evidence": []}

    reg = InMemoryCapabilityRegistry({"observability.search": mock_search})
    executor = InvestigationTool(reg)

    event = {
        "event_id": "evt-clean-01",
        "migration_id": "mig-01",
        "vm_id": "vm-01",
        "cluster_id": "cl-01",
        "failure_code": "storage.csi.error",
        "severity": "HIGH",
        "phase": "TARGET_STORAGE",
        "internal_debug_dump": "SHOULD_NOT_LEAK",
        "secret_token": "SENSITIVE_DATA",
        "raw_payload": {"nested": "value"},
    }

    state = AgentState(failure_case_id="case-clean-01", event=event)
    req = {
        "capability": "observability.search",
        "parameters": {"domain": "ocv", "signal": "pvc_state"},
    }

    executor.collect(state, req)

    # Allowed fields and req parameters are present
    assert received_params.get("domain") == "ocv"
    assert received_params.get("signal") == "pvc_state"
    assert received_params.get("event_id") == "evt-clean-01"
    assert received_params.get("migration_id") == "mig-01"
    assert received_params.get("vm_id") == "vm-01"
    assert received_params.get("cluster_id") == "cl-01"
    assert received_params.get("failure_code") == "storage.csi.error"
    assert received_params.get("severity") == "HIGH"
    assert received_params.get("phase") == "TARGET_STORAGE"
    assert received_params.get("failure_case_id") == "case-clean-01"

    # Leaked fields are absent
    assert "internal_debug_dump" not in received_params
    assert "secret_token" not in received_params
    assert "raw_payload" not in received_params


def test_contract_invalid_capability_is_not_invoked():
    """Requirements violating capability contract schema are not submitted to the executor."""
    invoked = False

    def mock_search(params):
        nonlocal invoked
        invoked = True
        return {"evidence": []}

    reg = InMemoryCapabilityRegistry({"observability.search": mock_search})
    executor = InvestigationTool(reg)

    state = AgentState(failure_case_id="case-invalid-01", event={"event_id": "e1"})
    # Invalid signal for observability.search
    req = {
        "capability": "observability.search",
        "parameters": {"domain": "ocv", "signal": "completely_invalid_signal_xyz"},
    }

    executor.collect(state, req)

    assert invoked is False
    assert len(state.capability_results) == 1
    res = state.capability_results[0]
    assert res["status"] == "ERROR"
    assert res["result_status"] == "CONTRACT_INVALID"
    assert res["error_category"] == "CONTRACT_INVALID"
