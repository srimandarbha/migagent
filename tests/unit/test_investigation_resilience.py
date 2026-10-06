import time
import pytest
from engine.contracts import AgentState, EvidenceStatus
from engine.integrations.registry import InMemoryCapabilityRegistry, RegistryError
from engine.integrations.contracts import GLOBAL_CONTRACT_REGISTRY
from engine.tools.investigation import InvestigationTool


def test_investigation_tool_timeout():
    def slow_capability(params):
        time.sleep(0.3)
        return {"evidence": []}

    registry = InMemoryCapabilityRegistry({
        "observability.search": slow_capability,
    })
    # Set a tiny 0.05s timeout
    tool = InvestigationTool(registry, timeout_seconds=0.05)
    state = AgentState(failure_case_id="case-timeout-test", event={"event_id": "ev-01"})

    tool.collect(state, {"capability": "observability.search", "parameters": {"domain": "ocv", "signal": "pvc_state"}})

    assert len(state.capability_results) == 1
    res = state.capability_results[0]
    assert res["status"] == EvidenceStatus.UNAVAILABLE.value
    assert res["result_status"] == "TIMEOUT"
    assert res["error_category"] == "TIMEOUT"
    assert "timed out" in res["error"]
    assert any("timed out" in err for err in state.capability_errors)


def test_investigation_tool_circuit_breaker():
    call_count = 0

    def failing_capability(params):
        nonlocal call_count
        call_count += 1
        raise RegistryError("Upstream service unavailable (500)")

    registry = InMemoryCapabilityRegistry({
        "observability.search": failing_capability,
    })
    tool = InvestigationTool(registry, timeout_seconds=2.0, failure_threshold=3, recovery_time_seconds=60.0)
    state = AgentState(failure_case_id="case-circuit-test", event={"event_id": "ev-02"})

    req = {"capability": "observability.search", "parameters": {"domain": "ocv", "signal": "pvc_state"}}

    # Call 3 times to trip breaker
    for _ in range(3):
        tool.collect(state, req)

    assert call_count == 3
    assert not tool._is_circuit_open("observability.search") is False  # Is open!

    # 4th call should fail fast without invoking registry
    tool.collect(state, req)
    assert call_count == 3  # Registry was NOT called!

    last_res = state.capability_results[-1]
    assert last_res["status"] == EvidenceStatus.UNAVAILABLE.value
    assert last_res["result_status"] == "CIRCUIT_OPEN"
    assert last_res["error_category"] == "CIRCUIT_OPEN"
    assert "circuit breaker" in state.capability_errors[-1].lower()


def test_new_capability_contracts_validation():
    # Test k8s.get_resource
    valid, err = GLOBAL_CONTRACT_REGISTRY.validate("k8s.get_resource", {"namespace": "openshift-mtv", "kind": "DataVolume", "name": "rhel-dv"})
    assert valid is True
    assert err is None

    invalid, err = GLOBAL_CONTRACT_REGISTRY.validate("k8s.get_resource", {"namespace": "openshift-mtv", "name": "rhel-dv"})
    assert invalid is False
    assert "Missing required parameter 'kind'" in err

    # Test vmware.get_vm_spec
    valid, err = GLOBAL_CONTRACT_REGISTRY.validate("vmware.get_vm_spec", {"vm_id": "vm-101"})
    assert valid is True
    assert err is None

    invalid, err = GLOBAL_CONTRACT_REGISTRY.validate("vmware.get_vm_spec", {})
    assert invalid is False
    assert "Missing required parameter 'vm_id'" in err

    # Test network.test_reachability
    valid, err = GLOBAL_CONTRACT_REGISTRY.validate("network.test_reachability", {"target_host": "esxi01.corp", "target_port": 902})
    assert valid is True
    assert err is None

    invalid, err = GLOBAL_CONTRACT_REGISTRY.validate("network.test_reachability", {"target_host": "esxi01.corp"})
    assert invalid is False
    assert "Missing required parameter 'target_port'" in err

    # Test vault.validate_secret_metadata
    valid, err = GLOBAL_CONTRACT_REGISTRY.validate("vault.validate_secret_metadata", {"secret_name": "vcenter-creds", "namespace": "openshift-mtv"})
    assert valid is True
    assert err is None

    # Test conversion.inspect_virt_v2v_pod
    valid, err = GLOBAL_CONTRACT_REGISTRY.validate("conversion.inspect_virt_v2v_pod", {"namespace": "openshift-mtv", "migration_id": "mig-001"})
    assert valid is True
    assert err is None
