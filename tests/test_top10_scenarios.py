"""Top 10 Source-Backed Migration Failure Scenario Tests.

Verifies the full vertical slice across the top 10 diagnostic scenarios:
  raw error -> classification -> investigation policy -> capability coverage
  -> evidence collection -> evidence evaluation -> differential diagnosis -> decision readiness.
"""
from __future__ import annotations

import pytest
from simulator.runner import simulate
from engine.workflow.engine import run_agent
from simulator.world import make_world


class FakeAdvisoryLLM:
    model = "fake-advisory-v1"

    def generate(self, messages, **kwargs):
        assert messages[0]["role"] == "system"
        return (
            '{"summary": "Evidence is insufficient; investigate endpoint reachability and network policy.", '
            '"suggested_investigations": ['
            '{"action": "CHECK_NETWORK_CONNECTIVITY", "purpose": "Verify endpoint routing", "capability": "observability.search", "parameters": {"domain": "network", "signal": "egress"}}, '
            '{"action": "INSPECT_STORAGE_EVENTS", "purpose": "Check volume attachment events", "capability": "observability.search", "parameters": {"domain": "ocv", "signal": "pvc_events"}}'
            '], "uncertainty": "No conclusive evidence available in the telemetry."}'
        )


def test_scenario_01_windows_vss():
    """Test 01: Windows VSS failure during snapshot creation."""
    out = simulate("windows-vss", memory_mode="none")
    assert out["classification"] == "GUEST.WINDOWS.VSS"
    assert out["diagnosis"]["status"] == "LIKELY"
    assert out["diagnosis"]["mechanism"] == "GUEST.WINDOWS.VSS"
    assert out["next_step"] == "INVESTIGATE_WINDOWS_VSS"
    assert out["decision_readiness"]["RETRY"]["status"] == "NOT_READY"
    blockers = out["decision_readiness"]["RETRY"]["blockers"]
    assert any("VSS_SERVICE_RUNNING" in b for b in blockers)


def test_scenario_02_windows_hivex():
    """Test 02: Windows registry Hivex error during virt-v2v inspection."""
    out = simulate("windows-hivex", memory_mode="none")
    assert out["classification"] == "GUEST.WINDOWS.REGISTRY"
    assert out["diagnosis"]["status"] == "LIKELY"
    assert out["diagnosis"]["mechanism"] == "GUEST.WINDOWS.REGISTRY"
    assert out["next_step"] == "REPAIR_GUEST_REGISTRY"
    assert out["decision_readiness"]["RETRY"]["status"] == "NOT_READY"
    blockers = out["decision_readiness"]["RETRY"]["blockers"]
    assert any("REGISTRY_HIVE_VALID" in b for b in blockers)


def test_scenario_03_windows_bitlocker():
    """Test 03: Windows BitLocker volume encryption block."""
    out = simulate("windows-bitlocker", memory_mode="none")
    assert out["classification"] == "GUEST.WINDOWS.BITLOCKER"
    assert out["diagnosis"]["status"] == "LIKELY"
    assert out["diagnosis"]["mechanism"] == "GUEST.WINDOWS.BITLOCKER"
    assert out["next_step"] == "DISABLE_OR_UNLOCK_BITLOCKER"
    assert out["decision_readiness"]["RETRY"]["status"] == "NOT_READY"
    blockers = out["decision_readiness"]["RETRY"]["blockers"]
    assert any("BITLOCKER_DECRYPTED" in b for b in blockers)


def test_scenario_04_windows_firmware():
    """Test 04: KubeVirt admission webhook EFI/BIOS mutual exclusion."""
    out = simulate("windows-firmware", memory_mode="none")
    assert out["classification"] == "OCV.VM.FIRMWARE"
    assert out["diagnosis"]["status"] == "LIKELY"
    assert out["diagnosis"]["mechanism"] == "OCV.VM.FIRMWARE"
    assert out["next_step"] == "RECONCILE_VM_BOOTLOADER"
    assert out["decision_readiness"]["RETRY"]["status"] == "NOT_READY"
    blockers = out["decision_readiness"]["RETRY"]["blockers"]
    assert any("FIRMWARE_CONFIG_VALID" in b for b in blockers)


def test_scenario_05_linux_btrfs():
    """Test 05: Linux Btrfs unsupported filesystem error."""
    out = simulate("linux-btrfs", memory_mode="none")
    assert out["classification"] == "GUEST.LINUX.BTRFS"
    assert out["diagnosis"]["status"] == "LIKELY"
    assert out["diagnosis"]["mechanism"] == "GUEST.LINUX.BTRFS"
    assert out["next_step"] == "CONVERT_OR_EXCLUDE_BTRFS"
    assert out["decision_readiness"]["RETRY"]["status"] == "NOT_READY"
    blockers = out["decision_readiness"]["RETRY"]["blockers"]
    assert any("SUPPORTED_FILESYSTEM" in b for b in blockers)


def test_scenario_06_vmware_vddk_permission():
    """Test 06: VDDK transport permission Error 13."""
    out = simulate("vmware-vddk-permission", memory_mode="none")
    assert out["classification"] == "VMWARE.VDDK.PERMISSION"
    assert out["diagnosis"]["status"] == "LIKELY"
    assert out["diagnosis"]["mechanism"] == "VMWARE.VDDK.PERMISSION"
    assert out["next_step"] == "CHECK_VDDK_PERMISSIONS"
    assert out["decision_readiness"]["RETRY"]["status"] == "NOT_READY"
    blockers = out["decision_readiness"]["RETRY"]["blockers"]
    assert any("VDDK_PERMISSIONS_GRANTED" in b for b in blockers)


def test_scenario_07_vmware_vddk_data_source():
    """Test 07: VDDK NBD export missing from data source."""
    out = simulate("vmware-vddk-data-source", memory_mode="none")
    assert out["classification"] == "VMWARE.VDDK.DATA_SOURCE"
    assert out["diagnosis"]["status"] == "LIKELY"
    assert out["diagnosis"]["mechanism"] == "VMWARE.VDDK.DATA_SOURCE"
    assert out["next_step"] == "VERIFY_VDDK_NBD_EXPORT"
    assert out["decision_readiness"]["RETRY"]["status"] == "NOT_READY"
    blockers = out["decision_readiness"]["RETRY"]["blockers"]
    assert any("NBD_EXPORT_ACTIVE" in b for b in blockers)


def test_scenario_08_csi_timeout_healthy_backend():
    """Test 08: CSI timeout with healthy storage backend -> controller path."""
    out = simulate("storage-csi-backend-healthy", memory_mode="none")
    assert out["classification"] == "STORAGE.CSI.PROVISIONING_TIMEOUT"
    assert out["diagnosis"]["status"] == "LIKELY"
    assert out["diagnosis"]["mechanism"] == "STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH"
    assert "backend degradation" not in out["diagnosis"]["statement"].lower()
    assert out["next_step"] == "REVIEW_CSI_CONTROLLER_FAILURE"


def test_scenario_09_csi_timeout_unhealthy_backend():
    """Test 09: CSI timeout with unhealthy backend -> backend degradation."""
    out = simulate("storage-csi-timeout", memory_mode="none")
    assert out["classification"] == "STORAGE.CSI.PROVISIONING_TIMEOUT"
    assert out["diagnosis"]["status"] == "LIKELY"
    assert out["diagnosis"]["mechanism"] == "STORAGE.BACKEND_DEGRADED"
    assert "backend degradation" in out["diagnosis"]["statement"].lower()
    assert out["next_step"] == "INVESTIGATE_STORAGE_BACKEND"


def test_scenario_10_unknown_insufficient_invokes_llm_without_hallucination():
    """Test 10: Unknown error with zero evidence falls back to LLM advisory."""
    scenario, registry, tracker, knowledge = make_world("insufficient-evidence", memory_mode="none")
    fake_llm = FakeAdvisoryLLM()
    req = {
        "failure_case_id": "test-unknown-llm",
        "memory_mode": "none",
        "event": {
            "event_id": "test-unknown-llm",
            "scenario": "insufficient-evidence",
            "phase": "UNKNOWN",
            "message": "Migration failed with unexpected internal error while processing source disk",
            "cluster_id": "ocv-prod-a",
        },
    }
    state = run_agent(req, registry, tracker=None, knowledge=None, memory_mode="none", llm_provider=fake_llm)
    assert state.classification == "UNKNOWN"
    assert state.diagnosis["status"] == "INSUFFICIENT_EVIDENCE"
    assert state.diagnosis["mechanism"] == "UNKNOWN"
    assert state.diagnosis["root_cause"] == "UNKNOWN"
    assert state.llm_advisory["status"] == "ADVISORY"
    assert len(state.llm_advisory["suggested_investigations"]) == 2
    assert state.llm_advisory["suggested_investigations"][0]["action"] == "CHECK_NETWORK_CONNECTIVITY"
    assert state.next_step == "COLLECT_MISSING_EVIDENCE"
