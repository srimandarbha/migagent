"""Capability parameter contracts and validation schemas.

Validates that capability invocations conform to declared domain/signal
and parameter specifications rather than only checking the capability ID.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Set, Tuple

ALLOWED_DOMAINS = {
    "ocv",
    "storage",
    "vmware",
    "conversion",
    "guest_os",
    "mtv",
    "network",
    "security",
    "environment",
}

KNOWN_SIGNALS = {
    # Conversion
    "virt_v2v_log",
    "virt_v2v_pod",
    "pod_lifecycle",
    "oom_status",
    # Guest OS
    "bitlocker_state",
    "boot_errors",
    "driver_state",
    "filesystem_state",
    "filesystem_type",
    "registry_state",
    "vss_state",
    # MTV
    "migration_state",
    "transfer_errors",
    "vddk_data_source",
    # Network
    "port_reachability",
    # OCV
    "admission_events",
    "csi_controller_errors",
    "csi_errors",
    "csi_provisioning_latency",
    "nad_state",
    "network_attachment_state",
    "network_events",
    "pod_status",
    "pvc_events",
    "pvc_state",
    "resource_status",
    "vm_spec",
    # Security / Vault
    "secret_metadata",
    # Storage
    "backend_health",
    "disk_resize_error",
    "volume_capacity",
    "iops",
    # VMware
    "attached_media",
    "auth_errors",
    "cbt_state",
    "datastore_vmdk",
    "esxi_connectivity",
    "hardware_spec",
    "permissions",
    "snapshot_errors",
    "snapshot_tree",
    "task_state",
    "vcenter_connectivity",
    "vddk_log",
    "vmx_configuration",
    # Environment
    "fingerprint",
}


@dataclass(frozen=True)
class CapabilityContract:
    capability: str
    required_parameters: Tuple[str, ...] = ("domain", "signal")
    optional_parameters: Tuple[str, ...] = ()
    allowed_domains: Optional[Set[str]] = None
    allowed_signals: Optional[Set[str]] = None
    allowed_access: Tuple[str, ...] = ("read",)

    def validate_parameters(self, params: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        if not isinstance(params, dict):
            return False, f"Parameters must be a dict, got {type(params).__name__}"

        for req in self.required_parameters:
            if req not in params:
                return False, f"Missing required parameter '{req}' for capability '{self.capability}'"
            val = params[req]
            if val is None or val == "":
                return False, f"Required parameter '{req}' cannot be empty for capability '{self.capability}'"

        domain = params.get("domain")
        if domain and self.allowed_domains and domain not in self.allowed_domains:
            return False, f"Domain '{domain}' is not in allowed domains for '{self.capability}': {sorted(self.allowed_domains)}"

        signal = params.get("signal")
        if signal and self.allowed_signals and signal not in self.allowed_signals:
            return False, f"Signal '{signal}' is not in known signals for '{self.capability}'"

        return True, None


@dataclass
class ContractRequirement:
    capability: str
    domain: Optional[str] = None
    signal: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    purpose: str = "Investigation"
    status: str = "UNKNOWN"

    @property
    def contract_key(self) -> str:
        d = self.domain or "*"
        s = self.signal or "*"
        return f"{self.capability}[domain={d},signal={s}]"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "capability": self.capability,
            "domain": self.domain,
            "signal": self.signal,
            "contract_key": self.contract_key,
            "parameters": self.parameters,
            "status": self.status,
        }


DEFAULT_CONTRACTS: Dict[str, CapabilityContract] = {
    "observability.search": CapabilityContract(
        capability="observability.search",
        required_parameters=("domain", "signal"),
        allowed_domains=ALLOWED_DOMAINS,
        allowed_signals=KNOWN_SIGNALS,
    ),
    "metrics.query": CapabilityContract(
        capability="metrics.query",
        required_parameters=("domain", "signal"),
        allowed_domains={"ocv", "storage"},
        allowed_signals={"csi_provisioning_latency", "iops"},
    ),
    "knowledge.search": CapabilityContract(
        capability="knowledge.search",
        required_parameters=("query",),
    ),
    "sre_tracker.search_history": CapabilityContract(
        capability="sre_tracker.search_history",
        required_parameters=(),
    ),
    "sre_tracker.search_periodic": CapabilityContract(
        capability="sre_tracker.search_periodic",
        required_parameters=(),
    ),
    "k8s.get_resource": CapabilityContract(
        capability="k8s.get_resource",
        required_parameters=("namespace", "kind", "name"),
        optional_parameters=("cluster_id", "domain", "signal"),
        allowed_domains={"ocv", "storage", "network"},
    ),
    "vmware.get_vm_spec": CapabilityContract(
        capability="vmware.get_vm_spec",
        required_parameters=("vm_id",),
        optional_parameters=("vcenter_url", "domain", "signal"),
        allowed_domains={"vmware"},
    ),
    "network.test_reachability": CapabilityContract(
        capability="network.test_reachability",
        required_parameters=("target_host", "target_port"),
        optional_parameters=("source_node", "domain", "signal"),
        allowed_domains={"network", "vmware"},
    ),
    "vault.validate_secret_metadata": CapabilityContract(
        capability="vault.validate_secret_metadata",
        required_parameters=("secret_name", "namespace"),
        optional_parameters=("domain", "signal"),
        allowed_domains={"security", "ocv"},
    ),
    "conversion.inspect_virt_v2v_pod": CapabilityContract(
        capability="conversion.inspect_virt_v2v_pod",
        required_parameters=("namespace", "migration_id"),
        optional_parameters=("domain", "signal"),
        allowed_domains={"conversion", "ocv"},
    ),
    "environment.get_fingerprint": CapabilityContract(
        capability="environment.get_fingerprint",
        required_parameters=(),
        optional_parameters=("cluster_id", "vm_id", "migration_id", "domain", "signal"),
        allowed_domains={"ocv", "vmware", "environment"},
    ),
}


class CapabilityContractRegistry:
    """Registry of known capability contracts."""

    def __init__(self, contracts: Optional[Dict[str, CapabilityContract]] = None):
        self._contracts = dict(contracts or DEFAULT_CONTRACTS)

    def register(self, contract: CapabilityContract) -> None:
        self._contracts[contract.capability] = contract

    def get(self, capability_id: str) -> Optional[CapabilityContract]:
        return self._contracts.get(capability_id)

    def validate(self, capability_id: str, params: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        contract = self.get(capability_id)
        if contract is None:
            return False, f"capability '{capability_id}' is not under contract management"
        return contract.validate_parameters(params)


GLOBAL_CONTRACT_REGISTRY = CapabilityContractRegistry()
