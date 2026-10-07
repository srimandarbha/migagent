"""Causal Chain Reasoning Engine for Migration Failures (Phase 4).

Constructs the formal causal chain:
Observable Symptom ──► Failure Mechanism ──► Contributing Factors ──► Root Cause Candidate

Replaces hardcoded 'root_cause: UNKNOWN' with evidence-backed causal attribution.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class CausalChain:
    symptom: str
    mechanism: str
    contributing_factors: List[str] = field(default_factory=list)
    root_cause: str = "UNKNOWN"
    causal_explanation: str = ""
    evidence_ids: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symptom": self.symptom,
            "mechanism": self.mechanism,
            "contributing_factors": self.contributing_factors,
            "root_cause": self.root_cause,
            "causal_explanation": self.causal_explanation,
            "evidence_ids": self.evidence_ids,
        }


# Canonical root cause deduction rules based on classification, mechanism, and observed facts
ROOT_CAUSE_MAP: Dict[str, Dict[str, Any]] = {
    "STORAGE.CSI.PROVISIONING_TIMEOUT": {
        "symptom": "PersistentVolumeClaim provisioning stalled in Pending phase",
        "branches": [
            {
                "when_fact": "BACKEND_DELL_DEGRADED",
                "root_cause": "STORAGE_BACKEND_DELL_POWERSTORE_DEGRADED",
                "explanation": (
                    "Dell PowerStore/PowerMax storage backend health is degraded or in an error state, "
                    "preventing the CSI driver from provisioning the target migration volume."
                ),
            },
            {
                "when_fact": "BACKEND_PURE_DEGRADED",
                "root_cause": "STORAGE_BACKEND_PURE_STORAGE_DEGRADED",
                "explanation": (
                    "Pure Storage array health is degraded or array status is offline, "
                    "preventing the CSI driver from provisioning the target migration volume."
                ),
            },
            {
                "when_fact": "BACKEND_PORTWORX_DEGRADED",
                "root_cause": "STORAGE_BACKEND_PORTWORX_DEGRADED",
                "explanation": (
                    "Portworx cluster or node status reports degraded storage pool, "
                    "preventing the CSI driver from provisioning the target migration volume."
                ),
            },
            {
                "when_fact": "BACKEND_TRIDENT_DEGRADED",
                "root_cause": "STORAGE_BACKEND_NETAPP_TRIDENT_DEGRADED",
                "explanation": (
                    "NetApp Trident storage backend reports degraded backend state or ONTAP communication failure, "
                    "preventing the CSI driver from provisioning the target migration volume."
                ),
            },
            {
                "when_fact": "BACKEND_CEPH_DEGRADED",
                "root_cause": "STORAGE_BACKEND_CEPH_ODF_DEGRADED",
                "explanation": (
                    "Ceph/ODF storage backend health is degraded or in an error state, "
                    "preventing the CSI driver from provisioning the target migration volume."
                ),
            },
            {
                "when_fact": "BACKEND_UNHEALTHY",
                "root_cause": "STORAGE_BACKEND_CLUSTER_DEGRADATION",
                "explanation": (
                    "Ceph/ODF or configured CSI storage backend health is degraded or in an error state, "
                    "preventing the CSI driver from provisioning the target migration volume."
                ),
            },
            {
                "when_fact": "CSI_CONTROLLER_PROVISIONING_ERROR",
                "root_cause": "CSI_PROVISIONER_CONTROLLER_COMMUNICATION_FAULT",
                "explanation": (
                    "Storage backend is healthy, but the CSI provisioner controller pod "
                    "failed to communicate with the driver or exceeded API timeout."
                ),
            },
            {
                "when_fact": "PVC_PENDING",
                "root_cause": "STORAGE_CSI_PROVISIONING_TIMEOUT",
                "explanation": (
                    "Volume provisioning request was accepted by Kubernetes but timed out "
                    "before the CSI storage driver completed allocation."
                ),
            },
        ],
    },
    "NETWORK.NAD.MISSING": {
        "symptom": "VM pod scheduling failed due to network attachment definition error",
        "root_cause": "NETWORK_ATTACHMENT_DEFINITION_MISSING_IN_TARGET_NAMESPACE",
        "explanation": (
            "The migration network mapping references a NetworkAttachmentDefinition (NAD) "
            "that does not exist in the target OpenShift Virtualization namespace."
        ),
    },
    "VMWARE.CBT": {
        "symptom": "Warm migration incremental sync failed during CBT snapshot capture",
        "root_cause": "VMWARE_CHANGED_BLOCK_TRACKING_INCONSISTENCY_OR_LOCK",
        "explanation": (
            "VMware vSphere CBT (Changed Block Tracking) snapshot could not be generated "
            "due to existing stale snapshots, locking, or disabled change tracking on the source VM."
        ),
    },
    "VMWARE.VDDK.PERMISSION": {
        "symptom": "VixDiskLib transport open failed with Error 13 (Access Rights)",
        "root_cause": "VDDK_SERVICE_ACCOUNT_MISSING_VIX_OR_DATASTORE_PRIVILEGES",
        "explanation": (
            "The VMware service account credentials lack necessary VIX, CIM, or datastore "
            "browse permissions to open the VMDK via VDDK NBD transport."
        ),
    },
    "VMWARE.ESXI.CONNECTIVITY": {
        "symptom": "Migration data connection to source ESXi host timed out",
        "root_cause": "ESXI_HOST_NETWORK_UNREACHABLE_OR_FIREWALL_BLOCKED",
        "explanation": (
            "OpenShift migration worker nodes cannot reach TCP port 443/902 on the ESXi host "
            "due to network routing, firewall ACLs, or ESXi host management agent failure."
        ),
    },
    "CONVERSION.VIRT_V2V.OOM": {
        "symptom": "virt-v2v conversion pod terminated with OOMKilled",
        "root_cause": "CONVERSION_POD_MEMORY_EXHAUSTED_BY_DISK_INSPECTION",
        "explanation": (
            "The memory required to inspect multiple partitions and run guestfs exceeded "
            "the container memory limit allocated to the virt-v2v conversion pod."
        ),
    },
    "CONVERSION.VIRT_V2V.CDROM": {
        "symptom": "virt-v2v failed to parse hardware configuration due to unmounted ISO",
        "root_cause": "STALE_OR_DISCONNECTED_CDROM_ISO_ATTACHED_TO_SOURCE_VM",
        "explanation": (
            "The source VM has a virtual CD-ROM device mapped to an ISO datastore path that "
            "is no longer accessible or mounted."
        ),
    },
    "OS.WINDOWS.FILESYSTEM_READONLY": {
        "symptom": "NTFS volume opened in read-only mode by libguestfs",
        "root_cause": "WINDOWS_FAST_STARTUP_HIBERNATION_LOCK_ACTIVE",
        "explanation": (
            "Windows Fast Startup left the NTFS filesystem in an unclosed hibernation state, "
            "preventing virt-v2v from safely writing VirtIO drivers."
        ),
    },
    "GUEST.WINDOWS.BITLOCKER": {
        "symptom": "virt-v2v unable to read Windows OS volume due to encryption",
        "root_cause": "BITLOCKER_FULL_VOLUME_ENCRYPTION_ACTIVE_WITHOUT_KEY",
        "explanation": (
            "Source Windows VM has BitLocker active on the system volume; cold conversion "
            "cannot mount encrypted partitions without decryption or suspension."
        ),
    },
    "GUEST.LINUX.BTRFS": {
        "symptom": "libguestfs failed to inspect Linux root filesystem",
        "root_cause": "UNSUPPORTED_BTRFS_SUBVOLUME_LAYOUT_IN_GUEST_OS",
        "explanation": (
            "The source Linux VM uses a non-standard Btrfs subvolume layout that is not "
            "supported by the current virt-v2v guest inspection engine."
        ),
    },
    "DISK.RESIZE_FAILED": {
        "symptom": "Failed to expand target PersistentVolumeClaim during disk preparation",
        "root_cause": "STORAGE_CLASS_DOES_NOT_SUPPORT_EXPANSION_OR_STORAGE_EXHAUSTION",
        "explanation": (
            "The target storage class does not support volume expansion or the underlying "
            "storage pool has insufficient capacity to accommodate the source disk size."
        ),
    },
}


def build_causal_chain(
    classification: str,
    mechanism: Optional[str],
    facts: Set[str],
    evidence_ids_by_fact: Dict[str, List[str]],
    context: Optional[Dict[str, Any]] = None,
) -> CausalChain:
    """Builds an evidence-backed causal chain for the migration failure."""
    mech = mechanism or classification
    rule_entry = ROOT_CAUSE_MAP.get(classification, {})

    symptom = rule_entry.get("symptom", f"Migration failed with signature {classification}")
    contributing_factors: List[str] = []
    root_cause = "UNKNOWN"
    explanation = ""

    # Check for general contributing factors
    if "NODE_MEMORY_PRESSURE" in facts:
        contributing_factors.append("Worker node experiencing memory pressure")
    if "NETWORK_PACKET_LOSS" in facts:
        contributing_factors.append("Elevated network packet loss detected")
    if "TRANSFER_RATE_DEGRADED" in facts:
        contributing_factors.append("MTV disk transfer throughput degraded")
    if any(f in facts for f in ("BACKEND_UNHEALTHY", "BACKEND_DELL_DEGRADED", "BACKEND_PURE_DEGRADED", "BACKEND_PORTWORX_DEGRADED", "BACKEND_TRIDENT_DEGRADED", "BACKEND_CEPH_DEGRADED")) and classification != "STORAGE.CSI.PROVISIONING_TIMEOUT":
        contributing_factors.append("Storage backend reports degraded health")
    if "BACKEND_HEALTH_UNKNOWN" in facts:
        contributing_factors.append("Storage backend health could not be verified from available telemetry")

    # Evaluate branches if present
    branches = rule_entry.get("branches", [])
    if branches:
        for branch in branches:
            when = branch.get("when_fact")
            if when in facts:
                root_cause = branch.get("root_cause", "UNKNOWN")
                explanation = branch.get("explanation", "")
                break

    # Contextual refinement if generic backend degradation matched
    if root_cause == "STORAGE_BACKEND_CLUSTER_DEGRADATION" and context:
        backend_name = str(context.get("storage_backend") or context.get("storage_class") or "").lower()
        if any(b in backend_name for b in ("dell", "powerstore", "powermax")):
            root_cause = "STORAGE_BACKEND_DELL_POWERSTORE_DEGRADED"
            explanation = "Dell PowerStore/PowerMax storage backend health is degraded, preventing CSI provisioning."
        elif "pure" in backend_name:
            root_cause = "STORAGE_BACKEND_PURE_STORAGE_DEGRADED"
            explanation = "Pure Storage FlashArray backend health is degraded or array status is offline, preventing CSI provisioning."
        elif any(b in backend_name for b in ("portworx", "px")):
            root_cause = "STORAGE_BACKEND_PORTWORX_DEGRADED"
            explanation = "Portworx cluster reports degraded storage pool or node error, preventing CSI provisioning."
        elif any(b in backend_name for b in ("trident", "netapp", "ontap")):
            root_cause = "STORAGE_BACKEND_NETAPP_TRIDENT_DEGRADED"
            explanation = "NetApp Trident storage backend reports degraded backend state or ONTAP communication failure, preventing CSI provisioning."
        elif any(b in backend_name for b in ("ceph", "odf", "ocs")):
            root_cause = "STORAGE_BACKEND_CEPH_ODF_DEGRADED"
            explanation = "Ceph/ODF storage backend health is degraded or in an error state, preventing CSI provisioning."

    if root_cause == "UNKNOWN" and "root_cause" in rule_entry:
        root_cause = rule_entry["root_cause"]
        explanation = rule_entry.get("explanation", "")

    if root_cause == "UNKNOWN":
        root_cause = f"ROOT_CAUSE_{classification.replace('.', '_')}"
        explanation = f"Current evidence establishes mechanism {mech}."

    all_involved_eids = []
    for f in facts:
        all_involved_eids.extend(evidence_ids_by_fact.get(f, []))

    return CausalChain(
        symptom=symptom,
        mechanism=mech,
        contributing_factors=contributing_factors,
        root_cause=root_cause,
        causal_explanation=explanation,
        evidence_ids=list(dict.fromkeys(all_involved_eids)),
    )
