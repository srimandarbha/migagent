"""Fact vocabulary and ontology registry.

Provides strongly-typed fact definitions across storage, network, vmware,
conversion, guest_os, and ocv domains to prevent typos and vocabulary drift.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


@dataclass(frozen=True)
class FactDefinition:
    code: str
    domain: str
    polarity: str  # 'POSITIVE', 'NEGATIVE', 'NEUTRAL'
    description: str = ""


_FACT_DEFINITIONS: List[FactDefinition] = [
    # Storage
    FactDefinition("BACKEND_HEALTHY", "storage", "POSITIVE", "Storage backend reports healthy status."),
    FactDefinition("BACKEND_UNHEALTHY", "storage", "NEGATIVE", "Storage backend reports degraded or unhealthy state."),
    FactDefinition("BACKEND_DELL_DEGRADED", "storage", "NEGATIVE", "Dell PowerStore / PowerMax storage backend reports degraded state."),
    FactDefinition("BACKEND_PURE_DEGRADED", "storage", "NEGATIVE", "Pure Storage array reports degraded state."),
    FactDefinition("BACKEND_PORTWORX_DEGRADED", "storage", "NEGATIVE", "Portworx cluster reports degraded or node error state."),
    FactDefinition("BACKEND_TRIDENT_DEGRADED", "storage", "NEGATIVE", "NetApp Trident storage backend reports degraded state."),
    FactDefinition("BACKEND_CEPH_DEGRADED", "storage", "NEGATIVE", "Ceph / ODF storage backend reports degraded state."),
    FactDefinition("BACKEND_HEALTH_UNKNOWN", "storage", "NEUTRAL", "Storage backend health could not be determined due to missing telemetry."),
    FactDefinition("PVC_PENDING", "storage", "NEGATIVE", "PersistentVolumeClaim remains in Pending state."),
    FactDefinition("PVC_BOUND", "storage", "POSITIVE", "PersistentVolumeClaim successfully transitioned to Bound."),
    FactDefinition("PVC_PROVISIONING_FAILED", "storage", "NEGATIVE", "PVC provisioning encountered an error event."),
    FactDefinition("CSI_PROVISIONING_TIMEOUT", "storage", "NEGATIVE", "CSI volume provisioning timed out."),
    FactDefinition("CSI_CONTROLLER_PROVISIONING_ERROR", "storage", "NEGATIVE", "CSI controller provisioner reported an error."),
    FactDefinition("CSI_CONTROLLER_HEALTHY", "storage", "POSITIVE", "CSI controller provisioner reports healthy operation."),
    FactDefinition("CSI_PROVISIONING_PATH_CONFIRMED", "storage", "NEGATIVE", "CSI controller provisioning path is confirmed as failing mechanism."),
    FactDefinition("CSI_PROVISIONING_LATENCY_HIGH", "storage", "NEGATIVE", "CSI provisioning latency exceeds latency threshold."),
    FactDefinition("VOLUME_AVAILABLE", "storage", "POSITIVE", "Target storage volume is available and accessible."),
    FactDefinition("VOLUME_ACCESSIBLE", "storage", "POSITIVE", "Volume is accessible to the workload."),
    FactDefinition("TARGET_VOLUME_RESIZED", "storage", "POSITIVE", "Target storage capacity overhead expanded to accommodate resize."),
    FactDefinition("RESIZE_FAILED", "storage", "NEGATIVE", "Failed to resize disk image to required capacity."),
    FactDefinition("DISK_RESIZE_FAILED", "storage", "NEGATIVE", "Disk image resize failed during conversion."),
    FactDefinition("DISK_COUNT_WITHIN_LIMITS", "storage", "POSITIVE", "Disk count or command line arguments within system limits."),

    # Network
    FactDefinition("NAD_AVAILABLE", "network", "POSITIVE", "NetworkAttachmentDefinition exists and is available."),
    FactDefinition("NAD_MISSING", "network", "NEGATIVE", "Required NetworkAttachmentDefinition does not exist in target namespace."),
    FactDefinition("NAD_NOT_FOUND", "network", "NEGATIVE", "Target cluster reports NetworkAttachmentDefinition not found."),
    FactDefinition("NETWORK_ATTACHMENT_FAILED", "network", "NEGATIVE", "Failed to attach pod network interface to NAD."),
    FactDefinition("NETWORK_MAPPING_VALID", "network", "POSITIVE", "MTV NetworkMap correctly maps source network to target NAD."),

    # VMware CBT & Transfer
    FactDefinition("CBT_READY", "vmware", "POSITIVE", "VMware Changed Block Tracking is enabled and query succeeds."),
    FactDefinition("CBT_FAILED", "vmware", "NEGATIVE", "VMware Changed Block Tracking query or snapshot operation failed."),
    FactDefinition("CBT_QUERY_FAILED", "vmware", "NEGATIVE", "QueryChangedDiskAreas returned an error."),
    FactDefinition("CBT_LIMIT_REACHED", "vmware", "NEGATIVE", "Warm migration snapshot iteration retry limit reached."),
    FactDefinition("VM_ACCESSIBLE", "vmware", "POSITIVE", "Source virtual machine is accessible via VMware APIs."),
    FactDefinition("VM_VALIDATED", "vmware", "POSITIVE", "Source virtual machine configuration is validated."),
    FactDefinition("TRANSFER_FAILED", "vmware", "NEGATIVE", "Data transfer from VMware source failed."),

    # VMware Connectivity
    FactDefinition("ESXI_PORT443_REACHABLE", "vmware", "POSITIVE", "ESXi host reachable on TCP 443."),
    FactDefinition("ESXI_PORT443_UNREACHABLE", "vmware", "NEGATIVE", "Target cluster cannot connect to ESXi host on TCP 443."),
    FactDefinition("ESXI_PORT902_REACHABLE", "vmware", "POSITIVE", "ESXi host reachable on TCP 902 for NFC data transfer."),
    FactDefinition("ESXI_PORT902_UNREACHABLE", "vmware", "NEGATIVE", "Target cluster cannot connect to ESXi host on TCP 902."),
    FactDefinition("NFC_TIMEOUT", "vmware", "NEGATIVE", "Network File Copy (NFC) session to ESXi timed out."),
    FactDefinition("VCENTER_PORT443_REACHABLE", "vmware", "POSITIVE", "vCenter endpoint reachable on TCP 443 with valid SSL."),
    FactDefinition("VCENTER_PORT443_UNREACHABLE", "vmware", "NEGATIVE", "vCenter endpoint unreachable on TCP 443."),
    FactDefinition("VCENTER_TIMEOUT", "vmware", "NEGATIVE", "Connection to vCenter endpoint timed out."),
    FactDefinition("THUMBPRINT_MISMATCH", "vmware", "NEGATIVE", "vCenter SSL certificate SHA1 thumbprint does not match Provider spec."),

    # VMware VDDK & Datastore
    FactDefinition("VDDK_ERROR_13", "vmware", "NEGATIVE", "VDDK transport returned Error 13 (Access rights missing)."),
    FactDefinition("VDDK_PERMISSION_DENIED", "vmware", "NEGATIVE", "VDDK permission denied when accessing VMDK."),
    FactDefinition("ACCESS_RIGHTS_MISSING", "vmware", "NEGATIVE", "VMware user lacks required cryptographic or disk privileges."),
    FactDefinition("VDDK_PERMISSIONS_GRANTED", "vmware", "POSITIVE", "VMware user verified to have required disk and snapshot access rights."),
    FactDefinition("SOURCE_DISK_ACCESSIBLE", "vmware", "POSITIVE", "Source VMDK disk accessible via VDDK transport."),
    FactDefinition("VDDK_CONNECT_FAILED", "vmware", "NEGATIVE", "Failed to connect to VDDK data source container."),
    FactDefinition("DATA_SOURCE_UNAVAILABLE", "vmware", "NEGATIVE", "VDDK data source service is unavailable."),
    FactDefinition("NBD_EXPORT_MISSING", "vmware", "NEGATIVE", "NBD server has no export named for the target disk."),
    FactDefinition("NBD_EXPORT_ACTIVE", "vmware", "POSITIVE", "NBD server export is active and serving data."),
    FactDefinition("VDDK_DATA_SOURCE_READY", "vmware", "POSITIVE", "VDDK container image and data source are ready."),
    FactDefinition("VMDK_NOT_FOUND", "vmware", "NEGATIVE", "VMDK file not found on datastore."),
    FactDefinition("VMDK_URL_NOT_FOUND", "vmware", "NEGATIVE", "Datastore HTTP/NFC URL for VMDK returned 404 Not Found."),
    FactDefinition("VMDK_PATH_VALID", "vmware", "POSITIVE", "VMDK path and flat-vmdk file existence verified on datastore."),
    FactDefinition("CREDENTIALS_UNAUTHORIZED", "vmware", "NEGATIVE", "VMware provider authentication failed (HTTP 401 Unauthorized)."),
    FactDefinition("HTTP_401", "vmware", "NEGATIVE", "Provider endpoint returned HTTP 401 Unauthorized."),
    FactDefinition("CREDENTIALS_VALIDATED", "vmware", "POSITIVE", "VMware provider credentials validated successfully."),

    # Guest OS - Windows
    FactDefinition("VSS_FAILED", "guest_os", "NEGATIVE", "Windows Volume Shadow Copy Service failed during snapshot."),
    FactDefinition("VSS_UNAVAILABLE", "guest_os", "NEGATIVE", "Windows VSS service unavailable or stopped."),
    FactDefinition("VSS_SERVICE_STOPPED", "guest_os", "NEGATIVE", "Windows VSS service is in stopped state."),
    FactDefinition("VSS_SERVICE_RUNNING", "guest_os", "POSITIVE", "Windows VSS service is running and healthy."),
    FactDefinition("SNAPSHOT_FAILED", "guest_os", "NEGATIVE", "VMware snapshot operation failed."),
    FactDefinition("HIVEX_ERROR", "guest_os", "NEGATIVE", "virt-v2v thrown unhandled exception in Hivex registry parser."),
    FactDefinition("REGISTRY_CORRUPTED", "guest_os", "NEGATIVE", "Windows registry hive corrupted or unreadable."),
    FactDefinition("REGISTRY_HIVE_VALID", "guest_os", "POSITIVE", "Windows registry hives verified intact and parseable."),
    FactDefinition("INSPECT_OS_FAILED", "guest_os", "NEGATIVE", "virt-v2v inspect_os failed to determine guest OS."),
    FactDefinition("GUEST_FS_CLEAN", "guest_os", "POSITIVE", "Guest filesystem marked clean."),
    FactDefinition("BITLOCKER_ENABLED", "guest_os", "NEGATIVE", "Windows BitLocker drive encryption is enabled on source disk."),
    FactDefinition("BITLK_UNSUPPORTED_METADATA", "guest_os", "NEGATIVE", "Unexpected BitLocker Volume Master Key metadata version."),
    FactDefinition("BITLK_DEVICE_INVALID", "guest_os", "NEGATIVE", "Device is not a valid BITLK device."),
    FactDefinition("BITLOCKER_DECRYPTED", "guest_os", "POSITIVE", "BitLocker encryption removed or volume fully unlocked."),
    FactDefinition("NTFS_DIRTY_OR_READONLY", "guest_os", "NEGATIVE", "Windows NTFS filesystem is dirty or mounted read-only."),
    FactDefinition("FILESYSTEM_READONLY", "guest_os", "NEGATIVE", "Guest filesystem is read-only, preventing driver injection."),
    FactDefinition("NTFS_FILESYSTEM_CLEAN", "guest_os", "POSITIVE", "Windows NTFS filesystem verified clean and read-write."),
    FactDefinition("INACCESSIBLE_BOOT_DEVICE", "guest_os", "NEGATIVE", "Windows failed to boot with INACCESSIBLE_BOOT_DEVICE."),
    FactDefinition("BUGCHECK_0X7B", "guest_os", "NEGATIVE", "Windows bugcheck 0x0000007B (missing storage driver)."),
    FactDefinition("VIRTIO_DRIVERS_MISSING", "guest_os", "NEGATIVE", "Windows guest missing VirtIO SCSI/block drivers."),
    FactDefinition("VIRTIO_DRIVERS_INSTALLED", "guest_os", "POSITIVE", "VirtIO drivers installed and present in guest OS."),

    # Guest OS - Linux
    FactDefinition("BTRFS_DETECTED", "guest_os", "NEGATIVE", "Btrfs filesystem detected on source Linux VM."),
    FactDefinition("BTRFS_UNSUPPORTED", "guest_os", "NEGATIVE", "virt-v2v does not support Btrfs filesystem conversion."),
    FactDefinition("FILESYSTEM_UNSUPPORTED", "guest_os", "NEGATIVE", "Source VM filesystem type is unsupported by virt-v2v."),
    FactDefinition("SUPPORTED_FILESYSTEM", "guest_os", "POSITIVE", "Source filesystems verified supported (e.g. ext4, xfs)."),

    # Conversion & Pod
    FactDefinition("CDROM_CONFIG_INVALID", "conversion", "NEGATIVE", "VMX contains disconnected or invalid CD-ROM device."),
    FactDefinition("VMX_ENTRY_INVALID", "conversion", "NEGATIVE", "VMX configuration entry is invalid."),
    FactDefinition("CDROM_VMX_CLEARED", "conversion", "POSITIVE", "Invalid CD-ROM device entries removed from VMX."),
    FactDefinition("VIRT_V2V_OOM", "conversion", "NEGATIVE", "virt-v2v conversion pod terminated due to Out Of Memory."),
    FactDefinition("OOM_KILLED", "conversion", "NEGATIVE", "Conversion container killed by Linux OOM killer."),
    FactDefinition("EXIT_CODE_137", "conversion", "NEGATIVE", "Pod exited with exit code 137 (SIGKILL / OOM)."),
    FactDefinition("CONVERSION_POD_MEMORY_INCREASED", "conversion", "POSITIVE", "Conversion pod memory requests and limits increased."),
    FactDefinition("ARG_LIST_TOO_LONG", "conversion", "NEGATIVE", "Image conversion failed with Argument list too long (E2BIG)."),
    FactDefinition("E2BIG", "conversion", "NEGATIVE", "System call returned E2BIG."),

    # OCV / KubeVirt
    FactDefinition("FIRMWARE_CONFLICT", "ocv", "NEGATIVE", "KubeVirt bootloader has both EFI and BIOS configured."),
    FactDefinition("BOOTLOADER_MUTUALLY_EXCLUSIVE", "ocv", "NEGATIVE", "EFI and BIOS bootloaders are mutually exclusive."),
    FactDefinition("ADMISSION_DENIED", "ocv", "NEGATIVE", "KubeVirt admission webhook denied VirtualMachine spec."),
    FactDefinition("FIRMWARE_CONFIG_VALID", "ocv", "POSITIVE", "VM bootloader firmware spec reconciled to single mode."),

    # General / Migration
    FactDefinition("FAILED", "mtv", "NEGATIVE", "Migration plan execution failed."),
]


class FactRegistry:
    def __init__(self, definitions: Optional[Iterable[FactDefinition]] = None):
        self._facts: Dict[str, FactDefinition] = {}
        for d in definitions or _FACT_DEFINITIONS:
            self._facts[d.code] = d

    def register(self, definition: FactDefinition) -> None:
        self._facts[definition.code] = definition

    def get(self, code: str) -> Optional[FactDefinition]:
        return self._facts.get(code)

    def is_valid(self, code: str) -> bool:
        return code in self._facts

    def validate_all(self, codes: Iterable[str]) -> List[str]:
        return [c for c in codes if c not in self._facts]

    def all_facts(self) -> Dict[str, FactDefinition]:
        return dict(self._facts)


FACT_REGISTRY = FactRegistry()
FACT_CATALOG = FACT_REGISTRY


def is_valid_fact(code: str) -> bool:
    return FACT_REGISTRY.is_valid(code)


def get_fact(code: str) -> Optional[FactDefinition]:
    return FACT_REGISTRY.get(code)
