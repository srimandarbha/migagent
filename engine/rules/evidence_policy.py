from pathlib import Path
import yaml

POLICY_DIR = Path(__file__).parents[2] / "policies"
POLICY_FILES = {
    "STORAGE.CSI.PROVISIONING_TIMEOUT": "storage.csi.provisioning_timeout.yaml",
    "NETWORK.NAD.MISSING": "network.nad.missing.yaml",
    "VMWARE.CBT": "vmware.cbt.yaml",
    "VMWARE.ESXI.CONNECTIVITY": "vmware.esxi.connectivity.yaml",
    "VMWARE.VCENTER.CONNECTIVITY": "vmware.vcenter.connectivity.yaml",
    "VMWARE.GUEST.VSS": "guest.windows.vss.yaml",
    "GUEST.WINDOWS.VSS": "guest.windows.vss.yaml",
    "GUEST.WINDOWS.REGISTRY": "guest.windows.registry.yaml",
    "GUEST.WINDOWS.BITLOCKER": "guest.windows.bitlocker.yaml",
    "OCV.VM.FIRMWARE": "ocv.vm.firmware.yaml",
    "GUEST.LINUX.BTRFS": "guest.linux.btrfs.yaml",
    "GUEST.LINUX.BTRFS_UNSUPPORTED": "guest.linux.btrfs.yaml",
    "VMWARE.VDDK.PERMISSION": "vmware.vddk.permission.yaml",
    "VMWARE.VDDK.DATA_SOURCE": "vmware.vddk.data_source.yaml",
    "CONVERSION.VIRT_V2V.CDROM": "conversion.virt_v2v.cdrom.yaml",
    "CONVERSION.VIRT_V2V.OOM": "conversion.virt_v2v.oom.yaml",
    "CONVERSION.IMAGE_CONVERSION.ARG_LIST": "conversion.arg_list.yaml",
    "OS.WINDOWS.FILESYSTEM_READONLY": "os.windows.filesystem_readonly.yaml",
    "DISK.RESIZE_FAILED": "disk.resize_failed.yaml",
    "OS.WINDOWS.VIRTIO_DRIVERS_MISSING": "os.windows.virtio.yaml",
    "GUEST.WINDOWS.VIRTIO": "os.windows.virtio.yaml",
    "GUEST.WINDOWS.VIRTIO_DRIVER": "os.windows.virtio.yaml",
    "VMWARE.VMDK.NOT_FOUND": "vmware.vmdk.not_found.yaml",
    "VMWARE.CREDENTIALS.UNAUTHORIZED": "vmware.credentials.unauthorized.yaml",
    "UNKNOWN": "unknown.yaml",
}

def load_policy(classification: str) -> dict:
    path = POLICY_DIR / POLICY_FILES.get(classification, "unknown.yaml")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def required_for(classification: str) -> list[dict]:
    return load_policy(classification).get("required_evidence", [])

def optional_for(classification: str) -> list[dict]:
    return load_policy(classification).get("optional_evidence", [])

# Backward-compatible exports for existing safety code/tests.
REQUIRED = {k: [x["capability"] + ":" + x["parameters"].get("signal", "") for x in required_for(k)] for k in POLICY_FILES}
OPTIONAL = {k: [x["capability"] + ":" + x["parameters"].get("signal", "") for x in optional_for(k)] for k in POLICY_FILES}


def adaptive_for(classification: str) -> dict:
    return load_policy(classification).get("adaptive_investigation", {})


def retry_for(classification: str) -> dict:
    """Return the classification-specific deterministic retry readiness policy."""
    return load_policy(classification).get("decision_readiness", {}).get("retry", {})


def hypotheses_for(classification: str) -> list[dict]:
    """Return the classification-specific declarative hypotheses from policy."""
    return load_policy(classification).get("hypotheses", [])

