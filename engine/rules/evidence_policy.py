from pathlib import Path
import yaml

POLICY_DIR = Path(__file__).parents[2] / "policies"
POLICY_FILES = {
    "STORAGE.CSI.PROVISIONING_TIMEOUT": "storage.csi.provisioning_timeout.yaml",
    "NETWORK.NAD.MISSING": "network.nad.missing.yaml",
    "VMWARE.CBT": "vmware.cbt.yaml",
    "VMWARE.ESXI.CONNECTIVITY": "vmware.esxi.connectivity.yaml",
    "VMWARE.GUEST.VSS": "vmware.cbt.yaml",
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
