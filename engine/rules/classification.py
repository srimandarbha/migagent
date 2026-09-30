FAILURE_CODE_RULES = {
    "storage.csi.provisioning_timeout": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.98),
    "storage.csi.provisioning-timeout": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.98),
    "network.nad.missing": ("NETWORK.NAD.MISSING", 0.98),
    "vmware.cbt": ("VMWARE.CBT", 0.95),
    "vmware.cbt.failure": ("VMWARE.CBT", 0.95),
    "vmware.guest.vss": ("VMWARE.GUEST.VSS", 0.95),
}

SCENARIO_RULES = {
    "storage-csi-timeout": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.90),
    "storage-csi-backend-healthy": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.90),
    "storage-csi-controller-healthy": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.90),
    "storage-csi-controller-error": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.90),
    "storage-csi-conflict": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.90),
    "network-nad-missing": ("NETWORK.NAD.MISSING", 0.90),
    "vmware-cbt-retry": ("VMWARE.CBT", 0.90),
    "vmware-cbt-failure": ("VMWARE.CBT", 0.90),
    "guest-vss-failure": ("VMWARE.GUEST.VSS", 0.85),
    "insufficient-evidence": ("UNKNOWN", 0.40),
    "capability-error": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.90),
    "unknown": ("UNKNOWN", 0.20),
}

def classify(event):
    # Explicit failure_code is the strongest deterministic classification signal.
    raw = str(event.get("failure_code", "")).strip().lower()
    if raw in FAILURE_CODE_RULES:
        return FAILURE_CODE_RULES[raw]
    scenario = str(event.get("scenario", "")).strip()
    if scenario in SCENARIO_RULES:
        return SCENARIO_RULES[scenario]
    return "UNKNOWN", 0.1
