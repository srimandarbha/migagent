import re

FAILURE_CODE_RULES = {
    "storage.csi.provisioning_timeout": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.98),
    "storage.csi.provisioning-timeout": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.98),
    "network.nad.missing": ("NETWORK.NAD.MISSING", 0.98),
    "network.destination_nad_missing": ("NETWORK.NAD.MISSING", 0.98),
    "vmware.cbt": ("VMWARE.CBT", 0.95),
    "vmware.cbt.failure": ("VMWARE.CBT", 0.95),
    "vmware.cbt.retry_limit": ("VMWARE.CBT", 0.99),
    "vmware.esxi.port443_unreachable": ("VMWARE.ESXI.CONNECTIVITY", 0.98),
    "vmware.esxi.port902_unreachable": ("VMWARE.ESXI.CONNECTIVITY", 0.98),
    "vmware.vcenter.port443_unreachable": ("VMWARE.VCENTER.CONNECTIVITY", 0.98),
    "vmware.guest.vss": ("GUEST.WINDOWS.VSS", 0.95),
    "guest.windows.vss": ("GUEST.WINDOWS.VSS", 0.95),
    "guest.windows.registry": ("GUEST.WINDOWS.REGISTRY", 0.95),
    "conversion.windows.hivex": ("GUEST.WINDOWS.REGISTRY", 0.95),
    "guest.windows.bitlocker": ("GUEST.WINDOWS.BITLOCKER", 0.95),
    "ocv.vm.firmware": ("OCV.VM.FIRMWARE", 0.95),
    "conversion.firmware_conflict": ("OCV.VM.FIRMWARE", 0.95),
    "guest.linux.btrfs": ("GUEST.LINUX.BTRFS", 0.95),
    "conversion.filesystem.btrfs": ("GUEST.LINUX.BTRFS", 0.95),
    "vmware.vddk.permission": ("VMWARE.VDDK.PERMISSION", 0.95),
    "vmware.vddk.error13": ("VMWARE.VDDK.PERMISSION", 0.95),
    "vmware.vddk.data_source": ("VMWARE.VDDK.DATA_SOURCE", 0.95),
    "vmware.vddk.nbd_export": ("VMWARE.VDDK.DATA_SOURCE", 0.95),
    "conversion.virt_v2v.cdrom": ("CONVERSION.VIRT_V2V.CDROM", 0.98),
    "conversion.virt_v2v.oom": ("CONVERSION.VIRT_V2V.OOM", 0.98),
    "conversion.image_conversion.arg_list": ("CONVERSION.IMAGE_CONVERSION.ARG_LIST", 0.98),
    "conversion.vmdk.url_not_found": ("VMWARE.VMDK.NOT_FOUND", 0.98),
    "conversion.esx.unauthorized": ("VMWARE.CREDENTIALS.UNAUTHORIZED", 0.98),
    "os.windows.filesystem_readonly": ("OS.WINDOWS.FILESYSTEM_READONLY", 0.98),
    "os.windows.virtio_drivers_missing": ("OS.WINDOWS.VIRTIO_DRIVERS_MISSING", 0.98),
    "disk.resize_failed": ("DISK.RESIZE_FAILED", 0.98),
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
    "mtv-009-esxi-port443": ("VMWARE.ESXI.CONNECTIVITY", 0.90),
    "guest-vss-failure": ("GUEST.WINDOWS.VSS", 0.85),
    "windows-vss": ("GUEST.WINDOWS.VSS", 0.90),
    "windows-hivex": ("GUEST.WINDOWS.REGISTRY", 0.90),
    "windows-bitlocker": ("GUEST.WINDOWS.BITLOCKER", 0.90),
    "windows-firmware": ("OCV.VM.FIRMWARE", 0.90),
    "linux-btrfs": ("GUEST.LINUX.BTRFS", 0.90),
    "vmware-vddk-permission": ("VMWARE.VDDK.PERMISSION", 0.90),
    "vmware-vddk-data-source": ("VMWARE.VDDK.DATA_SOURCE", 0.90),
    "insufficient-evidence": ("UNKNOWN", 0.40),
    "capability-error": ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.90),
    "unknown": ("UNKNOWN", 0.20),
}

MESSAGE_PATTERNS = [
    (r"(?:ide\d:\d.*cdrom|cdrom-image.*fileName|invalid.*vmx entry.*cdrom)", ("CONVERSION.VIRT_V2V.CDROM", 0.95)),
    (r"(?:url not found:.*-flat\.vmdk|vcenter: url not found)", ("VMWARE.VMDK.NOT_FOUND", 0.95)),
    (r"(?:vir_from_esx.*http response code 401|esx.*401 unauthorized)", ("VMWARE.CREDENTIALS.UNAUTHORIZED", 0.95)),
    (r"(?:filesystem was mounted read-only|ntfs.*read-only|ntfs.*dirty)", ("OS.WINDOWS.FILESYSTEM_READONLY", 0.95)),
    (r"(?:unable to resize disk image|qemu-img resize failed)", ("DISK.RESIZE_FAILED", 0.95)),
    (r"(?:cbt.*retry limit|cbt snapshot retry)", ("VMWARE.CBT", 0.95)),
    (r"(?:csi.*(?:provisioning\s+)?timeout|pvc.*pending.*datavolume|datavolume.*timed?\s*out)", ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.95)),
    (r"(?:destination network not found|networkattachmentdefinition.*not exist)", ("NETWORK.NAD.MISSING", 0.95)),
    (r"(?:esxi.*port 902|nfc.*timed out)", ("VMWARE.ESXI.CONNECTIVITY", 0.95)),
    (r"(?:vcenter.*port 443|thumbprint mismatch)", ("VMWARE.VCENTER.CONNECTIVITY", 0.95)),
    (r"(?:virt-v2v.*out of memory|virt-v2v.*oom|exit code 137)", ("CONVERSION.VIRT_V2V.OOM", 0.95)),
    (r"(?:argument list too long|e2big)", ("CONVERSION.IMAGE_CONVERSION.ARG_LIST", 0.95)),
    (r"(?:an error occurred while taking a snapshot.*failed to restart the virtual machine|volume shadow copy|vss.*unavailable)", ("GUEST.WINDOWS.VSS", 0.95)),
    (r"(?:hivex\.error|unhandled exception.*hivex|inspect_os.*hivex)", ("GUEST.WINDOWS.REGISTRY", 0.95)),
    (r"(?:not a valid bitlk device|unexpected metadata entry value '24'|volume master key.*bitlk)", ("GUEST.WINDOWS.BITLOCKER", 0.95)),
    (r"(?:bootloader has both efi and bios configured|efi and bios (?:configured, but )?are mutually exclusive)", ("OCV.VM.FIRMWARE", 0.95)),
    (r"(?:unknown filesystem type btrfs|virt-v2v.*btrfs)", ("GUEST.LINUX.BTRFS", 0.95)),
    (r"(?:vixdisklib.*error 13|error 13 \(you do not have access rights)", ("VMWARE.VDDK.PERMISSION", 0.95)),
    (r"(?:server has no export named ''|unable to connect to vddk data source|nbd_connect_uri.*handshake)", ("VMWARE.VDDK.DATA_SOURCE", 0.95)),
    (r"(?:inaccessible_boot_device|0x0000007b|virtio.*drivers.*missing)", ("OS.WINDOWS.VIRTIO_DRIVERS_MISSING", 0.95)),
]

def classify(event):
    # Explicit failure_code is the strongest deterministic classification signal.
    raw = str(event.get("failure_code", "")).strip().lower()
    if raw in FAILURE_CODE_RULES:
        return FAILURE_CODE_RULES[raw]
    if raw.startswith("vmware.cbt"):
        return ("VMWARE.CBT", 0.95)
    if raw.startswith("storage.csi"):
        return ("STORAGE.CSI.PROVISIONING_TIMEOUT", 0.95)
    if raw.startswith("network.nad") or raw.startswith("network.destination_nad"):
        return ("NETWORK.NAD.MISSING", 0.95)
    if raw.startswith("vmware.esxi"):
        return ("VMWARE.ESXI.CONNECTIVITY", 0.95)
    scenario = str(event.get("scenario", "")).strip()
    if scenario in SCENARIO_RULES:
        return SCENARIO_RULES[scenario]
    # Check error message text for authoritative signatures
    message = str(event.get("message", "") or event.get("error", "") or event.get("error_message", "")).strip()
    if message:
        for pattern, result in MESSAGE_PATTERNS:
            if re.search(pattern, message, re.IGNORECASE):
                return result
    return "UNKNOWN", 0.1
