"""Recovery Feasibility and Maintenance Window Reasoning Engine (Phase 5).

Decouples diagnostic readiness from operational feasibility by evaluating:
1. Failure Permanence: TRANSIENT vs ENVIRONMENTAL vs PERMANENT_SPECIFICATION.
2. Operational Feasibility: Disk size and transfer rate vs remaining change window.
3. SLA & Outage Risk: Preventing retries that would overrun customer maintenance windows.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from ..contracts import Readiness, RecoveryOption


class FailurePermanence(str, Enum):
    TRANSIENT = "TRANSIENT"
    ENVIRONMENTAL = "ENVIRONMENTAL"
    PERMANENT_SPECIFICATION = "PERMANENT_SPECIFICATION"


# Taxonomy of failure permanence
PERMANENCE_CLASSIFICATION: Dict[str, FailurePermanence] = {
    # Permanent without configuration/specification change
    "GUEST.WINDOWS.BITLOCKER": FailurePermanence.PERMANENT_SPECIFICATION,
    "GUEST.LINUX.BTRFS": FailurePermanence.PERMANENT_SPECIFICATION,
    "GUEST.LINUX.BTRFS_UNSUPPORTED": FailurePermanence.PERMANENT_SPECIFICATION,
    "CONVERSION.VIRT_V2V.CDROM": FailurePermanence.PERMANENT_SPECIFICATION,
    "OS.WINDOWS.FILESYSTEM_READONLY": FailurePermanence.PERMANENT_SPECIFICATION,
    "DISK.RESIZE_FAILED": FailurePermanence.PERMANENT_SPECIFICATION,
    "OS.WINDOWS.VIRTIO_DRIVERS_MISSING": FailurePermanence.PERMANENT_SPECIFICATION,
    "GUEST.WINDOWS.VIRTIO": FailurePermanence.PERMANENT_SPECIFICATION,
    "OCV.VM.FIRMWARE": FailurePermanence.PERMANENT_SPECIFICATION,
    # Environmental
    "STORAGE.CSI.PROVISIONING_TIMEOUT": FailurePermanence.ENVIRONMENTAL,
    "NETWORK.NAD.MISSING": FailurePermanence.ENVIRONMENTAL,
    "VMWARE.ESXI.CONNECTIVITY": FailurePermanence.ENVIRONMENTAL,
    "VMWARE.CREDENTIALS.UNAUTHORIZED": FailurePermanence.ENVIRONMENTAL,
    "VMWARE.VDDK.PERMISSION": FailurePermanence.ENVIRONMENTAL,
    # Transient
    "VMWARE.CBT": FailurePermanence.TRANSIENT,
    "CONVERSION.VIRT_V2V.OOM": FailurePermanence.TRANSIENT,
}


@dataclass
class FeasibilityEvaluation:
    permanence: FailurePermanence
    is_operationally_feasible: bool
    estimated_recovery_time_seconds: float
    maintenance_window_remaining_seconds: Optional[float]
    recommended_action: str
    feasibility_notes: List[str] = field(default_factory=list)


def evaluate_recovery_feasibility(
    state: Any,
    default_transfer_rate_mb_s: float = 40.0,
) -> FeasibilityEvaluation:
    """Evaluates whether retry or remediation can complete within operational constraints."""
    classification = state.classification or "UNKNOWN"
    permanence = PERMANENCE_CLASSIFICATION.get(classification, FailurePermanence.TRANSIENT)

    event = state.event or {}
    context = getattr(state, "context", {}) or {}

    # 1. Determine disk size and transfer progress
    disk_size_gb = float(event.get("disk_size_gb") or context.get("disk_size_gb") or 100.0)
    transferred_gb = float(event.get("transferred_gb") or context.get("transferred_gb") or 0.0)
    remaining_gb = max(0.0, disk_size_gb - transferred_gb)

    # 2. Determine transfer rate
    rate_mb_s = float(event.get("transfer_rate_mb_s") or context.get("transfer_rate_mb_s") or default_transfer_rate_mb_s)
    if rate_mb_s <= 0.1:
        rate_mb_s = default_transfer_rate_mb_s

    # 3. Calculate estimated time to complete recovery in seconds
    remaining_mb = remaining_gb * 1024.0
    est_seconds = round(remaining_mb / rate_mb_s, 1)

    # 4. Check maintenance window remaining
    window_min = event.get("maintenance_window_remaining_minutes") or context.get("maintenance_window_remaining_minutes")
    window_sec: Optional[float] = None
    if window_min is not None:
        try:
            window_sec = float(window_min) * 60.0
        except (ValueError, TypeError):
            window_sec = None

    notes: List[str] = []
    is_feasible = True
    recommended_action = "RETRY"

    # Evaluation rule A: Permanent failure specification
    if permanence == FailurePermanence.PERMANENT_SPECIFICATION:
        is_feasible = False
        recommended_action = "FIX_FORWARD" if classification in {"OS.WINDOWS.VIRTIO_DRIVERS_MISSING", "CONVERSION.VIRT_V2V.CDROM"} else "ROLLBACK"
        notes.append(
            f"Failure {classification} is PERMANENT_SPECIFICATION. Automated retry will fail without explicit remediation."
        )

    # Evaluation rule B: Maintenance window overrun
    if window_sec is not None and est_seconds > window_sec:
        is_feasible = False
        recommended_action = "ROLLBACK"
        est_min = round(est_seconds / 60.0, 1)
        win_min_val = round(window_sec / 60.0, 1)
        notes.append(
            f"Estimated transfer time ({est_min}m) exceeds remaining change window ({win_min_val}m). "
            f"Retrying risks production downtime beyond authorized maintenance window."
        )

    # Evaluation rule C: Environmental failure
    if permanence == FailurePermanence.ENVIRONMENTAL and is_feasible:
        notes.append("Environmental prerequisite must be verified before triggering retry.")

    return FeasibilityEvaluation(
        permanence=permanence,
        is_operationally_feasible=is_feasible,
        estimated_recovery_time_seconds=est_seconds,
        maintenance_window_remaining_seconds=window_sec,
        recommended_action=recommended_action,
        feasibility_notes=notes,
    )
