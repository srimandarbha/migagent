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
    estimated_recovery_time_seconds: Optional[float]
    maintenance_window_remaining_seconds: Optional[float]
    recommended_action: str
    feasibility_notes: List[str] = field(default_factory=list)


def evaluate_recovery_feasibility(
    state: Any,
    default_transfer_rate_mb_s: Optional[float] = None,
) -> FeasibilityEvaluation:
    """Evaluates whether retry or remediation can complete within operational constraints."""
    classification = state.classification or "UNKNOWN"
    permanence = PERMANENCE_CLASSIFICATION.get(classification, FailurePermanence.TRANSIENT)

    event = state.event or {}
    context = getattr(state, "context", {}) or {}

    notes: List[str] = []
    is_feasible = True
    recommended_action = "RETRY"

    # 1. Determine disk size and transfer rate telemetry (never fabricate absent telemetry)
    disk_size_raw = event.get("disk_size_gb") or context.get("disk_size_gb")
    rate_mb_s_raw = event.get("transfer_rate_mb_s") or context.get("transfer_rate_mb_s") or default_transfer_rate_mb_s

    est_seconds: Optional[float] = None
    if disk_size_raw is not None and rate_mb_s_raw is not None:
        try:
            disk_size_gb = float(disk_size_raw)
            rate_mb_s = float(rate_mb_s_raw)
            transferred_gb = float(event.get("transferred_gb") or context.get("transferred_gb") or 0.0)
            remaining_gb = max(0.0, disk_size_gb - transferred_gb)
            if rate_mb_s > 0.1:
                remaining_mb = remaining_gb * 1024.0
                est_seconds = round(remaining_mb / rate_mb_s, 1)
            else:
                notes.append("Transfer rate is stalled or near zero; recovery ETA cannot be reliably estimated.")
        except (ValueError, TypeError):
            notes.append("Malformed disk size or transfer rate telemetry.")
    else:
        if disk_size_raw is None and rate_mb_s_raw is None:
            notes.append("Disk size and transfer rate telemetry are absent; recovery time cannot be estimated.")
        elif disk_size_raw is None:
            notes.append("Disk size telemetry is absent; recovery time cannot be estimated.")
        else:
            notes.append("Transfer rate telemetry is absent; recovery time cannot be estimated.")

    # 2. Check maintenance window remaining
    window_min = event.get("maintenance_window_remaining_minutes") or context.get("maintenance_window_remaining_minutes")
    window_sec: Optional[float] = None
    if window_min is not None:
        try:
            window_sec = float(window_min) * 60.0
        except (ValueError, TypeError):
            window_sec = None

    # Evaluation rule A: Permanent failure specification
    if permanence == FailurePermanence.PERMANENT_SPECIFICATION:
        is_feasible = False
        recommended_action = "FIX_FORWARD" if classification in {"OS.WINDOWS.VIRTIO_DRIVERS_MISSING", "CONVERSION.VIRT_V2V.CDROM"} else "ROLLBACK"
        notes.append(
            f"Failure {classification} is PERMANENT_SPECIFICATION. Automated retry will fail without explicit remediation."
        )

    # Evaluation rule B: Maintenance window overrun
    if window_sec is not None:
        if est_seconds is not None:
            if est_seconds > window_sec:
                is_feasible = False
                recommended_action = "ROLLBACK"
                est_min = round(est_seconds / 60.0, 1)
                win_min_val = round(window_sec / 60.0, 1)
                notes.append(
                    f"Estimated transfer time ({est_min}m) exceeds remaining change window ({win_min_val}m). "
                    f"Retrying risks production downtime beyond authorized maintenance window."
                )
        else:
            # When maintenance window is strictly constrained, but recovery duration cannot be calculated
            # due to missing telemetry, fail closed: cannot verify that retry completes within window.
            is_feasible = False
            recommended_action = "MANUAL_ASSESSMENT"
            win_min_val = round(window_sec / 60.0, 1)
            notes.append(
                f"Remaining change window is {win_min_val}m, but recovery ETA cannot be verified due to missing telemetry. "
                f"Automated retry blocked to prevent maintenance window overrun."
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
