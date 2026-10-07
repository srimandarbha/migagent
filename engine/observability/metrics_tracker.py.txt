"""Tracks and analyzes MTV and OpenShift Virtualization metrics for migration failures.

Calculates instantaneous and windowed transfer throughput, estimated time
to completion (ETR), and platform health degradation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .metrics_catalog import METRICS_CATALOG, MetricDefinition


@dataclass
class MigrationMetricSample:
    metric_name: str
    value: float
    timestamp: str
    labels: Dict[str, str] = field(default_factory=dict)


class MigrationMetricsTracker:
    def __init__(self, migration_id: Optional[str] = None, vm_id: Optional[str] = None):
        self.migration_id = migration_id
        self.vm_id = vm_id
        self._samples: Dict[str, List[MigrationMetricSample]] = {}

    def record_sample(
        self,
        metric_name: str,
        value: float,
        timestamp: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        if not timestamp:
            timestamp = datetime.now(timezone.utc).isoformat()
        sample = MigrationMetricSample(
            metric_name=metric_name,
            value=float(value),
            timestamp=timestamp,
            labels=labels or {},
        )
        self._samples.setdefault(metric_name, []).append(sample)

    def get_latest_value(self, metric_name: str) -> Optional[float]:
        samples = self._samples.get(metric_name, [])
        return samples[-1].value if samples else None

    def calculate_transfer_rate_mb_s(self) -> Optional[float]:
        """Calculates MTV disk transfer throughput in MB/s from recorded samples."""
        metric_name = "mtv_workload_migration_disk_transfer_bytes_total"
        samples = self._samples.get(metric_name, [])
        if len(samples) < 2:
            return None

        s_first = samples[0]
        s_last = samples[-1]

        try:
            t0 = datetime.fromisoformat(s_first.timestamp.replace("Z", "+00:00")).timestamp()
            t1 = datetime.fromisoformat(s_last.timestamp.replace("Z", "+00:00")).timestamp()
            dt = t1 - t0
            if dt <= 0:
                return None
            bytes_delta = s_last.value - s_first.value
            if bytes_delta < 0:
                bytes_delta = s_last.value  # counter reset
            bytes_per_sec = bytes_delta / dt
            return round(bytes_per_sec / (1024 * 1024), 2)
        except Exception:
            return None

    def estimate_recovery_time_seconds(
        self,
        disk_size_bytes: float,
        transferred_bytes: Optional[float] = None,
        default_rate_mb_s: float = 40.0,
    ) -> float:
        """Estimates time required to transfer remaining disk data."""
        if transferred_bytes is None:
            latest_bytes = self.get_latest_value("mtv_workload_migration_disk_transfer_bytes_total")
            transferred_bytes = latest_bytes if latest_bytes is not None else 0.0

        remaining_bytes = max(0.0, disk_size_bytes - transferred_bytes)
        measured_rate_mb_s = self.calculate_transfer_rate_mb_s()
        effective_rate_mb_s = measured_rate_mb_s if (measured_rate_mb_s and measured_rate_mb_s > 0.1) else default_rate_mb_s

        effective_rate_bytes_s = effective_rate_mb_s * 1024 * 1024
        estimated_seconds = remaining_bytes / effective_rate_bytes_s
        return round(estimated_seconds, 1)

    def evaluate_cluster_health(self) -> Dict[str, Any]:
        """Evaluates OpenShift cluster and workload health from tracked metrics."""
        node_memory = self.get_latest_value("node_memory_MemAvailable_bytes")
        cdi_restarts = self.get_latest_value("kubevirt_cdi_import_pods_high_restart")
        failed_dvs = self.get_latest_value("kubevirt_cdi_data_volume_failed_total")

        degradations = []
        is_healthy = True

        if node_memory is not None and node_memory < 2.0 * 1024 * 1024 * 1024:
            degradations.append(f"Worker node memory low: {round(node_memory / (1024*1024*1024), 2)} GiB available")

        if cdi_restarts is not None and cdi_restarts >= 2.0:
            degradations.append(f"High CDI import pod restart count ({cdi_restarts})")
            is_healthy = False

        if failed_dvs is not None and failed_dvs >= 1.0:
            degradations.append(f"CDI DataVolumes in Failed phase ({failed_dvs})")
            is_healthy = False

        return {
            "is_healthy": is_healthy,
            "degradations": degradations,
            "active_metrics_count": sum(len(v) for v in self._samples.values()),
        }
