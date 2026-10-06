"""Configurable PromQL Query Catalog for Prometheus MCP in DMZ.

Queries provide authoritative time-series metrics for OpenShift Virtualization,
CDI data volumes, MTV disk transfer rates, and Ceph/ODF storage health.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


DEFAULT_PROMQL_TEMPLATES: Dict[str, str] = {
    # 1. Cluster Health: Node Readiness (1=All Ready, 0=Degraded)
    "cluster_health": 'min(kube_node_status_condition{condition="Ready", status="true"})',

    # 2. Storage Health: Dedicated storage backend cluster health (e.g., Ceph/ODF, Pure, Portworx, Trident: 0=HEALTHY, 1=WARN, 2=ERROR)
    # Banned vector(0): Missing metrics MUST return NO_DATA rather than synthesizing a false healthy 0.
    "storage_backend_health": 'max(ceph_health_status) or max(pure_storage_array_status) or max(portworx_cluster_status) or max(netapp_trident_backend_health)',

    # 3. Storage PVC Phase: Pending PVC count (Kubernetes scheduling state, NOT hardware health)
    "pvc_state": 'sum(kube_persistentvolumeclaim_status_phase{phase="Pending"})',

    # 4. MTV: Active Migration Status Total by Status
    "mtv_migration_status": "sum by (status) (cluster:mtv_migrations_status_total:sum)",

    # 5. MTV: Disk Transfer Rate (Bytes/Sec over 5m window)
    "mtv_transfer_rate": 'sum(rate(mtv_workload_migration_disk_transfer_bytes_total{migration_id="{migration_id}"}[5m]))',

    # 6. CDI: Importer Pod Restart Rate
    "cdi_importer_restarts": "sum(kubevirt_cdi_import_pods_high_restart)",

    # 7. CDI: Pending DataVolumes
    "cdi_pending_datavolumes": "sum(kubevirt_cdi_data_volume_pending_total)",

    # 8. Cluster: Worker Node Available Memory (Bytes)
    "node_available_memory": 'min(node_memory_MemAvailable_bytes{cluster_id="{cluster_id}"})',

    # 9. Storage: Write IOPS Across Cluster
    "storage_write_iops": "sum(rate(kubevirt_vmi_storage_iops_write_total[5m]))",

    # 10. CSI Provisioning Latency: CSI Volume Provisioning Duration (Seconds)
    "csi_provisioning_latency": "histogram_quantile(0.95, sum(rate(storage_operation_duration_seconds_bucket{operation_name='volume_create'}[5m])) by (le))",
}


class PrometheusQueryCatalog:
    """Manages PromQL query templates with customization and override capabilities."""

    def __init__(self, templates: Optional[Dict[str, str]] = None):
        self._templates: Dict[str, str] = dict(DEFAULT_PROMQL_TEMPLATES)
        if templates:
            self._templates.update(templates)

    def override_template(self, name: str, promql: str) -> None:
        """Allow operators to customize or replace PromQL queries for the DMZ."""
        self._templates[name] = promql

    def build_query(self, query_name: str, params: Dict[str, Any]) -> str:
        """Interpolates parameters into the PromQL template."""
        template = self._templates.get(query_name)
        if not template:
            template = 'up{job="kubevirt"}'

        format_dict = {
            "cluster_id": params.get("cluster_id", ".*"),
            "migration_id": params.get("migration_id", ".*"),
            "vm_id": params.get("vm_id", ".*"),
        }
        format_dict.update({k: v for k, v in params.items() if isinstance(v, (str, int, float))})

        query = template
        for k, v in format_dict.items():
            query = query.replace(f"{{{k}}}", str(v))
        return query


# Global singleton instance for easy replacement
PROMQL_CATALOG = PrometheusQueryCatalog()


def _eval_storage_backend(val: float, labels: Dict[str, Any]) -> str:
    metric_str = str(labels or {}).lower()
    if val > 0:
        if "dell" in metric_str:
            return "BACKEND_DELL_DEGRADED"
        if "pure" in metric_str:
            return "BACKEND_PURE_DEGRADED"
        if "portworx" in metric_str:
            return "BACKEND_PORTWORX_DEGRADED"
        if "trident" in metric_str or "netapp" in metric_str:
            return "BACKEND_TRIDENT_DEGRADED"
        if "ceph" in metric_str or "odf" in metric_str:
            return "BACKEND_CEPH_DEGRADED"
        return "BACKEND_UNHEALTHY"
    return "BACKEND_HEALTHY"


def _eval_pvc_state(val: float, labels: Dict[str, Any]) -> str:
    return "PVC_PENDING" if val >= 1.0 else "PVC_BOUND"


def _eval_transfer_rate(val: float, labels: Dict[str, Any]) -> str:
    return "TRANSFER_RATE_DEGRADED" if val < 1024 * 1024 else "TRANSFER_RATE_NORMAL"


def _eval_csi_latency(val: float, labels: Dict[str, Any]) -> str:
    if val > 60.0:
        return "CSI_PROVISIONING_TIMEOUT_RISK"
    if val > 15.0:
        return "CSI_PROVISIONING_LATENCY_HIGH"
    return "CSI_PROVISIONING_LATENCY_NORMAL"


def _eval_cluster_health(val: float, labels: Dict[str, Any]) -> str:
    return "CLUSTER_NODES_READY" if val >= 1.0 else "CLUSTER_NODES_DEGRADED"


def _eval_iops(val: float, labels: Dict[str, Any]) -> str:
    return "STORAGE_IOPS_STALLED" if val < 10.0 else "STORAGE_IOPS_ACTIVE"


def _eval_migration_status(val: float, labels: Dict[str, Any]) -> str:
    status_label = str((labels or {}).get("status", "")).lower()
    if "fail" in status_label and val > 0:
        return "MIGRATION_FAILED"
    if "run" in status_label and val > 0:
        return "MIGRATION_RUNNING"
    if "complete" in status_label and val > 0:
        return "MIGRATION_COMPLETED"
    return "MTV_STATUS_RECORDED"


from typing import Tuple, Callable

METRIC_EVALUATION_CATALOG: Dict[Tuple[str, str], Callable[[float, Dict[str, Any]], str]] = {
    ("storage", "backend_health"): _eval_storage_backend,
    ("storage", "pvc_state"): _eval_pvc_state,
    ("storage", "iops"): _eval_iops,
    ("ocv", "csi_provisioning_latency"): _eval_csi_latency,
    ("ocv", "pod_status"): _eval_cluster_health,
    ("mtv", "migration_state"): _eval_migration_status,
    ("mtv", "transfer_errors"): _eval_transfer_rate,
}
