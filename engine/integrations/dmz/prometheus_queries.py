"""Configurable PromQL Query Catalog for Prometheus MCP in DMZ.

Queries provide authoritative time-series metrics for OpenShift Virtualization,
CDI data volumes, MTV disk transfer rates, and Ceph/ODF storage health.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


DEFAULT_PROMQL_TEMPLATES: Dict[str, str] = {
    # 1. Cluster Health: Node Readiness (1=All Ready, 0=Degraded)
    "cluster_health": 'min(kube_node_status_condition{condition="Ready", status="true"})',

    # 2. Storage Health: PersistentVolumeClaim Status (pending PVC count)
    "storage_backend_health": 'sum(kube_persistentvolumeclaim_status_phase{phase="Pending"})',

    # 3. MTV: Active Migration Status Total by Status
    "mtv_migration_status": "sum by (status) (cluster:mtv_migrations_status_total:sum)",

    # 4. MTV: Disk Transfer Rate (Bytes/Sec over 5m window)
    "mtv_transfer_rate": 'sum(rate(mtv_workload_migration_disk_transfer_bytes_total{migration_id="{migration_id}"}[5m]))',

    # 5. CDI: Importer Pod Restart Rate
    "cdi_importer_restarts": "sum(kubevirt_cdi_import_pods_high_restart)",

    # 6. CDI: Pending DataVolumes
    "cdi_pending_datavolumes": "sum(kubevirt_cdi_data_volume_pending_total)",

    # 7. Cluster: Worker Node Available Memory (Bytes)
    "node_available_memory": 'min(node_memory_MemAvailable_bytes{cluster_id="{cluster_id}"})',

    # 8. Storage: Write IOPS Across Cluster
    "storage_write_iops": "sum(rate(kubevirt_vmi_storage_iops_write_total[5m]))",
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
