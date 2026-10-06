"""Configurable SPL (Search Processing Language) Query Catalog for Splunk MCP.

In the DMZ deployment, VMware and direct hardware APIs are unavailable. All container
logs (virt-v2v, forklift-controller, cdi-importer, virt-launcher), Kubernetes events,
and forwarded syslog flow centrally into Splunk.

Queries here are parameterized templates that can be replaced or tuned for the DMZ environment.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional


def sanitize_identifier(val: Any) -> str:
    """Sanitizes an identifier parameter to prevent SPL injection."""
    if val is None:
        return "*"
    cleaned = re.sub(r'[^a-zA-Z0-9_\-\.:*]', '', str(val))
    return cleaned if cleaned else "*"


def sanitize_index(val: Any) -> str:
    """Sanitizes index names."""
    if val is None:
        return ""
    return re.sub(r'[^a-zA-Z0-9_\-]', '', str(val))


def sanitize_search_terms(val: Any) -> str:
    """Sanitizes freeform search terms, stripping SPL pipe, quote, and eval characters."""
    if val is None:
        return ""
    return re.sub(r'[^a-zA-Z0-9_\-\.:* ]', '', str(val)).strip()


# Configurable SPL templates for DMZ Splunk MCP queries
DEFAULT_SPL_TEMPLATES: Dict[str, str] = {
    # 1. Cluster Health & Infrastructure Warnings
    "cluster_health_events": (
        'index={k8s_events_index} sourcetype=kubernetes:events cluster_id="{cluster_id}" '
        '(type="Warning" OR reason="NodeNotReady" OR reason="SystemOOM" OR reason="Evicted") '
        '| table _time, cluster_id, reason, message, involvedObject.kind, involvedObject.name '
        '| head 500'
    ),

    # 2. OpenShift Virtualization: Storage & CSI Provisioning Errors
    "storage_csi_errors": (
        'index={k8s_events_index} sourcetype=kubernetes:events cluster_id="{cluster_id}" '
        '(reason="ProvisioningFailed" OR reason="FailedMount" OR reason="FailedAttachVolume" OR "csi.volume.kubernetes.io") '
        '| table _time, cluster_id, reason, message, involvedObject.name '
        '| head 500'
    ),

    # 3. MTV / Forklift: Virt-v2v Conversion Pod Logs
    "virt_v2v_logs": (
        'index={containers_index} namespace=openshift-mtv (pod_name="*virt-v2v*" OR container_name="virt-v2v") '
        'cluster_id="{cluster_id}" (vm_id="{vm_id}" OR migration_id="{migration_id}" OR "error" OR "fatal" OR "timeout" OR "OOM") '
        '| table _time, pod_name, log_level, message '
        '| head 500'
    ),

    # 4. MTV / Forklift: Controller Step Execution & Migration Plan Logs
    "forklift_controller_logs": (
        'index={containers_index} namespace=openshift-mtv (pod_name="*forklift-controller*" OR container_name="controller") '
        'cluster_id="{cluster_id}" (migration_id="{migration_id}" OR vm_id="{vm_id}") '
        '| table _time, pod_name, log_level, message '
        '| head 500'
    ),

    # 5. OpenShift Virtualization: CDI Importer Pod Logs
    "cdi_importer_logs": (
        'index={containers_index} namespace=openshift-cnv pod_name="*importer*" '
        'cluster_id="{cluster_id}" (error OR fatal OR "qemu-img: error" OR "SSL" OR "Connection refused") '
        '| table _time, pod_name, message '
        '| head 500'
    ),

    # 6. VMware / ESXi Syslog (Forwarded Centrally to Splunk)
    "vmware_syslog_logs": (
        'index={vmware_syslog_index} cluster_id="{cluster_id}" (vm_id="{vm_id}" OR "{vm_name}") '
        '("VixDiskLib" OR "NBD_ERR" OR "Error 13" OR "CBT" OR "Snapshot" OR "Connection reset by peer") '
        '| table _time, host, process, message '
        '| head 500'
    ),

    # 7. Guest OS & FileSystem Errors (Forwarded to Splunk)
    "guest_os_events": (
        'index={containers_index} pod_name="*virt-v2v*" '
        '("BitLocker" OR "VSS" OR "btrfs" OR "NTFS" OR "hibernation" OR "VirtIO") '
        '| table _time, pod_name, message '
        '| head 500'
    ),

    # 8. Network & NAD State
    "network_nad_events": (
        'index={k8s_events_index} sourcetype=kubernetes:events cluster_id="{cluster_id}" '
        '(reason="NetworkAttachmentNotFound" OR "NetworkAttachmentDefinition" OR "CNI failed") '
        '| table _time, cluster_id, reason, message '
        '| head 500'
    ),

    # 9. MTV & OCV Metrics Queried via Splunk (when metrics are streamed to Splunk)
    "splunk_mtv_transfer_rate": (
        '| mstats rate(mtv_workload_migration_disk_transfer_bytes_total) as transfer_rate '
        'WHERE index={metrics_index} migration_id="{migration_id}" span=5m'
    ),
    "splunk_mtv_migration_status": (
        '| mstats latest(cluster:mtv_migrations_status_total:sum) as count '
        'WHERE index={metrics_index} by status'
    ),
    "splunk_cdi_restarts": (
        '| mstats max(kubevirt_cdi_import_pods_high_restart) as restarts '
        'WHERE index={metrics_index}'
    ),
}


class SplunkQueryCatalog:
    """Manages SPL query templates with environment defaults and overrides."""

    def __init__(self, templates: Optional[Dict[str, str]] = None):
        self._templates: Dict[str, str] = dict(DEFAULT_SPL_TEMPLATES)
        if templates:
            self._templates.update(templates)
        self.default_indices = {
            "k8s_events_index": "k8s_events",
            "containers_index": "ocv_containers",
            "vmware_syslog_index": "vmware_syslog",
            "metrics_index": "metrics",
        }

    def override_template(self, name: str, spl: str) -> None:
        """Allow operators to customize or replace SPL queries for the DMZ."""
        self._templates[name] = spl

    def build_query(self, query_name: str, params: Dict[str, Any]) -> str:
        """Interpolates parameters into the SPL template with parameter sanitization."""
        template = self._templates.get(query_name)
        if not template:
            # Fallback to general container search with table projection and head limit
            template = (
                'index={containers_index} cluster_id="{cluster_id}" '
                '(migration_id="{migration_id}" OR vm_id="{vm_id}") '
                '| table _time, pod_name, message | head 500'
            )

        format_dict = {
            "k8s_events_index": sanitize_index(self.default_indices.get("k8s_events_index")),
            "containers_index": sanitize_index(self.default_indices.get("containers_index")),
            "vmware_syslog_index": sanitize_index(self.default_indices.get("vmware_syslog_index")),
            "metrics_index": sanitize_index(self.default_indices.get("metrics_index")),
            "cluster_id": sanitize_identifier(params.get("cluster_id")),
            "vm_id": sanitize_identifier(params.get("vm_id")),
            "vm_name": sanitize_identifier(params.get("vm_name")),
            "migration_id": sanitize_identifier(params.get("migration_id")),
            "search_terms": sanitize_search_terms(params.get("search_terms")),
        }
        # Add custom scalar parameters with basic sanitization
        for k, v in params.items():
            if k not in format_dict and isinstance(v, (str, int, float)):
                if isinstance(v, (int, float)):
                    format_dict[k] = str(v)
                else:
                    format_dict[k] = sanitize_identifier(v)

        query = template
        for k, v in format_dict.items():
            query = query.replace(f"{{{k}}}", str(v))
        return query


# Global singleton instance for easy replacement
SPL_CATALOG = SplunkQueryCatalog()
