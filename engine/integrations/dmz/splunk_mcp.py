"""DMZ Splunk MCP Adapter.

Provides the 'observability.search' capability backed by Splunk MCP.
In DMZ environments, VMware and direct hardware access are absent;
container logs, Kubernetes events, and forwarded syslog are queried via Splunk.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .splunk_queries import SPL_CATALOG, SplunkQueryCatalog
from ...rules.facts import FACT_CATALOG


class DMZSplunkMCPAdapter:
    """Adapts Splunk MCP to the 'observability.search' capability contract."""

    def __init__(
        self,
        mcp_client: Optional[Callable[[str, Dict[str, Any]], Dict[str, Any]]] = None,
        query_catalog: Optional[SplunkQueryCatalog] = None,
    ):
        self.mcp_client = mcp_client
        self.query_catalog = query_catalog or SPL_CATALOG
        self._signal_to_template = {
            # Conversion
            ("conversion", "virt_v2v_log"): "virt_v2v_logs",
            # MTV
            ("mtv", "migration_state"): "forklift_controller_logs",
            ("mtv", "transfer_errors"): "virt_v2v_logs",
            ("mtv", "vddk_data_source"): "cdi_importer_logs",
            # OCV
            ("ocv", "pvc_state"): "storage_csi_errors",
            ("ocv", "pvc_events"): "storage_csi_errors",
            ("ocv", "csi_errors"): "storage_csi_errors",
            ("ocv", "csi_controller_errors"): "storage_csi_errors",
            ("ocv", "nad_state"): "network_nad_events",
            ("ocv", "network_events"): "network_nad_events",
            ("ocv", "network_attachment_state"): "network_nad_events",
            ("ocv", "pod_status"): "cluster_health_events",
            ("ocv", "admission_events"): "cluster_health_events",
            # Storage
            ("storage", "backend_health"): "storage_csi_errors",
            ("storage", "disk_resize_error"): "storage_csi_errors",
            # Guest OS
            ("guest_os", "vss_state"): "guest_os_events",
            ("guest_os", "bitlocker_state"): "guest_os_events",
            ("guest_os", "filesystem_type"): "guest_os_events",
            ("guest_os", "filesystem_state"): "guest_os_events",
            ("guest_os", "driver_state"): "guest_os_events",
            ("guest_os", "registry_state"): "guest_os_events",
            ("guest_os", "boot_errors"): "guest_os_events",
            # VMware (Forwarded to Splunk syslog)
            ("vmware", "cbt_state"): "vmware_syslog_logs",
            ("vmware", "vddk_log"): "vmware_syslog_logs",
            ("vmware", "permissions"): "vmware_syslog_logs",
            ("vmware", "esxi_connectivity"): "vmware_syslog_logs",
            ("vmware", "datastore_vmdk"): "vmware_syslog_logs",
            ("vmware", "auth_errors"): "vmware_syslog_logs",
            ("vmware", "snapshot_errors"): "vmware_syslog_logs",
            ("vmware", "vcenter_connectivity"): "vmware_syslog_logs",
        }

    def search(self, params: Dict[str, Any]) -> Dict[str, Any]:
        domain = params.get("domain")
        signal = params.get("signal")
        template_name = self._signal_to_template.get((domain, signal), "virt_v2v_logs")

        spl_query = self.query_catalog.build_query(template_name, params)

        if self.mcp_client:
            raw_result = self.mcp_client("splunk.search", {"query": spl_query, "earliest_time": "-4h"})
            return self._parse_mcp_results(raw_result, domain, signal, spl_query)

        # Default fallback structure when MCP client is connected via tool runtime
        return {
            "status": "SUCCESS",
            "evidence": [],
            "metadata": {
                "generated_spl": spl_query,
                "template": template_name,
                "domain": domain,
                "signal": signal,
            },
        }

    def _parse_mcp_results(
        self,
        raw_result: Dict[str, Any],
        domain: Optional[str],
        signal: Optional[str],
        query: str,
    ) -> Dict[str, Any]:
        events = raw_result.get("results", raw_result.get("events", []))
        evidence_items = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for ev in events:
            message = ev.get("message", ev.get("_raw", ""))
            matched_fact = self._match_fact_from_log(message, domain, signal)
            evidence_items.append({
                "source": "splunk_mcp",
                "fact": matched_fact,
                "status": "SUCCESS",
                "observed_at": ev.get("_time", now_iso),
                "confidence": 0.95,
                "reliability": 0.95,
                "domain": domain,
                "signal": signal,
                "resource": ev.get("involvedObject.name") or ev.get("pod_name") or ev.get("host"),
                "value": message[:200],
                "provenance": {"spl_query": query, "sourcetype": ev.get("sourcetype")},
            })

        return {
            "status": "SUCCESS" if evidence_items else "NO_DATA",
            "evidence": evidence_items,
            "metadata": {"query": query, "matched_count": len(evidence_items)},
        }

    def _match_fact_from_log(self, message: str, domain: Optional[str], signal: Optional[str]) -> str:
        msg_lower = message.lower()
        if "provisioningfailed" in msg_lower or "failed to provision volume" in msg_lower:
            return "PVC_PROVISIONING_FAILED"
        if "pending" in msg_lower:
            return "PVC_PENDING"
        if "backend" in msg_lower and ("unhealthy" in msg_lower or "degraded" in msg_lower):
            return "BACKEND_UNHEALTHY"
        if "cbt" in msg_lower and ("disabled" in msg_lower or "error" in msg_lower):
            return "CBT_DISABLED"
        if "error 13" in msg_lower or "access rights" in msg_lower:
            return "VDDK_PERMISSION_DENIED"
        if "networkattachmentdefinition" in msg_lower and "not found" in msg_lower:
            return "NAD_NOT_FOUND"
        if "bitlocker" in msg_lower:
            return "BITLOCKER_VOLUME_LOCKED"
        if "btrfs" in msg_lower:
            return "BTRFS_ROOT_DETECTED"
        if "vss" in msg_lower and ("fail" in msg_lower or "timeout" in msg_lower):
            return "WINDOWS_VSS_FREEZE_FAILED"
        return "UNKNOWN"
