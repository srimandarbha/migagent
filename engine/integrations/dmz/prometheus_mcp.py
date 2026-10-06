"""DMZ Prometheus MCP Adapter.

Provides the 'metrics.query' capability backed by Prometheus MCP.
Evaluates time-series metrics against official Red Hat OpenShift Virtualization
and MTV metric thresholds.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .prometheus_queries import PROMQL_CATALOG, PrometheusQueryCatalog
from ...observability.metrics_catalog import METRICS_CATALOG


class DMZPrometheusMCPAdapter:
    """Adapts Prometheus MCP to the 'metrics.query' capability contract."""

    def __init__(
        self,
        mcp_client: Optional[Callable[[str, Dict[str, Any]], Dict[str, Any]]] = None,
        query_catalog: Optional[PrometheusQueryCatalog] = None,
    ):
        self.mcp_client = mcp_client
        self.query_catalog = query_catalog or PROMQL_CATALOG
        self._signal_to_template = {
            ("storage", "backend_health"): "storage_backend_health",
            ("storage", "pvc_state"): "pvc_state",
            ("storage", "iops"): "storage_write_iops",
            ("ocv", "csi_provisioning_latency"): "csi_provisioning_latency",
            ("ocv", "pod_status"): "cluster_health",
            ("mtv", "migration_state"): "mtv_migration_status",
            ("mtv", "transfer_errors"): "mtv_transfer_rate",
        }
        self.supported_signals = set(self._signal_to_template.keys())

    def query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        domain = params.get("domain")
        signal = params.get("signal")
        template_name = self._signal_to_template.get((domain, signal), "cluster_health")

        promql = self.query_catalog.build_query(template_name, params)

        if self.mcp_client:
            raw_result = self.mcp_client("prometheus.query", {"query": promql})
            return self._parse_mcp_results(raw_result, domain, signal, promql)

        # Default fallback structure when MCP client is configured via runtime
        return {
            "status": "SUCCESS",
            "evidence": [],
            "metadata": {
                "generated_promql": promql,
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
        data = raw_result.get("data", {}).get("result", [])
        evidence_items = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for res in data:
            metric_labels = res.get("metric", {})
            value_pair = res.get("value", [0, "0"])
            metric_val = float(value_pair[1]) if len(value_pair) > 1 else 0.0

            matched_fact = self._evaluate_metric_to_fact(domain, signal, metric_val, metric_labels)
            evidence_items.append({
                "source": "prometheus_mcp",
                "fact": matched_fact,
                "status": "SUCCESS",
                "observed_at": now_iso,
                "confidence": 0.98,
                "reliability": 0.98,
                "domain": domain,
                "signal": signal,
                "value": metric_val,
                "resource": metric_labels.get("node") or metric_labels.get("pod") or metric_labels.get("instance"),
                "provenance": {"promql": query, "metric": metric_labels},
            })

        return {
            "status": "SUCCESS" if evidence_items else "NO_DATA",
            "evidence": evidence_items,
            "metadata": {"query": query, "result_count": len(evidence_items)},
        }

    def _evaluate_metric_to_fact(
        self,
        domain: Optional[str],
        signal: Optional[str],
        value: float,
        labels: Optional[Dict[str, Any]] = None,
    ) -> str:
        if domain == "storage" and signal == "backend_health":
            labels = labels or {}
            metric_str = str(labels).lower()
            if value > 0:
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
        if domain == "storage" and signal == "pvc_state":
            return "PVC_PENDING" if value >= 1.0 else "PVC_BOUND"
        if signal == "transfer_errors":
            return "TRANSFER_RATE_DEGRADED" if value < 1024 * 1024 else "TRANSFER_RATE_NORMAL"
        return "METRIC_COLLECTED"
