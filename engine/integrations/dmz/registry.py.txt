"""DMZ Capability Registry Builder.

Configures capabilities strictly for DMZ environments with only Prometheus MCP
and Splunk MCP available (no direct VMware or hardware API access).
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from ..registry import InMemoryCapabilityRegistry
from .splunk_mcp import DMZSplunkMCPAdapter
from .prometheus_mcp import DMZPrometheusMCPAdapter
from .splunk_queries import SplunkQueryCatalog
from .prometheus_queries import PrometheusQueryCatalog


def build_dmz_registry(
    *,
    splunk_mcp_client: Optional[Callable[[str, Dict[str, Any]], Dict[str, Any]]] = None,
    prometheus_mcp_client: Optional[Callable[[str, Dict[str, Any]], Dict[str, Any]]] = None,
    splunk_query_catalog: Optional[SplunkQueryCatalog] = None,
    prometheus_query_catalog: Optional[PrometheusQueryCatalog] = None,
    metrics_backend: str = "prometheus",
) -> InMemoryCapabilityRegistry:
    """Builds a CapabilityRegistry configured for DMZ operations using only 2 MCPs.

    Args:
        metrics_backend: 'prometheus' (default, queries Prometheus MCP),
                         'splunk' (queries metrics streamed to Splunk), or
                         'dual' (Prometheus primary with Splunk fallback).
    """
    splunk_adapter = DMZSplunkMCPAdapter(
        mcp_client=splunk_mcp_client,
        query_catalog=splunk_query_catalog,
    )
    prom_adapter = DMZPrometheusMCPAdapter(
        mcp_client=prometheus_mcp_client,
        query_catalog=prometheus_query_catalog,
    )

    if metrics_backend == "splunk":
        metrics_handler = splunk_adapter.search
    elif metrics_backend == "dual":
        def dual_metrics_query(params: Dict[str, Any]) -> Dict[str, Any]:
            res = prom_adapter.query(params)
            if res.get("status") == "SUCCESS" and res.get("evidence"):
                return res
            # Fallback to Splunk when Prometheus returns NO_DATA or is unavailable
            return splunk_adapter.search(params)
        metrics_handler = dual_metrics_query
    else:
        metrics_handler = prom_adapter.query

    registry = InMemoryCapabilityRegistry({
        "observability.search": splunk_adapter.search,
        "metrics.query": metrics_handler,
    })
    return registry
