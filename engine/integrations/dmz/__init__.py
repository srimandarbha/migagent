"""DMZ capability adapters and query catalogs for Prometheus MCP and Splunk MCP."""
from .splunk_queries import SPL_CATALOG, SplunkQueryCatalog, DEFAULT_SPL_TEMPLATES
from .prometheus_queries import PROMQL_CATALOG, PrometheusQueryCatalog, DEFAULT_PROMQL_TEMPLATES
from .splunk_mcp import DMZSplunkMCPAdapter
from .prometheus_mcp import DMZPrometheusMCPAdapter
from .registry import build_dmz_registry

__all__ = [
    "SPL_CATALOG",
    "SplunkQueryCatalog",
    "DEFAULT_SPL_TEMPLATES",
    "PROMQL_CATALOG",
    "PrometheusQueryCatalog",
    "DEFAULT_PROMQL_TEMPLATES",
    "DMZSplunkMCPAdapter",
    "DMZPrometheusMCPAdapter",
    "build_dmz_registry",
]
