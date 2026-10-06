"""Unit tests for DMZ MCP adapters and query catalogs."""
from engine.integrations.dmz.splunk_queries import SplunkQueryCatalog
from engine.integrations.dmz.prometheus_queries import PrometheusQueryCatalog
from engine.integrations.dmz.splunk_mcp import DMZSplunkMCPAdapter
from engine.integrations.dmz.prometheus_mcp import DMZPrometheusMCPAdapter
from engine.integrations.dmz.registry import build_dmz_registry


def test_splunk_query_catalog_formatting_and_override():
    catalog = SplunkQueryCatalog()
    query = catalog.build_query("virt_v2v_logs", {
        "cluster_id": "ocv-prod-01",
        "vm_id": "vm-999",
        "migration_id": "mig-888",
    })
    assert 'cluster_id="ocv-prod-01"' in query
    assert 'vm_id="vm-999"' in query
    assert 'openshift-mtv' in query

    # Test query override for DMZ
    catalog.override_template("virt_v2v_logs", 'index=custom_dmz_logs cluster="{cluster_id}"')
    custom_query = catalog.build_query("virt_v2v_logs", {"cluster_id": "dmz-c1"})
    assert custom_query == 'index=custom_dmz_logs cluster="dmz-c1"'


def test_prometheus_query_catalog_formatting():
    catalog = PrometheusQueryCatalog()
    promql = catalog.build_query("mtv_transfer_rate", {"migration_id": "mig-555"})
    assert 'migration_id="mig-555"' in promql
    assert "mtv_workload_migration_disk_transfer_bytes_total" in promql


def test_splunk_mcp_adapter_with_mock_client():
    mock_log = "virt-v2v: error: libguestfs error: btrfs root detected on guest"
    def mock_mcp(tool_name, args):
        assert tool_name == "splunk.search"
        assert "query" in args
        return {
            "results": [
                {"_raw": mock_log, "_time": "2026-10-05T12:00:00Z", "pod_name": "virt-v2v-pod"}
            ]
        }

    adapter = DMZSplunkMCPAdapter(mcp_client=mock_mcp)
    result = adapter.search({"domain": "guest_os", "signal": "filesystem_type", "cluster_id": "c1"})

    assert result["status"] == "SUCCESS"
    assert len(result["evidence"]) == 1
    ev = result["evidence"][0]
    assert ev["fact"] == "BTRFS_ROOT_DETECTED"
    assert ev["reliability"] == 0.95
    assert ev["resource"] == "virt-v2v-pod"


def test_prometheus_mcp_adapter_with_mock_client():
    def mock_mcp(tool_name, args):
        assert tool_name == "prometheus.query"
        return {
            "data": {
                "result": [
                    {"metric": {"node": "worker-1"}, "value": [1600000000, "2.0"]}
                ]
            }
        }

    adapter = DMZPrometheusMCPAdapter(mcp_client=mock_mcp)
    result = adapter.query({"domain": "storage", "signal": "backend_health"})

    assert result["status"] == "SUCCESS"
    assert len(result["evidence"]) == 1
    ev = result["evidence"][0]
    assert ev["fact"] == "BACKEND_UNHEALTHY"
    assert ev["value"] == 2.0


def test_build_dmz_registry_capabilities():
    registry = build_dmz_registry()
    assert registry.has("observability.search")
    assert registry.has("metrics.query")
    # Direct VMware access must not be in the registry
    assert not registry.has("vmware.api")


def test_build_dmz_registry_metrics_backend_options():
    # Test Splunk as metrics backend
    called = []
    def mock_splunk(tool_name, args):
        called.append(tool_name)
        return {"results": [{"message": "pending", "pod_name": "p1"}]}

    reg_splunk = build_dmz_registry(splunk_mcp_client=mock_splunk, metrics_backend="splunk")
    res = reg_splunk.invoke("metrics.query", {"domain": "ocv", "signal": "pvc_state"})
    assert "splunk.search" in called
    assert res["status"] == "SUCCESS"

    # Test Dual mode (Prometheus primary with Splunk fallback)
    def mock_prom_empty(tool_name, args):
        return {"data": {"result": []}}  # Empty Prometheus result

    called.clear()
    reg_dual = build_dmz_registry(
        prometheus_mcp_client=mock_prom_empty,
        splunk_mcp_client=mock_splunk,
        metrics_backend="dual",
    )
    res_dual = reg_dual.invoke("metrics.query", {"domain": "ocv", "signal": "pvc_state"})
    # Fell back to Splunk!
    assert "splunk.search" in called
    assert res_dual["status"] == "SUCCESS"


def test_prometheus_pvc_state_does_not_corrupt_backend_health():
    # P0-6: Verify pending PVC evaluates to PVC_PENDING, not BACKEND_UNHEALTHY
    def mock_mcp(tool_name, args):
        assert tool_name == "prometheus.query"
        return {
            "data": {
                "result": [
                    {"metric": {"namespace": "openshift-mtv"}, "value": [1600000000, "3.0"]}
                ]
            }
        }

    adapter = DMZPrometheusMCPAdapter(mcp_client=mock_mcp)
    # Query PVC state
    pvc_result = adapter.query({"domain": "storage", "signal": "pvc_state"})
    assert pvc_result["status"] == "SUCCESS"
    assert pvc_result["evidence"][0]["fact"] == "PVC_PENDING"
    assert pvc_result["evidence"][0]["fact"] != "BACKEND_UNHEALTHY"

    # Query Backend Health with clean status (0.0)
    def mock_clean_backend(tool_name, args):
        return {
            "data": {
                "result": [
                    {"metric": {"cluster": "ceph"}, "value": [1600000000, "0.0"]}
                ]
            }
        }

    adapter_clean = DMZPrometheusMCPAdapter(mcp_client=mock_clean_backend)
    backend_result = adapter_clean.query({"domain": "storage", "signal": "backend_health"})
    assert backend_result["evidence"][0]["fact"] == "BACKEND_HEALTHY"

