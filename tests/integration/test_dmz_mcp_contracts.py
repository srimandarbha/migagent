"""Integration contract tests for DMZ MCP Adapters (Splunk & Prometheus).

Validates:
1. Capability contract conformance against GLOBAL_CONTRACT_REGISTRY.
2. Authority metadata separation (Splunk=HISTORICAL_TELEMETRY, Prometheus=LIVE_TELEMETRY).
3. Typed metric evaluation rules in Prometheus adapter.
4. Fact matching and evidence synthesis in Splunk adapter.
5. Fail-closed behavior on NO_DATA / missing metrics.
"""
import pytest
from engine.integrations.contracts import GLOBAL_CONTRACT_REGISTRY
from engine.integrations.dmz.splunk_mcp import DMZSplunkMCPAdapter
from engine.integrations.dmz.prometheus_mcp import DMZPrometheusMCPAdapter
from engine.integrations.dmz.registry import build_dmz_registry
from engine.contracts import Evidence, EvidenceStatus


def test_dmz_splunk_adapter_contract_and_historical_authority():
    """Verify Splunk MCP adapter satisfies observability.search contract and sets HISTORICAL_TELEMETRY."""
    def mock_mcp(method, args):
        assert method == "splunk.search"
        assert "earliest_time" in args
        return {
            "results": [
                {
                    "_time": "2026-10-06T12:00:00Z",
                    "sourcetype": "kube:container:virt-v2v",
                    "message": "ProvisioningFailed: failed to provision volume with StorageClass csi-ceph",
                    "involvedObject.name": "pvc-migration-vol0",
                }
            ]
        }

    adapter = DMZSplunkMCPAdapter(mcp_client=mock_mcp)
    params = {"domain": "ocv", "signal": "csi_errors"}

    # Validate against GLOBAL_CONTRACT_REGISTRY schema
    is_valid, err = GLOBAL_CONTRACT_REGISTRY.validate("observability.search", params)
    assert is_valid is True, f"Contract schema violation: {err}"

    res = adapter.search(params)
    assert res["status"] == "SUCCESS"
    assert len(res["evidence"]) == 1

    ev_dict = res["evidence"][0]
    assert ev_dict["source"] == "splunk_mcp"
    assert ev_dict["fact"] == "PVC_PROVISIONING_FAILED"
    assert ev_dict["confidence"] == 0.95
    assert ev_dict["provenance"]["authority"] == "HISTORICAL_TELEMETRY"
    assert ev_dict["provenance"]["observation_type"] == "HISTORICAL_EVENT"

    # Verify instantiation of authoritative Evidence model
    ev_model = Evidence(
        id="ev-splunk-01",
        fact=ev_dict["fact"],
        status=EvidenceStatus(ev_dict["status"]),
        source=ev_dict["source"],
        domain=ev_dict["domain"],
        signal=ev_dict["signal"],
        confidence=ev_dict["confidence"],
        reliability=ev_dict["reliability"],
        observed_at=ev_dict["observed_at"],
        metadata=ev_dict,
    )
    assert ev_model.status == EvidenceStatus.SUCCESS


def test_dmz_splunk_adapter_returns_no_data_when_empty():
    """Verify Splunk returns NO_DATA without fabricating facts when no log events are found."""
    def mock_mcp(method, args):
        return {"results": []}

    adapter = DMZSplunkMCPAdapter(mcp_client=mock_mcp)
    res = adapter.search({"domain": "conversion", "signal": "virt_v2v_log"})
    assert res["status"] == "NO_DATA"
    assert res["evidence"] == []


def test_dmz_prometheus_adapter_contract_and_live_authority():
    """Verify Prometheus MCP adapter satisfies metrics.query contract and evaluates typed metrics."""
    def mock_mcp(method, args):
        assert method == "prometheus.query"
        query = args["query"]
        if "ceph_health_status" in query:
            return {
                "data": {
                    "result": [
                        {
                            "metric": {"cluster": "ceph-storage", "vendor": "ceph"},
                            "value": [1728216000, "2"],  # Ceph ERROR
                        }
                    ]
                }
            }
        elif "kube_persistentvolumeclaim" in query:
            return {
                "data": {
                    "result": [
                        {
                            "metric": {"phase": "Pending"},
                            "value": [1728216000, "1"],
                        }
                    ]
                }
            }
        return {"data": {"result": []}}

    adapter = DMZPrometheusMCPAdapter(mcp_client=mock_mcp)

    # 1. Test Ceph degraded metric evaluation
    params_ceph = {"domain": "storage", "signal": "backend_health"}
    is_valid, err = GLOBAL_CONTRACT_REGISTRY.validate("metrics.query", params_ceph)
    assert is_valid is True, f"Contract schema violation: {err}"

    res = adapter.query(params_ceph)
    assert res["status"] == "SUCCESS"
    assert len(res["evidence"]) == 1
    ev_ceph = res["evidence"][0]
    assert ev_ceph["fact"] == "BACKEND_CEPH_DEGRADED"
    assert ev_ceph["provenance"]["authority"] == "LIVE_TELEMETRY"
    assert ev_ceph["provenance"]["observation_type"] == "CURRENT_METRIC"

    # 2. Test PVC Pending state evaluation
    params_pvc = {"domain": "storage", "signal": "pvc_state"}
    res_pvc = adapter.query(params_pvc)
    assert res_pvc["status"] == "SUCCESS"
    assert res_pvc["evidence"][0]["fact"] == "PVC_PENDING"


def test_dmz_prometheus_adapter_healthy_backend():
    """Verify backend_health evaluates to BACKEND_HEALTHY when value is 0."""
    def mock_mcp(method, args):
        return {
            "data": {
                "result": [
                    {
                        "metric": {"cluster": "odf-storage"},
                        "value": [1728216000, "0"],
                    }
                ]
            }
        }

    adapter = DMZPrometheusMCPAdapter(mcp_client=mock_mcp)
    res = adapter.query({"domain": "storage", "signal": "backend_health"})
    assert res["status"] == "SUCCESS"
    assert res["evidence"][0]["fact"] == "BACKEND_HEALTHY"


def test_build_dmz_registry_capabilities_and_dual_fallback():
    """Verify build_dmz_registry registers approved capabilities and respects fallback."""
    prom_calls = []
    splunk_calls = []

    def mock_prom(method, args):
        prom_calls.append(args)
        return {"data": {"result": []}}  # Returns NO_DATA

    def mock_splunk(method, args):
        splunk_calls.append(args)
        return {
            "results": [
                {
                    "_time": "2026-10-06T12:00:00Z",
                    "message": "pvc is pending",
                    "sourcetype": "kube:events",
                }
            ]
        }

    registry = build_dmz_registry(
        prometheus_mcp_client=mock_prom,
        splunk_mcp_client=mock_splunk,
        metrics_backend="dual",
    )

    assert set(registry.list_capabilities()) == {"observability.search", "metrics.query"}

    # Invoke metrics.query with dual backend - Prometheus fails to return data, fall back to Splunk
    result = registry.invoke("metrics.query", {"domain": "storage", "signal": "pvc_state"})
    assert len(prom_calls) == 1
    assert len(splunk_calls) == 1
    assert result["status"] == "SUCCESS"
    assert result["evidence"][0]["fact"] == "PVC_PENDING"
