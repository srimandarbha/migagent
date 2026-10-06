"""Unit tests for Prometheus exporter and Kubernetes health probes."""
import json
import urllib.request
import pytest

from engine.observability.prometheus_exporter import AgentMetrics, MetricsServer


def test_agent_metrics_collection_and_formatting():
    metrics = AgentMetrics()
    metrics.record_event_processed("COMPLETED", duration_seconds=1.25)
    metrics.record_event_processed("FAILED", duration_seconds=0.50)
    metrics.record_event_processed("DLQ")
    metrics.record_infra_outage()
    metrics.record_circuit_breaker("observability.search")
    metrics.record_diagnosis("VMWARE_CBT_FAILURE", "CONFIRMED")

    formatted = metrics.format_prometheus_metrics()

    assert "mfa_up 1" in formatted
    assert 'mfa_events_processed_total{status="COMPLETED"} 1' in formatted
    assert 'mfa_events_processed_total{status="FAILED"} 1' in formatted
    assert 'mfa_events_processed_total{status="DLQ"} 1' in formatted
    assert "mfa_infrastructure_outages_total 1" in formatted
    assert 'mfa_circuit_breaker_trips_total{capability="observability.search"} 1' in formatted
    assert 'mfa_diagnoses_total{classification="VMWARE_CBT_FAILURE",status="CONFIRMED"} 1' in formatted
    assert "mfa_processing_events_measured_total 2" in formatted


def test_metrics_server_endpoints():
    metrics = AgentMetrics()
    metrics.record_event_processed("COMPLETED", duration_seconds=0.42)

    ready = True
    server = MetricsServer(
        host="127.0.0.1",
        port=0,  # OS assigned port
        ready_check_fn=lambda: ready,
        metrics_collector=metrics,
    )
    assigned_port = server.httpd.server_port
    server.start()

    try:
        # Test /livez
        with urllib.request.urlopen(f"http://127.0.0.1:{assigned_port}/livez") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "alive"

        # Test /readyz (ready=True)
        with urllib.request.urlopen(f"http://127.0.0.1:{assigned_port}/readyz") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "ready"

        # Test /metrics
        with urllib.request.urlopen(f"http://127.0.0.1:{assigned_port}/metrics") as resp:
            assert resp.status == 200
            content = resp.read().decode()
            assert "mfa_up 1" in content
            assert 'mfa_events_processed_total{status="COMPLETED"} 1' in content

        # Test /readyz (ready=False -> 503)
        ready = False
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{assigned_port}/readyz")
            pytest.fail("Should have failed with 503")
        except urllib.error.HTTPError as err:
            assert err.code == 503
            data = json.loads(err.read().decode())
            assert data["status"] == "not_ready"

    finally:
        server.stop()
