"""Prometheus metrics exporter and Kubernetes liveness/readiness server for MFA.

Exposes:
- /metrics: Prometheus text format metrics (events processed, latency, diagnoses, circuit breakers).
- /livez: Kubernetes liveness probe (200 OK if process is running).
- /readyz: Kubernetes readiness probe (200 OK if Kafka consumer and tracker are initialized).
"""
from __future__ import annotations

import http.server
import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

LOG = logging.getLogger("migration-failure-agent.observability.metrics")


class AgentMetrics:
    """Thread-safe in-memory metrics collector with Prometheus exposition format."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events_processed: Dict[str, int] = {
            "COMPLETED": 0,
            "FAILED": 0,
            "DLQ": 0,
            "SKIPPED": 0,
            "INSUFFICIENT_EVIDENCE": 0,
        }
        self.diagnoses_total: Dict[Tuple[str, str], int] = {}
        self.infra_outages_total: int = 0
        self.circuit_breaker_trips: Dict[str, int] = {}
        self.total_duration_seconds: float = 0.0
        self.duration_count: int = 0
        self.start_time: float = time.time()

    def record_event_processed(self, status: str, duration_seconds: Optional[float] = None) -> None:
        with self._lock:
            st = (status or "UNKNOWN").upper()
            self.events_processed[st] = self.events_processed.get(st, 0) + 1
            if duration_seconds is not None and duration_seconds >= 0:
                self.total_duration_seconds += duration_seconds
                self.duration_count += 1

    def record_diagnosis(self, classification: str, status: str) -> None:
        with self._lock:
            key = (classification or "UNKNOWN", status or "UNKNOWN")
            self.diagnoses_total[key] = self.diagnoses_total.get(key, 0) + 1

    def record_infra_outage(self) -> None:
        with self._lock:
            self.infra_outages_total += 1

    def record_circuit_breaker(self, capability: str) -> None:
        with self._lock:
            cap = capability or "unknown"
            self.circuit_breaker_trips[cap] = self.circuit_breaker_trips.get(cap, 0) + 1

    def format_prometheus_metrics(self) -> str:
        lines = [
            "# HELP mfa_up Whether the migration failure agent process is running",
            "# TYPE mfa_up gauge",
            "mfa_up 1",
            "",
            "# HELP mfa_uptime_seconds Process uptime in seconds",
            "# TYPE mfa_uptime_seconds gauge",
            f"mfa_uptime_seconds {max(0.0, time.time() - self.start_time):.1f}",
            "",
            "# HELP mfa_events_processed_total Total migration failure events processed by status",
            "# TYPE mfa_events_processed_total counter",
        ]
        with self._lock:
            for status, count in sorted(self.events_processed.items()):
                lines.append(f'mfa_events_processed_total{{status="{status}"}} {count}')

            lines.extend([
                "",
                "# HELP mfa_processing_duration_seconds_total Total time spent processing events",
                "# TYPE mfa_processing_duration_seconds_total counter",
                f"mfa_processing_duration_seconds_total {self.total_duration_seconds:.4f}",
                "",
                "# HELP mfa_processing_events_measured_total Total events with duration measured",
                "# TYPE mfa_processing_events_measured_total counter",
                f"mfa_processing_events_measured_total {self.duration_count}",
                "",
                "# HELP mfa_infrastructure_outages_total Transient database or service outages observed",
                "# TYPE mfa_infrastructure_outages_total counter",
                f"mfa_infrastructure_outages_total {self.infra_outages_total}",
            ])

            if self.diagnoses_total:
                lines.extend([
                    "",
                    "# HELP mfa_diagnoses_total Diagnoses produced by classification and confidence",
                    "# TYPE mfa_diagnoses_total counter",
                ])
                for (cls_name, diag_status), cnt in sorted(self.diagnoses_total.items()):
                    lines.append(f'mfa_diagnoses_total{{classification="{cls_name}",status="{diag_status}"}} {cnt}')

            if self.circuit_breaker_trips:
                lines.extend([
                    "",
                    "# HELP mfa_circuit_breaker_trips_total Circuit breaker open events by capability",
                    "# TYPE mfa_circuit_breaker_trips_total counter",
                ])
                for cap, cnt in sorted(self.circuit_breaker_trips.items()):
                    lines.append(f'mfa_circuit_breaker_trips_total{{capability="{cap}"}} {cnt}')

        lines.append("")
        return "\n".join(lines)


GLOBAL_METRICS = AgentMetrics()


class _MetricsRequestHandler(http.server.BaseHTTPRequestHandler):
    ready_check_fn: Optional[Callable[[], bool]] = None
    metrics_collector: AgentMetrics = GLOBAL_METRICS

    def do_GET(self) -> None:
        if self.path == "/metrics":
            body = self.metrics_collector.format_prometheus_metrics().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/livez":
            body = json.dumps({"status": "alive"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/readyz":
            is_ready = True
            if self.ready_check_fn:
                try:
                    is_ready = bool(self.ready_check_fn())
                except Exception as exc:
                    LOG.warning("Readiness check exception: %s", exc)
                    is_ready = False
            status_code = 200 if is_ready else 503
            resp = {"status": "ready" if is_ready else "not_ready"}
            body = json.dumps(resp).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        # Don't pollute console logs with high-frequency probe scrapes
        LOG.debug("%s - %s", self.address_string(), format % args)


class MetricsServer:
    """HTTP server running in a daemon thread."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        ready_check_fn: Optional[Callable[[], bool]] = None,
        metrics_collector: Optional[AgentMetrics] = None,
    ) -> None:
        self.host = host
        self.port = port
        handler_cls = type(
            "CustomMetricsRequestHandler",
            (_MetricsRequestHandler,),
            {
                "ready_check_fn": staticmethod(ready_check_fn) if ready_check_fn else None,
                "metrics_collector": metrics_collector or GLOBAL_METRICS,
            },
        )
        self.httpd = http.server.ThreadingHTTPServer((host, port), handler_cls)
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self.httpd.serve_forever, daemon=True, name="mfa-metrics-server")
        self._thread.start()
        LOG.info("Metrics and health server listening on http://%s:%d (/metrics, /livez, /readyz)", self.host, self.port)

    def stop(self) -> None:
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
            LOG.info("Metrics server stopped")


def start_metrics_server(
    port: Optional[int] = None,
    ready_check_fn: Optional[Callable[[], bool]] = None,
) -> Optional[MetricsServer]:
    """Start the metrics server if enabled by environment variables."""
    metrics_port = port or int(os.getenv("MFA_METRICS_PORT", os.getenv("METRICS_PORT", "8080")))
    enabled = os.getenv("METRICS_ENABLED", "true").lower() in ("1", "true", "yes")
    if not enabled:
        LOG.info("Metrics server disabled by METRICS_ENABLED=false")
        return None
    try:
        server = MetricsServer(port=metrics_port, ready_check_fn=ready_check_fn)
        server.start()
        return server
    except Exception as exc:
        LOG.error("Failed to start metrics server on port %d: %s", metrics_port, exc)
        return None
