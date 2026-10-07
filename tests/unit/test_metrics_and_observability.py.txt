"""Unit tests for OpenShift Virtualization and MTV metrics catalog and tracker."""
from engine.observability.metrics_catalog import METRICS_CATALOG, MetricComponent, MetricType
from engine.observability.metrics_tracker import MigrationMetricsTracker


def test_metrics_catalog_contains_mtv_and_ocv_metrics():
    assert "cluster:mtv_migrations_status_total:sum" in METRICS_CATALOG
    assert "mtv_workload_migration_disk_transfer_bytes_total" in METRICS_CATALOG
    assert "kubevirt_cdi_import_pods_high_restart" in METRICS_CATALOG
    assert "kube_persistentvolumeclaim_status_phase" in METRICS_CATALOG
    assert "up" in METRICS_CATALOG

    mtv_metric = METRICS_CATALOG["cluster:mtv_migrations_status_total:sum"]
    assert mtv_metric.component == MetricComponent.MTV_FORKLIFT
    assert mtv_metric.metric_type == MetricType.GAUGE
    assert "MIGRATION_RUNNING" in mtv_metric.associated_facts


def test_metrics_tracker_calculates_transfer_rate():
    tracker = MigrationMetricsTracker(migration_id="mig-101", vm_id="vm-db-01")

    # Record two samples 100 seconds apart: 500 MB transferred -> 5 MB/s
    t0 = "2026-10-05T12:00:00Z"
    t1 = "2026-10-05T12:01:40Z"
    tracker.record_sample("mtv_workload_migration_disk_transfer_bytes_total", 1000 * 1024 * 1024, timestamp=t0)
    tracker.record_sample("mtv_workload_migration_disk_transfer_bytes_total", 1500 * 1024 * 1024, timestamp=t1)

    rate = tracker.calculate_transfer_rate_mb_s()
    assert rate == 5.0

    # 10 GB disk, 1.5 GB transferred, 8.5 GB remaining at 5 MB/s = 1740.8 seconds
    est_sec = tracker.estimate_recovery_time_seconds(disk_size_bytes=10 * 1024 * 1024 * 1024)
    assert est_sec > 1700 and est_sec < 1750


def test_metrics_tracker_cluster_health():
    tracker = MigrationMetricsTracker(migration_id="mig-102")

    # Healthy state
    tracker.record_sample("node_memory_MemAvailable_bytes", 16.0 * 1024 * 1024 * 1024)
    tracker.record_sample("kubevirt_cdi_import_pods_high_restart", 0.0)
    tracker.record_sample("kubevirt_cdi_data_volume_failed_total", 0.0)
    health = tracker.evaluate_cluster_health()
    assert health["is_healthy"] is True
    assert len(health["degradations"]) == 0

    # Degraded state: failed DataVolumes, high CDI restarts, and node memory pressure
    tracker.record_sample("node_memory_MemAvailable_bytes", 1.0 * 1024 * 1024 * 1024)
    tracker.record_sample("kubevirt_cdi_import_pods_high_restart", 4.0)
    tracker.record_sample("kubevirt_cdi_data_volume_failed_total", 2.0)
    degraded_health = tracker.evaluate_cluster_health()
    assert degraded_health["is_healthy"] is False
    assert len(degraded_health["degradations"]) == 3
