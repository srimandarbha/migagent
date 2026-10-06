"""Canonical OpenShift Virtualization (OCV) and Migration Toolkit for Virtualization (MTV) metrics catalog.

Based on Red Hat OpenShift Virtualization, KubeVirt, CDI (Containerized Data Importer),
and Forklift (MTV) official Prometheus metrics documentation.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class MetricComponent(str, Enum):
    MTV_FORKLIFT = "mtv_forklift"
    KUBEVIRT_VMI = "kubevirt_vmi"
    CDI_IMPORTER = "cdi_importer"
    STORAGE_CSI = "storage_csi"
    OCV_CLUSTER = "ocv_cluster"


class MetricType(str, Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


@dataclass(frozen=True)
class MetricDefinition:
    name: str
    component: MetricComponent
    metric_type: MetricType
    description: str
    unit: str
    critical_threshold: Optional[float] = None
    warning_threshold: Optional[float] = None
    associated_facts: Tuple[str, ...] = ()


# Canonical catalog of Red Hat MTV and OpenShift Virtualization Prometheus metrics
METRICS_CATALOG: Dict[str, MetricDefinition] = {
    # --- MTV / Forklift Migration Metrics ---
    "cluster:mtv_migrations_status_total:sum": MetricDefinition(
        name="cluster:mtv_migrations_status_total:sum",
        component=MetricComponent.MTV_FORKLIFT,
        metric_type=MetricType.GAUGE,
        description="Total number of MTV VM migrations by status (Running, Succeeded, Failed, Critical).",
        unit="count",
        associated_facts=("MIGRATION_RUNNING", "MIGRATION_FAILED"),
    ),
    "forklift_migrations_status_total": MetricDefinition(
        name="forklift_migrations_status_total",
        component=MetricComponent.MTV_FORKLIFT,
        metric_type=MetricType.COUNTER,
        description="Forklift VM migrations partitioned by provider, mode, and target.",
        unit="count",
        associated_facts=("MIGRATION_FAILED",),
    ),
    "mtv_workload_migrations": MetricDefinition(
        name="mtv_workload_migrations",
        component=MetricComponent.MTV_FORKLIFT,
        metric_type=MetricType.COUNTER,
        description="MTV migration counter partitioned by status, provider, and target namespace.",
        unit="count",
    ),
    "mtv_workload_migration_pipeline_duration_seconds": MetricDefinition(
        name="mtv_workload_migration_pipeline_duration_seconds",
        component=MetricComponent.MTV_FORKLIFT,
        metric_type=MetricType.HISTOGRAM,
        description="Duration of migration pipeline phases (DiskTransfer, ImageConversion, Cutover).",
        unit="seconds",
        warning_threshold=3600.0,
        critical_threshold=7200.0,
        associated_facts=("PIPELINE_PHASE_STALLED",),
    ),
    "mtv_workload_migration_disk_transfer_duration_seconds": MetricDefinition(
        name="mtv_workload_migration_disk_transfer_duration_seconds",
        component=MetricComponent.MTV_FORKLIFT,
        metric_type=MetricType.GAUGE,
        description="Elapsed time spent copying disk data for the migration.",
        unit="seconds",
    ),
    "mtv_workload_migration_disk_transfer_bytes_total": MetricDefinition(
        name="mtv_workload_migration_disk_transfer_bytes_total",
        component=MetricComponent.MTV_FORKLIFT,
        metric_type=MetricType.COUNTER,
        description="Total bytes transferred across all disks for the migration.",
        unit="bytes",
    ),
    "vsphere_xcopy_volume_populator_progress": MetricDefinition(
        name="vsphere_xcopy_volume_populator_progress",
        component=MetricComponent.MTV_FORKLIFT,
        metric_type=MetricType.GAUGE,
        description="Progress percentage (0-100) of storage-offloaded volume population.",
        unit="percentage",
    ),
    "vsphere_xcopy_volume_populator_xcopy_used": MetricDefinition(
        name="vsphere_xcopy_volume_populator_xcopy_used",
        component=MetricComponent.MTV_FORKLIFT,
        metric_type=MetricType.GAUGE,
        description="Whether hardware offload (vSphere XCOPY) was utilized (1=true, 0=false).",
        unit="boolean",
    ),

    # --- CDI (Containerized Data Importer) Metrics ---
    "kubevirt_cdi_import_pods_high_restart": MetricDefinition(
        name="kubevirt_cdi_import_pods_high_restart",
        component=MetricComponent.CDI_IMPORTER,
        metric_type=MetricType.GAUGE,
        description="Indicates CDI import pods with anomalous restart counts.",
        unit="count",
        warning_threshold=2.0,
        critical_threshold=5.0,
        associated_facts=("CDI_IMPORT_RESTART_LOOP", "CDI_IMPORTER_CRASH"),
    ),
    "kubevirt_cdi_data_volume_total": MetricDefinition(
        name="kubevirt_cdi_data_volume_total",
        component=MetricComponent.CDI_IMPORTER,
        metric_type=MetricType.GAUGE,
        description="Total number of DataVolumes managed by CDI.",
        unit="count",
    ),
    "kubevirt_cdi_data_volume_pending_total": MetricDefinition(
        name="kubevirt_cdi_data_volume_pending_total",
        component=MetricComponent.CDI_IMPORTER,
        metric_type=MetricType.GAUGE,
        description="Number of DataVolumes stuck in Pending phase.",
        unit="count",
        warning_threshold=1.0,
        critical_threshold=5.0,
        associated_facts=("PVC_PENDING", "DATAVOLUME_PENDING"),
    ),
    "kubevirt_cdi_data_volume_failed_total": MetricDefinition(
        name="kubevirt_cdi_data_volume_failed_total",
        component=MetricComponent.CDI_IMPORTER,
        metric_type=MetricType.GAUGE,
        description="Number of DataVolumes in Failed phase.",
        unit="count",
        critical_threshold=1.0,
        associated_facts=("DATAVOLUME_FAILED", "PVC_PROVISIONING_FAILED"),
    ),

    # --- KubeVirt VMI Metrics ---
    "kubevirt_vmi_phase_count": MetricDefinition(
        name="kubevirt_vmi_phase_count",
        component=MetricComponent.KUBEVIRT_VMI,
        metric_type=MetricType.GAUGE,
        description="VirtualMachineInstance count by phase (Running, Pending, Failed, Scheduling).",
        unit="count",
    ),
    "kubevirt_vmi_storage_iops_read_total": MetricDefinition(
        name="kubevirt_vmi_storage_iops_read_total",
        component=MetricComponent.KUBEVIRT_VMI,
        metric_type=MetricType.COUNTER,
        description="Total read I/O operations for VM disks.",
        unit="operations",
    ),
    "kubevirt_vmi_storage_iops_write_total": MetricDefinition(
        name="kubevirt_vmi_storage_iops_write_total",
        component=MetricComponent.KUBEVIRT_VMI,
        metric_type=MetricType.COUNTER,
        description="Total write I/O operations for VM disks.",
        unit="operations",
    ),
    "kubevirt_vmi_storage_read_traffic_bytes_total": MetricDefinition(
        name="kubevirt_vmi_storage_read_traffic_bytes_total",
        component=MetricComponent.KUBEVIRT_VMI,
        metric_type=MetricType.COUNTER,
        description="Total storage read traffic in bytes.",
        unit="bytes",
    ),
    "kubevirt_vmi_storage_write_traffic_bytes_total": MetricDefinition(
        name="kubevirt_vmi_storage_write_traffic_bytes_total",
        component=MetricComponent.KUBEVIRT_VMI,
        metric_type=MetricType.COUNTER,
        description="Total storage write traffic in bytes.",
        unit="bytes",
    ),
    "kubevirt_vmi_network_receive_packets_dropped_total": MetricDefinition(
        name="kubevirt_vmi_network_receive_packets_dropped_total",
        component=MetricComponent.KUBEVIRT_VMI,
        metric_type=MetricType.COUNTER,
        description="Total inbound network packets dropped.",
        unit="packets",
        warning_threshold=50.0,
        associated_facts=("NETWORK_PACKET_LOSS",),
    ),
    "kubevirt_vmi_memory_available_bytes": MetricDefinition(
        name="kubevirt_vmi_memory_available_bytes",
        component=MetricComponent.KUBEVIRT_VMI,
        metric_type=MetricType.GAUGE,
        description="Available memory in bytes inside the VMI.",
        unit="bytes",
    ),

    # --- OpenShift Cluster & Storage Health Metrics ---
    "up": MetricDefinition(
        name="up",
        component=MetricComponent.OCV_CLUSTER,
        metric_type=MetricType.GAUGE,
        description="Scrape target availability (1=UP, 0=DOWN).",
        unit="boolean",
        critical_threshold=0.5,
        associated_facts=("TARGET_UNREACHABLE",),
    ),
    "kube_node_status_condition": MetricDefinition(
        name="kube_node_status_condition",
        component=MetricComponent.OCV_CLUSTER,
        metric_type=MetricType.GAUGE,
        description="Node status condition (Ready, DiskPressure, MemoryPressure).",
        unit="boolean",
        associated_facts=("NODE_NOT_READY", "NODE_PRESSURE"),
    ),
    "node_memory_MemAvailable_bytes": MetricDefinition(
        name="node_memory_MemAvailable_bytes",
        component=MetricComponent.OCV_CLUSTER,
        metric_type=MetricType.GAUGE,
        description="Free memory available on OpenShift worker node.",
        unit="bytes",
        warning_threshold=2.0 * 1024 * 1024 * 1024,  # 2 GiB
        associated_facts=("NODE_MEMORY_PRESSURE",),
    ),
    "kube_persistentvolumeclaim_status_phase": MetricDefinition(
        name="kube_persistentvolumeclaim_status_phase",
        component=MetricComponent.OCV_CLUSTER,
        metric_type=MetricType.GAUGE,
        description="Phase of PersistentVolumeClaim (Bound, Pending, Lost).",
        unit="boolean",
        associated_facts=("PVC_PENDING", "PVC_BOUND"),
    ),
}
