#!/usr/bin/env python3
"""RAG Retrieval Quality and Applicability Benchmark (30 Golden Queries).

Evaluates:
  - Recall@1
  - Recall@3
  - Recall@5
  - MRR (Mean Reciprocal Rank)
  - Version & error compatibility filtering
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engine.integrations.local.postgres import build_postgres_services
from engine.knowledge_compatibility import evaluate_candidate
import requests

def make_query_embedding(base_url: str, model: str, prefix: str = "search_query: "):
    def query_embedding(text: str):
        r = requests.post(base_url.rstrip("/") + "/embeddings", json={"model": model, "input": prefix + text}, timeout=120)
        r.raise_for_status()
        return r.json()["data"][0]["embedding"]
    return query_embedding

GOLDEN_BENCHMARK = [
    {
        "id": "Q01",
        "domain": "VMware/CBT",
        "query": "VMware CBT snapshot retry limit reached during warm migration precopy",
        "expected_doc_id": "RH-DOC-MTV-CBT-RETRY-LIMIT",
        "failure_code": "vmware.cbt.retry_limit",
    },
    {
        "id": "Q02",
        "domain": "VMware/CBT",
        "query": "Warm migration preflight validation failed because Changed Block Tracking is disabled on source VM",
        "expected_doc_id": "RH-DOC-MTV-CBT-DISABLED",
        "failure_code": "vmware.cbt.disabled",
    },
    {
        "id": "Q03",
        "domain": "VMware/Snapshots",
        "query": "Virtual machine snapshot consolidation failed and locked delta VMDK file descriptors on ESXi",
        "expected_doc_id": "RH-DOC-VMWARE-SNAPSHOT-CONSOLIDATION",
        "failure_code": "vmware.snapshot.consolidation_failed",
    },
    {
        "id": "Q04",
        "domain": "VMware/Connectivity",
        "query": "Connection to NFC service on ESXi host port 902 timed out during VDDK disk block transfer",
        "expected_doc_id": "RH-SOL-ESXI-PORT902-CONNECTIVITY",
        "failure_code": "vmware.esxi.port902_timeout",
    },
    {
        "id": "Q05",
        "domain": "VMware/Connectivity",
        "query": "VMware vCenter HTTPS port 443 connection failed with TLS certificate thumbprint mismatch",
        "expected_doc_id": "RH-SOL-VCENTER-PORT443-TLS",
        "failure_code": "vmware.vcenter.port443_unreachable",
    },
    {
        "id": "Q06",
        "domain": "VMware/Connectivity",
        "query": "OpenShift CoreDNS cannot resolve VMware ESXi hostname during migration transfer",
        "expected_doc_id": "RH-SOL-VMWARE-HOST-DNS-RESOLUTION",
        "failure_code": "vmware.esxi.dns_resolution",
    },
    {
        "id": "Q07",
        "domain": "VMware/Permissions",
        "query": "VMware vCenter connection policy denied and insufficient privileges for MTV service account",
        "expected_doc_id": "RH-SOL-VCENTER-PERMISSIONS-POLICY",
        "failure_code": "vmware.vcenter.policy_denied",
    },
    {
        "id": "Q08",
        "domain": "VMware/VDDK",
        "query": "MTV vddk-validator pod permission denied reading VDDK container image files",
        "expected_doc_id": "RH-SOL-VDDK-IMAGE-PERMISSIONS",
        "failure_code": "vmware.vddk.permission_denied",
    },
    {
        "id": "Q09",
        "domain": "VMware/VDDK",
        "query": "ForkliftController settings missing VDDK image required for VMware warm migration",
        "expected_doc_id": "RH-SOL-VDDK-CONFIG-REQUIRED",
        "failure_code": "vmware.vddk.required",
    },
    {
        "id": "Q10",
        "domain": "VMware/VDDK",
        "query": "VDDK data source export missing handshake error and nbdkit server has no export",
        "expected_doc_id": "RH-SOL-VDDK-DATA-SOURCE-EXPORT",
        "failure_code": "vmware.vddk.export_missing",
    },
    {
        "id": "Q11",
        "domain": "Storage/CSI",
        "query": "CSI storage dynamic provisioning timeout and target PVC remains pending during CopyDisks",
        "expected_doc_id": "RH-SOL-STORAGE-CSI-PROVISIONING-TIMEOUT",
        "failure_code": "storage.csi.provisioning_timeout",
    },
    {
        "id": "Q12",
        "domain": "Storage/StorageClass",
        "query": "StorageClass WaitForFirstConsumer volume binding mode stall during AllocateDisks phase",
        "expected_doc_id": "RH-SOL-STORAGECLASS-WAIT-FOR-FIRST-CONSUMER",
        "failure_code": "storage.storageclass.wait_for_first_consumer",
    },
    {
        "id": "Q13",
        "domain": "Storage/HostPath",
        "query": "HostPath volume node affinity conflict prevents CDI importer pod from scheduling",
        "expected_doc_id": "RH-SOL-STORAGE-HOSTPATH-NODE-AFFINITY",
        "failure_code": "storage.hostpath.node_affinity",
    },
    {
        "id": "Q14",
        "domain": "Storage/Disk",
        "query": "Destination PVC block device is smaller than source disk during nbdcopy transfer",
        "expected_doc_id": "RH-SOL-DISK-DESTINATION-SMALLER",
        "failure_code": "disk.destination_smaller_than_source",
    },
    {
        "id": "Q15",
        "domain": "Storage/Quota",
        "query": "CDI importer pod failed because OpenShift target namespace storage ResourceQuota exceeded",
        "expected_doc_id": "RH-SOL-CDI-IMPORTER-STORAGE-QUOTA",
        "failure_code": "storage.quota_exceeded",
    },
    {
        "id": "Q16",
        "domain": "Storage/Ceph",
        "query": "Ceph RBD exclusive volume lock conflict on OpenShift Data Foundation during volume mount",
        "expected_doc_id": "RH-SOL-CEPH-RBD-VOLUME-LOCK",
        "failure_code": "storage.csi.rbd_lock_conflict",
    },
    {
        "id": "Q17",
        "domain": "Network/NAD",
        "query": "Destination NetworkAttachmentDefinition NAD missing in target namespace for Multus secondary network",
        "expected_doc_id": "RH-DOC-NETWORK-DESTINATION-NAD-MISSING",
        "failure_code": "network.destination_nad_missing",
    },
    {
        "id": "Q18",
        "domain": "Network/OVN",
        "query": "Multus error adding container to OVN secondary transfer network for CDI importer pod",
        "expected_doc_id": "RH-SOL-NETWORK-OVN-TRANSFER-NAD",
        "failure_code": "network.ovn_transfer_nad",
    },
    {
        "id": "Q19",
        "domain": "Network/MTU",
        "query": "Network MTU mismatch between ESXi jumbo frames and OpenShift node causing stalled disk transfer",
        "expected_doc_id": "RH-SOL-NETWORK-MTU-MISMATCH",
        "failure_code": "network.mtu_mismatch",
    },
    {
        "id": "Q20",
        "domain": "Network/VLAN",
        "query": "Linux bridge NetworkAttachmentDefinition VLAN tagging mismatch causing VM packet loss",
        "expected_doc_id": "RH-SOL-NETWORK-BRIDGE-VLAN",
        "failure_code": "network.bridge_vlan_mismatch",
    },
    {
        "id": "Q21",
        "domain": "Conversion/virt-v2v",
        "query": "virt-v2v conversion pod terminated by out of memory OOM killer exit code 137",
        "expected_doc_id": "RH-SOL-CONVERSION-VIRT-V2V-OOM",
        "failure_code": "conversion.virt_v2v.oom",
    },
    {
        "id": "Q22",
        "domain": "Conversion/virt-v2v",
        "query": "Image conversion pod fails with argument list too long E2BIG error for multi-disk VM",
        "expected_doc_id": "RH-SOL-CONVERSION-ARGUMENT-LIST-TOO-LONG",
        "failure_code": "conversion.image_conversion.argument_list_too_long",
    },
    {
        "id": "Q23",
        "domain": "Guest/RHEL",
        "query": "virt-v2v cannot update EFI GRUB boot configuration on RHEL guest",
        "expected_doc_id": "RH-SOL-OS-RHEL-GRUB-CONFIG",
        "failure_code": "os.rhel.grub_configuration",
    },
    {
        "id": "Q24",
        "domain": "Guest/RHEL",
        "query": "Augeas parse failure on /etc/fstab empty mount options during virt-v2v inspection",
        "expected_doc_id": "RH-SOL-OS-RHEL-FSTAB-PARSE",
        "failure_code": "os.rhel.fstab_parse",
    },
    {
        "id": "Q25",
        "domain": "Guest/SLES",
        "query": "virt-v2v inspection cannot determine source guest OS on SLES with Btrfs subvolumes",
        "expected_doc_id": "RH-SOL-OS-SLES-INSPECTION",
        "failure_code": "os.sles.inspection_failed",
    },
    {
        "id": "Q26",
        "domain": "Guest/Windows",
        "query": "Windows NTFS filesystem is dirty and mounts read-only preventing VirtIO driver injection",
        "expected_doc_id": "RH-SOL-OS-WINDOWS-FILESYSTEM-READONLY",
        "failure_code": "os.windows.filesystem_readonly",
    },
    {
        "id": "Q27",
        "domain": "Guest/Windows",
        "query": "Windows Volume Shadow Copy Service VSS unavailable during warm migration snapshot quiesce",
        "expected_doc_id": "RH-SOL-OS-WINDOWS-VSS-UNAVAILABLE",
        "failure_code": "os.windows.vss_unavailable",
    },
    {
        "id": "Q28",
        "domain": "Guest/Windows",
        "query": "Migrated Windows VM crashes with BSOD inaccessible boot device missing VirtIO drivers",
        "expected_doc_id": "RH-SOL-OS-WINDOWS-VIRTIO-DRIVERS",
        "failure_code": "os.windows.virtio_drivers_missing",
    },
    {
        "id": "Q29",
        "domain": "Version/MTV212",
        "query": "Migration Toolkit for Virtualization 2.12 Forklift v1beta2 release notes and APIs",
        "expected_doc_id": "RH-DOC-MTV-VERSION-2-12-ONLY",
        "failure_code": "mtv.v2_12.feature_only",
    },
    {
        "id": "Q30",
        "domain": "Version/Unspecified",
        "query": "General architecture and guidelines for MTV migration without version constraints",
        "expected_doc_id": "RH-DOC-GENERIC-UNSPECIFIED-VERSION",
        "failure_code": "mtv.generic.unspecified",
    },
]


def run_benchmark(dsn: str, embed_url: str) -> dict[str, Any]:
    current_env = {
        "cluster_id": "ocv-prod-01",
        "ocp_version": "4.19.23",
        "ocv_version": "4.19.23",
        "mtv_version": "2.11.0",
        "source_provider": "vmware",
        "source_provider_version": "8.0",
    }

    embed_fn = make_query_embedding(embed_url, "nomic-ai/nomic-embed-text-v1.5-GGUF:Q4_K_M")
    _, knowledge = build_postgres_services(dsn, embed=embed_fn)

    total_queries = len(GOLDEN_BENCHMARK)
    r1_count = 0
    r3_count = 0
    r5_count = 0
    mrr_sum = 0.0

    print("=" * 80)
    print(f"RUNNING RAG BENCHMARK: {total_queries} GOLDEN TEST QUERIES")
    print(f"Target Environment: MTV=2.11.0, OCV=4.19.23, vCenter=8.0")
    print("=" * 80)

    results = []

    for item in GOLDEN_BENCHMARK:
        qid = item["id"]
        qtext = item["query"]
        expected_doc = item["expected_doc_id"]
        expected_code = item["failure_code"]

        resp = knowledge.search({"query": qtext, "top_k": 5})
        docs = resp.get("documents", resp) if isinstance(resp, dict) else (resp or [])

        rank = None
        for i, doc in enumerate(docs, 1):
            doc_id = (
                doc.get("id")
                or (doc.get("doc_metadata") or {}).get("external_id")
                or (doc.get("metadata") or {}).get("external_id")
                or ""
            )
            # Check external_id or URL substring
            if expected_doc in doc_id or (doc.get("title") and expected_doc in doc.get("title")):
                rank = i
                break
            # Also check if failure codes match
            candidate_codes = set((doc.get("metadata") or {}).get("failure_codes") or [])
            if (doc.get("metadata") or {}).get("failure_code"):
                candidate_codes.add((doc.get("metadata") or {}).get("failure_code"))
            if expected_code in candidate_codes:
                rank = i
                break

        r1 = rank == 1
        r3 = rank is not None and rank <= 3
        r5 = rank is not None and rank <= 5
        rr = 1.0 / rank if rank is not None and rank <= 5 else 0.0

        if r1:
            r1_count += 1
        if r3:
            r3_count += 1
        if r5:
            r5_count += 1
        mrr_sum += rr

        # Evaluate version applicability of the top retrieved document
        top_doc = docs[0] if docs else {}
        top_decision = evaluate_candidate(current_env, {"failure_code": expected_code}, top_doc) if top_doc else {}

        status_flag = "PASS" if r3 else "WARN"
        print(f"[{status_flag}] {qid} (Rank: {rank if rank else 'N/A'}, RR: {rr:.2f}) | {qtext[:50]}...")
        if docs:
            top_title = top_doc.get("title") or top_doc.get("id")
            top_score = top_doc.get("score", 0.0)
            rec_status = top_decision.get("recommendation_status", "UNKNOWN")
            print(f"      Top-1 [{top_score:.4f}]: {str(top_title)[:60]} | Compat: {rec_status}")

        results.append({
            "id": qid,
            "rank": rank,
            "r1": r1,
            "r3": r3,
            "r5": r5,
            "rr": rr,
            "top_doc": top_doc,
            "top_decision": top_decision,
        })

    recall_1 = r1_count / total_queries
    recall_3 = r3_count / total_queries
    recall_5 = r5_count / total_queries
    mrr = mrr_sum / total_queries

    print("=" * 80)
    print("BENCHMARK SUMMARY")
    print(f"Total Queries: {total_queries}")
    print(f"Recall@1:      {recall_1 * 100:.1f}% ({r1_count}/{total_queries})")
    print(f"Recall@3:      {recall_3 * 100:.1f}% ({r3_count}/{total_queries})")
    print(f"Recall@5:      {recall_5 * 100:.1f}% ({r5_count}/{total_queries})")
    print(f"MRR:           {mrr:.4f}")
    print("=" * 80)

    return {
        "total_queries": total_queries,
        "recall_1": recall_1,
        "recall_3": recall_3,
        "recall_5": recall_5,
        "mrr": mrr,
        "results": results,
    }


def main():
    parser = argparse.ArgumentParser(description="Run 30-query golden RAG benchmark")
    parser.add_argument("--dsn", default=os.getenv("DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:5432/migration_agent"))
    parser.add_argument("--embed-url", default=os.getenv("LOCAL_EMBEDDING_URL", "http://127.0.0.1:11434/v1"))
    args = parser.parse_args()

    metrics = run_benchmark(args.dsn, args.embed_url)
    if metrics["recall_3"] < 0.70 or metrics["mrr"] < 0.70:
        print("ERROR: Benchmark failed minimum retrieval thresholds (Recall@3 >= 70%, MRR >= 0.70)")
        sys.exit(1)
    print("SUCCESS: Benchmark passed retrieval quality gates.")


if __name__ == "__main__":
    main()
