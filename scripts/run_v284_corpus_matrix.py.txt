#!/usr/bin/env python3
"""Plan/run the expanded source-backed MTV corpus matrix.

The script is deliberately honest: source-backed corpus cases without a deterministic
classification/policy/skill are reported as NOT_IMPLEMENTED, never as PASS.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import yaml

ROOT=Path(__file__).resolve().parents[1]
CONDITIONS=["NONE","SRE_ONLY","RHOKP_ONLY","BOTH","CONFLICT","INSUFFICIENT"]

# Current v2.8.4 implementation mapping. Expand only when deterministic policy,
# skill, fixture and expected-output assertions are added.
IMPLEMENTED={
    "storage.csi.provisioning_timeout": "storage-csi-controller-error",
    "network.nad.missing": "network-nad-missing",
    "vmware.cbt.retry_limit": "vmware-cbt-retry",
}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--status", choices=["all","implemented","not-implemented"], default="all")
    args=ap.parse_args()
    corpus=yaml.safe_load((ROOT/"datasets/mtv_source_backed_corpus.yaml").read_text())
    cases=corpus["cases"]
    rows=[]
    for case in cases:
        implementation=IMPLEMENTED.get(case["failure_code"])
        for condition in CONDITIONS:
            status="IMPLEMENTED" if implementation else "NOT_IMPLEMENTED"
            row={"case":case["id"],"failure_code":case["failure_code"],"condition":condition,"status":status,"scenario":implementation}
            if args.status=="all" or (args.status=="implemented" and implementation) or (args.status=="not-implemented" and not implementation):
                rows.append(row)
    print("=== V2.8.4 SOURCE-BACKED MTV MATRIX PLAN ===")
    print(f"Corpus cases : {len(cases)}")
    print(f"Conditions   : {len(CONDITIONS)}")
    print(f"Planned runs : {len(cases)*len(CONDITIONS)}")
    print(f"Implemented  : {sum(1 for r in rows if r['status']=='IMPLEMENTED') if args.status=='all' else sum(1 for c in cases if c['failure_code'] in IMPLEMENTED)*len(CONDITIONS)}")
    print(f"Not ready    : {sum(1 for r in rows if r['status']=='NOT_IMPLEMENTED') if args.status=='all' else sum(1 for c in cases if c['failure_code'] not in IMPLEMENTED)*len(CONDITIONS)}")
    print()
    for r in rows:
        print(f"{r['case']} | {r['condition']:<12} | {r['status']:<15} | {r['failure_code']}")

if __name__ == "__main__":
    main()
