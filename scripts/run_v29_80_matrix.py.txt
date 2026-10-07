#!/usr/bin/env python3
from pathlib import Path
import argparse,yaml
ROOT=Path(__file__).resolve().parents[1]
CONDITIONS=["NONE","SRE_ONLY","RHOKP_ONLY","BOTH","CONFLICT","INSUFFICIENT"]
# Existing executable mappings are intentionally small. Add only after policy+skill+fixture+expected-output tests exist.
IMPLEMENTED={"storage.csi.provisioning_timeout":"storage-csi-controller-error","network.destination_nad_missing":"network-nad-missing","vmware.cbt.retry_limit":"vmware-cbt-retry"}
def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--status",choices=["all","implemented","not-implemented"],default="all"); args=ap.parse_args()
    corpus=yaml.safe_load((ROOT/"datasets/mtv_80_scenario_corpus.yaml").read_text()); cases=corpus["cases"]
    rows=[]
    for c in cases:
        impl=IMPLEMENTED.get(c["failure_code"])
        for cond in CONDITIONS:
            st="IMPLEMENTED" if impl else "NOT_IMPLEMENTED"
            if args.status=="all" or (args.status=="implemented" and impl) or (args.status=="not-implemented" and not impl): rows.append((c,cond,st,impl))
    print("=== V2.9.0 80-SCENARIO MTV MATRIX PLAN ===")
    print(f"Corpus cases : {len(cases)}")
    print(f"Conditions   : {len(CONDITIONS)}")
    print(f"Planned runs : {len(cases)*len(CONDITIONS)}")
    implemented=sum(1 for c in cases if c["failure_code"] in IMPLEMENTED)
    print(f"Implemented  : {implemented*len(CONDITIONS)}")
    print(f"Not ready    : {(len(cases)-implemented)*len(CONDITIONS)}")
    print(f"Source status: {sum(1 for c in cases if c.get('source_status')=='VERIFIED')}/{len(cases)} VERIFIED")
    print()
    for c,cond,st,impl in rows: print(f"{c['id']} | {cond:<12} | {st:<15} | {c['failure_code']}")
if __name__=="__main__": main()
