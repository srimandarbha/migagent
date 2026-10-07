#!/usr/bin/env python3
from pathlib import Path
from urllib.parse import urlparse
import yaml
ROOT=Path(__file__).resolve().parents[1]
d=yaml.safe_load((ROOT/'datasets/mtv_80_scenario_corpus.yaml').read_text())
cases=d['cases']; errors=[]
if len(cases)!=80: errors.append(f'expected 80 cases, got {len(cases)}')
ids=[c['id'] for c in cases]; codes=[c['failure_code'] for c in cases]
if len(set(ids))!=80: errors.append('duplicate case IDs')
if len(set(codes))!=80: errors.append('duplicate failure codes')
for c in cases:
    for f in ['id','title','category','phase','failure_code','source_id','source_url','source_backed_signature','expected_mechanism','source_status','synthetic_fixture_policy']:
        if not c.get(f): errors.append(f"{c.get('id')}: missing {f}")
    if c.get('source_status')!='VERIFIED': errors.append(f"{c['id']}: source not VERIFIED")
    u=urlparse(c['source_url'])
    if u.netloc!='docs.redhat.com': errors.append(f"{c['id']}: non-Red-Hat source {c['source_url']}")
if d.get('matrix',{}).get('planned_runs') != 480: errors.append('matrix planned_runs != 480')
print(f'cases={len(cases)} unique_ids={len(set(ids))} unique_codes={len(set(codes))} sources={len(d["sources"])}')
print('validation=PASS' if not errors else 'validation=FAIL')
for e in errors: print('ERROR:',e)
raise SystemExit(1 if errors else 0)
