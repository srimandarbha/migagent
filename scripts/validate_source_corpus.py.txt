#!/usr/bin/env python3
from pathlib import Path
import yaml

ROOT=Path(__file__).resolve().parents[1]
corpus=yaml.safe_load((ROOT/'datasets/mtv_source_backed_corpus.yaml').read_text())
items=corpus.get('cases',[])
required={'id','title','category','phase','failure_code','source_id','source_url','source_backed_signature','expected_mechanism'}
assert len(items)==20, f'expected 20 corpus cases, got {len(items)}'
ids=[x['id'] for x in items]
assert len(set(ids))==20, 'duplicate corpus IDs'
for item in items:
    missing=required-set(item)
    assert not missing, f"{item.get('id')}: missing {sorted(missing)}"
    assert item['source_url'].startswith(('https://docs.redhat.com/','https://access.redhat.com/'))
print(f'SOURCE CORPUS VALID: {len(items)} cases')
for item in items:
    print(f"{item['id']} | {item['category']:<12} | {item['failure_code']:<45} | {item['source_id']}")
