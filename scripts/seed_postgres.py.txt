#!/usr/bin/env python3
"""Apply schema and seed SRE Tracker + normalized knowledge documents."""
import argparse, json, os
from pathlib import Path
import yaml
import psycopg

DEFAULT_DSN='postgresql://postgres:postgres@127.0.0.1:5432/migration_agent'
ROOT=Path(__file__).resolve().parents[1]
SCHEMA=ROOT/'sql'/'mfa_postgres.sql'

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--dsn',default=os.getenv('DATABASE_URL',DEFAULT_DSN))
    ap.add_argument('--dataset',default=str(ROOT/'datasets'/'common_dataset.yaml'))
    args=ap.parse_args()
    data=yaml.safe_load(Path(args.dataset).read_text(encoding='utf-8'))
    with psycopg.connect(args.dsn) as conn:
        conn.execute(SCHEMA.read_text(encoding='utf-8'))
        for r in data.get('sre_tracker',{}).get('failure_cases',[]):
            conn.execute('''INSERT INTO sre.failure_cases
              (failure_case_id,event_id,migration_id,vm_id,cluster_id,failure_class,failure_code,status,severity,first_seen_at,last_updated_at,agent_version,policy_version)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now(),'fixture-seed','fixture')
              ON CONFLICT (event_id) DO NOTHING''',
              (r['failure_case_id'],r['event_id'],r['migration_id'],r.get('vm_id'),r.get('cluster_id'),r.get('failure_class'),r.get('failure_code'),r.get('status','RETAINED'),'INFO'))
        for d in data.get('redhat_knowledge',[]):
            import uuid
            did=uuid.uuid5(uuid.NAMESPACE_URL,d.get('url',d['id']))
            content=d.get('text','')
            ch=d.get('content_hash') or __import__('hashlib').sha256(content.encode()).hexdigest()
            conn.execute('''INSERT INTO knowledge.documents
              (document_id,source,source_url,product,content_type,title,summary,retrieved_at,content_hash,metadata,content)
              VALUES (%s,%s,%s,%s,%s,%s,%s,now(),%s,%s,%s)
              ON CONFLICT (content_hash) DO UPDATE SET title=EXCLUDED.title,content_type=EXCLUDED.content_type,metadata=EXCLUDED.metadata,content=EXCLUDED.content,updated_at=now()''',
              (did,d.get('source','redhat'),d.get('url'),d.get('product'),d.get('kind','documentation'),d.get('title',d['id']),content[:500],ch,json.dumps({'tags':d.get('tags',[]),'external_id':d.get('id')}),content))
            conn.execute('''INSERT INTO knowledge.chunks(chunk_id,document_id,chunk_index,content,metadata)
              VALUES (%s,%s,0,%s,%s) ON CONFLICT(document_id,chunk_index) DO UPDATE SET content=EXCLUDED.content,metadata=EXCLUDED.metadata''',
              (uuid.uuid5(uuid.NAMESPACE_URL,did.hex+':0'),did,content,json.dumps({'source_url':d.get('url'),'product':d.get('product'),'content_type':d.get('kind')})))
        conn.commit()
    print('PostgreSQL schema applied and SRE Tracker/knowledge dataset seeded.')

if __name__=='__main__': main()
