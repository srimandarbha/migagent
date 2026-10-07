#!/usr/bin/env python3
"""Migrate an existing knowledge vector column to the configured dimension.

Safe default: embeddings are cleared before a dimension change because vectors
from different embedding spaces must never be mixed. Document/chunk text is kept.
"""
import argparse, os, re
import psycopg

DSN_DEFAULT='postgresql://postgres:postgres@127.0.0.1:5432/migration_agent'

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--dsn',default=os.getenv('DATABASE_URL',DSN_DEFAULT))
    ap.add_argument('--dimension',type=int,default=int(os.getenv('EMBEDDING_DIMENSION','768')))
    ap.add_argument('--force',action='store_true',help='clear existing embeddings before changing dimension')
    a=ap.parse_args()
    if not 1 <= a.dimension <= 4096: raise SystemExit('invalid dimension')
    with psycopg.connect(a.dsn) as conn:
        row=conn.execute("SELECT format_type(a.atttypid,a.atttypmod) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='knowledge' AND c.relname='chunks' AND a.attname='embedding' AND NOT a.attisdropped").fetchone()
        if not row: raise SystemExit('knowledge.chunks.embedding not found; run setup_local_postgres.py first')
        current=row[0]
        m=re.search(r'vector\((\d+)\)',current)
        if not m: raise SystemExit(f'unexpected embedding type: {current}')
        cur=int(m.group(1))
        if cur==a.dimension:
            print(f'embedding dimension already {cur}; nothing to do')
            return
        existing=conn.execute('SELECT count(*) FROM knowledge.chunks WHERE embedding IS NOT NULL').fetchone()[0]
        if existing and not a.force:
            raise SystemExit(f'{existing} embeddings exist at dimension {cur}; rerun with --force to clear them before changing to {a.dimension}')
        conn.execute('UPDATE knowledge.chunks SET embedding=NULL')
        conn.execute('DROP INDEX IF EXISTS knowledge.idx_knowledge_chunks_embedding')
        conn.execute(f'ALTER TABLE knowledge.chunks ALTER COLUMN embedding TYPE vector({a.dimension})')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_knowledge_chunks_embedding ON knowledge.chunks USING hnsw (embedding vector_cosine_ops)')
        conn.commit()
        print(f'migrated knowledge.chunks embedding {cur} -> {a.dimension}; embeddings cleared, text preserved')

if __name__=='__main__': main()
