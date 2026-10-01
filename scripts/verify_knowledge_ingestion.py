#!/usr/bin/env python3
"""Verify that Red Hat documents, chunks, and embeddings are actually in PostgreSQL."""
import argparse, os, psycopg

DEFAULT_DSN="postgresql://postgres:postgres@127.0.0.1:5432/migration_agent"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--dsn',default=os.getenv('DATABASE_URL',DEFAULT_DSN))
    args=ap.parse_args()
    with psycopg.connect(args.dsn) as conn:
        checks={
            'documents': "SELECT count(*) FROM knowledge.documents WHERE source='redhat'",
            'chunks': "SELECT count(*) FROM knowledge.chunks c JOIN knowledge.documents d ON d.document_id=c.document_id WHERE d.source='redhat'",
            'embedded_chunks': "SELECT count(*) FROM knowledge.chunks c JOIN knowledge.documents d ON d.document_id=c.document_id WHERE d.source='redhat' AND c.embedding IS NOT NULL",
        }
        for name,sql in checks.items():
            print(f"{name}={conn.execute(sql).fetchone()[0]}")
        print('\nDocuments:')
        rows=conn.execute("""SELECT d.title,d.product,d.product_version,d.source_url,count(c.chunk_id) chunks,count(c.embedding) embedded
          FROM knowledge.documents d LEFT JOIN knowledge.chunks c ON c.document_id=d.document_id
          WHERE d.source='redhat' GROUP BY d.document_id ORDER BY d.updated_at DESC""").fetchall()
        for r in rows:
            print(f"- {r[0]} | product={r[1]} | version={r[2]} | chunks={r[4]} | embedded={r[5]} | {r[3]}")

if __name__=='__main__': main()
