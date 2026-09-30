#!/usr/bin/env python3
"""Smoke-test PostgreSQL/pgvector RAG retrieval using the configured embedder."""
import argparse, os, requests
from persistence.repository import SRETrackerRepository
from persistence.knowledge import PostgresVectorKnowledgeRepository

def query_embedding(text):
    base=os.getenv('EMBEDDING_BASE_URL','http://127.0.0.1:8080/v1')
    model=os.getenv('EMBEDDING_MODEL','nomic-embed-text-v1.5')
    prefix=os.getenv('EMBEDDING_QUERY_PREFIX','search_query: ')
    r=requests.post(base.rstrip('/')+'/embeddings',json={'model':model,'input':prefix+text},timeout=120)
    r.raise_for_status()
    return r.json()['data'][0]['embedding']

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('query'); ap.add_argument('--top-k',type=int,default=5); ap.add_argument('--lexical',action='store_true')
    a=ap.parse_args()
    repo=SRETrackerRepository(os.getenv('DATABASE_URL','postgresql://postgres:postgres@127.0.0.1:5432/migration_agent'))
    kr=PostgresVectorKnowledgeRepository(repo, None if a.lexical else query_embedding)
    results=kr.search(a.query,a.top_k)
    for r in results:
        print(f"[{r.get('score',0):.4f}] {r['title']} | {r.get('source_url','')}")
        print(r.get('content','')[:500]); print()
if __name__=='__main__': main()
