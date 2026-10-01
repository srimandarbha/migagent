#!/usr/bin/env python3
"""Smoke-test PostgreSQL/pgvector RAG retrieval using the configured embedder."""
import argparse, os, requests
from persistence.repository import SRETrackerRepository
from persistence.knowledge import PostgresVectorKnowledgeRepository

def make_query_embedding(base_url, model, prefix="search_query: "):
    def query_embedding(text):
        r=requests.post(base_url.rstrip('/')+'/embeddings',json={'model':model,'input':prefix+text},timeout=120)
        r.raise_for_status()
        return r.json()['data'][0]['embedding']
    return query_embedding

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('query')
    ap.add_argument('--top-k',type=int,default=5)
    ap.add_argument('--lexical',action='store_true')
    ap.add_argument('--base-url',default=os.getenv('EMBEDDING_BASE_URL','http://127.0.0.1:11434/v1'))
    ap.add_argument('--model',default=os.getenv('EMBEDDING_MODEL','nomic-ai/nomic-embed-text-v1.5-GGUF:Q4_K_M'))
    a=ap.parse_args()
    repo=SRETrackerRepository(os.getenv('DATABASE_URL','postgresql://postgres:postgres@127.0.0.1:5432/migration_agent'))
    kr=PostgresVectorKnowledgeRepository(repo, None if a.lexical else make_query_embedding(a.base_url, a.model))
    results=kr.search(a.query,a.top_k)
    docs = results.get('documents', results) if isinstance(results, dict) else results
    for r in docs:
        print(f"[{r.get('score',0):.4f}] {r['title']} | product={r.get('product')} | version={r.get('product_version')} | {r.get('source_url','')}")
        if r.get('applicability'):
            print(f"  Applicability: {r.get('applicability')}")
        print(r.get('content','')[:500]); print()
if __name__=='__main__': main()
