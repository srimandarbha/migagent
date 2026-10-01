#!/usr/bin/env python3
"""Embed knowledge chunks through an OpenAI-compatible embeddings endpoint.

Designed for llama.cpp, Ollama, or another OpenAI-compatible server. Nomic
retrieval prefixes are applied by default and the vector dimension is checked
against PostgreSQL before any write occurs.
"""
import argparse, os
import requests, psycopg

DEFAULT_DSN="postgresql://postgres:postgres@127.0.0.1:5432/migration_agent"
DEFAULT_BASE_URL="http://127.0.0.1:11434/v1"
DEFAULT_MODEL="nomic-ai/nomic-embed-text-v1.5-GGUF:Q4_K_M"
DEFAULT_DIMENSION=768

def embed(base_url, model, text, prefix="search_document: "):
    url=base_url.rstrip('/')+'/embeddings'
    payload={'model':model,'input':prefix+text if prefix else text}
    r=requests.post(url,json=payload,timeout=120)
    r.raise_for_status()
    data=r.json().get('data')
    if not data or 'embedding' not in data[0]:
        raise RuntimeError('Embedding endpoint returned no embedding')
    return data[0]['embedding']

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--dsn',default=os.getenv('DATABASE_URL',DEFAULT_DSN))
    ap.add_argument('--base-url',default=os.getenv('EMBEDDING_BASE_URL',DEFAULT_BASE_URL))
    ap.add_argument('--model',default=os.getenv('EMBEDDING_MODEL',DEFAULT_MODEL))
    ap.add_argument('--dimension',type=int,default=int(os.getenv('EMBEDDING_DIMENSION',str(DEFAULT_DIMENSION))))
    ap.add_argument('--prefix',default=os.getenv('EMBEDDING_DOCUMENT_PREFIX','search_document: '))
    args=ap.parse_args()
    with psycopg.connect(args.dsn) as conn:
        rows=conn.execute('SELECT chunk_id,content FROM knowledge.chunks WHERE embedding IS NULL ORDER BY chunk_id').fetchall()
        count=0
        for chunk_id,content in rows:
            vec=embed(args.base_url,args.model,content,args.prefix)
            if len(vec)!=args.dimension:
                raise SystemExit(f'Embedding dimension {len(vec)} != configured VECTOR dimension {args.dimension}')
            conn.execute('UPDATE knowledge.chunks SET embedding=%s WHERE chunk_id=%s',(vec,chunk_id))
            count+=1
        conn.commit()
    print(f'embedded {count} chunks using {args.model} ({args.dimension} dimensions)')

if __name__=='__main__': main()
