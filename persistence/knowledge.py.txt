from __future__ import annotations
from typing import Callable, Sequence

class PostgresVectorKnowledgeRepository:
    """PostgreSQL/pgvector knowledge adapter. Embedding generation stays outside the repository."""
    def __init__(self, tracker_repo, embed: Callable[[str], Sequence[float]] | None = None):
        self.tracker=tracker_repo
        self.embed=embed

    @staticmethod
    def _vector_literal(values: Sequence[float]) -> str:
        return '[' + ','.join(str(float(x)) for x in values) + ']'

    def search(self, query: str | dict, top_k: int = 5, product: str | None = None):
        if isinstance(query, dict):
            params = query
            query_str = str(params.get('query') or '')
            top_k = int(params.get('top_k', top_k))
            product = params.get('product', product)
        else:
            query_str = str(query or '')

        with self.tracker.connection() as conn:
            import json
            if self.embed:
                vec=self._vector_literal(self.embed(query_str))
                if product:
                    rows=conn.execute("""SELECT d.document_id,d.source,d.source_url,d.product,d.product_version,d.content_type,d.title,d.metadata AS doc_metadata,c.chunk_id,c.chunk_index,c.content,c.metadata,1-(c.embedding <=> %s::vector) AS score
                      FROM knowledge.chunks c JOIN knowledge.documents d ON d.document_id=c.document_id
                      WHERE c.embedding IS NOT NULL AND d.product=%s ORDER BY c.embedding <=> %s::vector LIMIT %s""", (vec,product,vec,top_k)).fetchall()
                else:
                    rows=conn.execute("""SELECT d.document_id,d.source,d.source_url,d.product,d.product_version,d.content_type,d.title,d.metadata AS doc_metadata,c.chunk_id,c.chunk_index,c.content,c.metadata,1-(c.embedding <=> %s::vector) AS score
                      FROM knowledge.chunks c JOIN knowledge.documents d ON d.document_id=c.document_id
                      WHERE c.embedding IS NOT NULL ORDER BY c.embedding <=> %s::vector LIMIT %s""", (vec,vec,top_k)).fetchall()
                docs=[]
                for r in rows:
                    item=dict(r)
                    cm=item.get('metadata') if isinstance(item.get('metadata'), dict) else (json.loads(item['metadata']) if isinstance(item.get('metadata'), str) else {})
                    dm=item.get('doc_metadata') if isinstance(item.get('doc_metadata'), dict) else (json.loads(item['doc_metadata']) if isinstance(item.get('doc_metadata'), str) else {})
                    item['applicability']=cm.get('applicability') or dm.get('applicability') or {}
                    item['metadata']={**dm, **cm}
                    item['id']=str(item['document_id'])
                    docs.append(item)
            else:
                # Explicit lexical fallback for environments before embeddings are loaded.
                terms=[t.lower() for t in query_str.split() if len(t)>2]
                if product:
                    rows=conn.execute("SELECT d.document_id,d.source,d.source_url,d.product,d.product_version,d.content_type,d.title,d.metadata AS doc_metadata,c.chunk_id,c.chunk_index,c.content,c.metadata FROM knowledge.chunks c JOIN knowledge.documents d ON d.document_id=c.document_id WHERE d.product=%s LIMIT 500", (product,)).fetchall()
                else:
                    rows=conn.execute("SELECT d.document_id,d.source,d.source_url,d.product,d.product_version,d.content_type,d.title,d.metadata AS doc_metadata,c.chunk_id,c.chunk_index,c.content,c.metadata FROM knowledge.chunks c JOIN knowledge.documents d ON d.document_id=c.document_id LIMIT 500").fetchall()
                scored=[]
                for r in rows:
                    hay=(str(r['title'])+' '+str(r['content'])).lower(); score=sum(t in hay for t in terms)/(len(terms) or 1)
                    if score: scored.append((score,dict(r)))
                scored.sort(key=lambda x:x[0],reverse=True)
                docs=[]
                for s,r in scored[:top_k]:
                    item={**r,'score':s}
                    cm=item.get('metadata') if isinstance(item.get('metadata'), dict) else (json.loads(item['metadata']) if isinstance(item.get('metadata'), str) else {})
                    dm=item.get('doc_metadata') if isinstance(item.get('doc_metadata'), dict) else (json.loads(item['doc_metadata']) if isinstance(item.get('doc_metadata'), str) else {})
                    item['applicability']=cm.get('applicability') or dm.get('applicability') or {}
                    item['metadata']={**dm, **cm}
                    item['id']=str(item['document_id'])
                    docs.append(item)
            return {'documents': docs}

