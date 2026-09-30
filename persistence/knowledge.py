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

    def search(self, query: str, top_k: int = 5, product: str | None = None):
        with self.tracker.connection() as conn:
            if self.embed:
                vec=self._vector_literal(self.embed(query))
                if product:
                    rows=conn.execute("""SELECT d.document_id,d.source,d.source_url,d.product,d.product_version,d.content_type,d.title,c.content,c.metadata,1-(c.embedding <=> %s::vector) AS score
                      FROM knowledge.chunks c JOIN knowledge.documents d ON d.document_id=c.document_id
                      WHERE c.embedding IS NOT NULL AND d.product=%s ORDER BY c.embedding <=> %s::vector LIMIT %s""", (vec,product,vec,top_k)).fetchall()
                else:
                    rows=conn.execute("""SELECT d.document_id,d.source,d.source_url,d.product,d.product_version,d.content_type,d.title,c.content,c.metadata,1-(c.embedding <=> %s::vector) AS score
                      FROM knowledge.chunks c JOIN knowledge.documents d ON d.document_id=c.document_id
                      WHERE c.embedding IS NOT NULL ORDER BY c.embedding <=> %s::vector LIMIT %s""", (vec,vec,top_k)).fetchall()
                return [dict(r) for r in rows]
            # Explicit lexical fallback for environments before embeddings are loaded.
            terms=[t.lower() for t in query.split() if len(t)>2]
            rows=conn.execute("SELECT d.document_id,d.source,d.source_url,d.product,d.product_version,d.content_type,d.title,c.content,c.metadata FROM knowledge.chunks c JOIN knowledge.documents d ON d.document_id=c.document_id LIMIT 500").fetchall()
            scored=[]
            for r in rows:
                hay=(r['title']+' '+r['content']).lower(); score=sum(t in hay for t in terms)/(len(terms) or 1)
                if score: scored.append((score,dict(r)))
            scored.sort(key=lambda x:x[0],reverse=True)
            return [{**r,'score':s} for s,r in scored[:top_k]]
