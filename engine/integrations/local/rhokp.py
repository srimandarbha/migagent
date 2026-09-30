class FixtureRHOKPRAGAdapter:
    """Local RHOKP/RAG adapter. Uses the same Red Hat knowledge dataset as production ingestion will populate."""
    def __init__(self, documents): self.documents=list(documents)
    def search(self, params):
        q=(params.get('query') or '').lower().split()
        product=params.get('product')
        matches=[]
        for d in self.documents:
            if product and d.get('product') != product: continue
            hay=' '.join([d.get('title',''), d.get('text',''), ' '.join(d.get('tags',[]))]).lower()
            score=sum(1 for token in q if token in hay)
            if score: matches.append((score,d))
        matches.sort(key=lambda x:x[0], reverse=True)
        return {'documents':[d for _,d in matches[:int(params.get('top_k',5))]]}
