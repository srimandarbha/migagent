from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from ingest_redhat import chunk_text, load_normalized_yaml

def test_chunking_has_overlap_and_no_empty_chunks():
    text='x'*12000
    chunks=chunk_text(text, chunk_size=5000, overlap=500)
    assert len(chunks)==3
    assert all(chunks)
    assert chunks[0][-500:] == chunks[1][:500]

def test_common_dataset_contains_multiple_redhat_records():
    data=load_normalized_yaml(ROOT/'datasets/common_dataset.yaml')
    assert len(data) >= 2
    assert all(d.get('source') == 'redhat' for d in data)
