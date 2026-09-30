#!/usr/bin/env python3
"""Ingest an authorized Red Hat corpus into a normalized local RAG dataset.

Default mode uses a small list of public Red Hat pages. For restricted Red Hat
Knowledgebase content, pass URLs/files you are authorized to access. This tool
never attempts to bypass authentication or licensing controls.
"""
import argparse, hashlib, re
from pathlib import Path
import requests, yaml
from bs4 import BeautifulSoup

DEFAULT_URLS = [
    'https://access.redhat.com/products/red-hat-openshift-virtualization/',
    'https://access.redhat.com/articles/7119411',
]

def kind(url):
    if '/security/cve/' in url: return 'cve'
    if '/errata/' in url: return 'errata'
    if '/solutions/' in url: return 'solution'
    if '/articles/' in url: return 'documentation'
    return 'product'

def product(url, title):
    s=(url+' '+title).lower()
    if 'migration-toolkit' in s or re.search(r'\bmtv\b', s): return 'migration-toolkit-for-virtualization'
    if 'virtualization' in s: return 'openshift-virtualization'
    if 'openshift' in s: return 'openshift-container-platform'
    return 'red-hat'

def normalize(text):
    return ' '.join(text.split())

def scrape(url, timeout=20):
    r=requests.get(url, timeout=timeout, headers={'User-Agent':'migration-failure-agent-dataset/1.0'})
    r.raise_for_status()
    soup=BeautifulSoup(r.text, 'html.parser')
    for x in soup(['script','style','noscript']): x.decompose()
    title=soup.title.get_text(' ', strip=True) if soup.title else url
    text=normalize(soup.get_text(' ', strip=True))
    digest=hashlib.sha256(text.encode('utf-8')).hexdigest()
    return {
        'id': 'RH-'+digest[:24], 'source':'redhat', 'kind':kind(url),
        'product':product(url,title), 'title':title, 'url':url,
        'text':text[:50000], 'content_hash':digest, 'tags':[]
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--urls', nargs='*', default=DEFAULT_URLS)
    ap.add_argument('--output', default='datasets/redhat_knowledge_scraped.yaml')
    ap.add_argument('--timeout', type=int, default=20)
    args=ap.parse_args()
    docs=[]
    for url in args.urls:
        try:
            d=scrape(url,args.timeout); docs.append(d); print('OK',url)
        except Exception as exc: print('WARN',url,exc)
    out=Path(args.output); out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(yaml.safe_dump({'redhat_knowledge':docs},sort_keys=False,allow_unicode=True), encoding='utf-8')
    print(f'wrote {len(docs)} documents to {out}')

if __name__=='__main__': main()
