#!/usr/bin/env python3
"""Ingest authorized Red Hat knowledge into PostgreSQL/pgvector.

Two modes are supported:
  1. --urls / --urls-file: fetch public/authorized URLs, normalize them, persist
     documents/chunks to knowledge.*, then optionally embed them.
  2. --input-yaml: load an already authorized normalized corpus and persist it
     directly. This is useful for authenticated Red Hat KB exports.

This script is deliberately the DB ingestion entrypoint. Scraping to YAML alone
is not considered ingestion because the agent reads PostgreSQL in production.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import uuid
from pathlib import Path
from typing import Any

import psycopg
import requests
import yaml
from bs4 import BeautifulSoup

DEFAULT_DSN = "postgresql://postgres:postgres@127.0.0.1:5432/migration_agent"
DEFAULT_URLS = [
    "https://access.redhat.com/products/red-hat-openshift-virtualization/",
    "https://access.redhat.com/articles/7119411",
]
ROOT = Path(__file__).resolve().parents[1]


def kind(url: str) -> str:
    if "/security/cve/" in url:
        return "cve"
    if "/errata/" in url:
        return "errata"
    if "/solutions/" in url:
        return "solution"
    if "/articles/" in url:
        return "documentation"
    return "product"


def product(url: str, title: str) -> str:
    s = (url + " " + title).lower()
    if "migration-toolkit" in s or re.search(r"\bmtv\b", s):
        return "migration-toolkit-for-virtualization"
    if "virtualization" in s:
        return "openshift-virtualization"
    if "openshift" in s:
        return "openshift-container-platform"
    return "red-hat"


def extract_product_and_version(doc: dict[str, Any]) -> tuple[str, str | None, dict[str, Any]]:
    """Deterministically extract product, product_version, and applicability without hallucination.

    Precedence:
      1. Explicit document metadata (product, product_version/version, applicability)
      2. Source URL regex matching
      3. Structured document title
      4. Fallback (version=None)
    """
    url = str(doc.get("url") or doc.get("source_url") or "").strip()
    title = str(doc.get("title") or "").strip()
    explicit_prod = doc.get("product")
    explicit_ver = doc.get("product_version") or doc.get("version")
    applicability = dict(doc.get("applicability") or (doc.get("metadata") or {}).get("applicability") or {})

    # Check applicability for explicit versions if not set
    if not explicit_ver and applicability:
        if applicability.get("mtv", {}).get("versions"):
            explicit_ver = str(applicability["mtv"]["versions"][0])
            explicit_prod = explicit_prod or "migration-toolkit-for-virtualization"
        elif applicability.get("mtv", {}).get("min") and applicability.get("mtv", {}).get("min") == applicability.get("mtv", {}).get("max"):
            explicit_ver = str(applicability["mtv"]["min"])
            explicit_prod = explicit_prod or "migration-toolkit-for-virtualization"
        elif applicability.get("ocv", {}).get("versions"):
            explicit_ver = str(applicability["ocv"]["versions"][0])
            explicit_prod = explicit_prod or "openshift-virtualization"

    ver = str(explicit_ver).strip() if explicit_ver else None

    # URL extraction
    if not ver and url:
        m = re.search(r"/(?:migration_toolkit_for_virtualization|mtv)/(\d+\.\d+(?:\.\d+)?)/", url, re.I)
        if m:
            ver = m.group(1)
            explicit_prod = explicit_prod or "migration-toolkit-for-virtualization"
        else:
            m = re.search(r"/(?:openshift_virtualization|red-hat-openshift-virtualization|ocv)/(\d+\.\d+(?:\.\d+)?)/", url, re.I)
            if m:
                ver = m.group(1)
                explicit_prod = explicit_prod or "openshift-virtualization"
            else:
                m = re.search(r"/(?:openshift_container_platform|ocp)/(\d+\.\d+(?:\.\d+)?)/", url, re.I)
                if m:
                    ver = m.group(1)
                    explicit_prod = explicit_prod or "openshift-container-platform"

    # Title extraction
    if not ver and title:
        m = re.search(r"\b(?:Migration Toolkit for Virtualization|MTV)\s+(\d+\.\d+(?:\.\d+)?)\b", title, re.I)
        if m:
            ver = m.group(1)
            explicit_prod = explicit_prod or "migration-toolkit-for-virtualization"
        else:
            m = re.search(r"\b(?:OpenShift Virtualization|OCV)\s+(\d+\.\d+(?:\.\d+)?)\b", title, re.I)
            if m:
                ver = m.group(1)
                explicit_prod = explicit_prod or "openshift-virtualization"

    prod = explicit_prod or product(url, title)

    # Derive applicability if missing
    if not applicability:
        if prod == "migration-toolkit-for-virtualization" and ver:
            applicability = {"mtv": {"versions": [ver], "min": ver, "max": ver}}
        elif prod == "openshift-virtualization" and ver:
            applicability = {"ocv": {"versions": [ver], "min": ver, "max": ver}}
        elif prod == "openshift-container-platform" and ver:
            applicability = {"ocp": {"versions": [ver], "min": ver, "max": ver}}
        else:
            applicability = {}

    return prod, ver, applicability


def normalize(text: str) -> str:
    return " ".join(text.split())


def scrape(url: str, timeout: int = 30) -> dict[str, Any]:
    r = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": "migration-failure-agent-dataset/2.11"},
    )
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    for x in soup(["script", "style", "noscript"]):
        x.decompose()
    title = soup.title.get_text(" ", strip=True) if soup.title else url
    # Target main content container if present, avoiding navigational boilerplate
    main_el = soup.find("main") or soup.find("article") or soup.find("div", class_=re.compile(r"content|body|document", re.I)) or soup
    text = normalize(main_el.get_text(" ", strip=True))
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    doc_stub = {"url": url, "title": title}
    prod, ver, applicability = extract_product_and_version(doc_stub)
    return {
        "id": "RH-" + digest[:24],
        "source": "redhat",
        "kind": kind(url),
        "product": prod,
        "product_version": ver,
        "applicability": applicability,
        "title": title,
        "url": url,
        "text": text[:100000],
        "content_hash": digest,
        "tags": [],
    }


def load_urls_file(path: Path) -> list[str]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if isinstance(data, list):
        return [str(x) for x in data]
    if isinstance(data, dict):
        urls = data.get("urls", [])
        if isinstance(urls, list):
            return [str(x) for x in urls]
    raise ValueError(f"Unsupported URL manifest format: {path}")


def load_normalized_yaml(path: Path) -> list[dict[str, Any]]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    docs = data.get("redhat_knowledge", data.get("documents", []))
    if not isinstance(docs, list):
        raise ValueError(f"Expected redhat_knowledge/documents list in {path}")
    return docs


def chunk_text(text: str, chunk_size: int = 1200, overlap: int = 200) -> list[str]:
    """Chunk text respecting section headers and paragraph boundaries with sliding-window overlap."""
    text = text.strip()
    if not text:
        return []
    if overlap >= chunk_size:
        raise ValueError("chunk overlap must be smaller than chunk size")

    # If text is small enough, return as single chunk
    if len(text) <= chunk_size:
        return [text]

    # Check for natural section delimiters (markdown headers, double newlines)
    raw_sections = re.split(r"\n(?=#{1,4}\s+|==+|\b[A-Z0-9\.\s]{3,30}:\n)", text)
    if len(raw_sections) <= 1:
        paras = [p for p in text.split("\n\n") if p.strip()]
        if len(paras) > 1:
            raw_sections = paras

    # If multiple markdown/paragraph sections exist, preserve section boundaries
    if len(raw_sections) > 1:
        chunks = []
        current = ""
        for sec in raw_sections:
            sec_clean = sec.strip()
            if not sec_clean:
                continue
            if len(sec_clean) > chunk_size:
                if current:
                    chunks.append(current)
                    current = ""
                start = 0
                while start < len(sec_clean):
                    end = min(len(sec_clean), start + chunk_size)
                    chunks.append(sec_clean[start:end])
                    if end == len(sec_clean):
                        break
                    start = end - overlap
            else:
                if len(current) + len(sec_clean) + 2 <= chunk_size:
                    current = (current + "\n\n" + sec_clean).strip()
                else:
                    if current:
                        chunks.append(current)
                    current = sec_clean
        if current and (not chunks or chunks[-1] != current):
            chunks.append(current)
        return [c for c in chunks if c.strip()]

    # Standard sliding window for continuous or unstructured text
    chunks = []
    start = 0
    while start < len(text):
        end = min(len(text), start + chunk_size)
        chunks.append(text[start:end])
        if end == len(text):
            break
        start = end - overlap
    return chunks


def persist_documents(dsn: str, docs: list[dict[str, Any]], chunk_size: int = 1200, overlap: int = 200) -> tuple[int, int]:
    inserted_docs = 0
    written_chunks = 0
    schema = (ROOT / "persistence" / "schema.sql").read_text(encoding="utf-8")

    with psycopg.connect(dsn) as conn:
        conn.execute(schema)
        for d in docs:
            content = str(d.get("text") or d.get("content") or "").strip()
            if not content:
                print(f"SKIP empty content: {d.get('id') or d.get('url')}")
                continue

            source_url = d.get("url") or d.get("source_url")
            source = d.get("source", "redhat")
            product_name, product_ver, applicability = extract_product_and_version(d)
            content_hash = d.get("content_hash") or hashlib.sha256(content.encode("utf-8")).hexdigest()
            document_id = uuid.uuid5(uuid.NAMESPACE_URL, str(source_url or d.get("id") or content_hash))

            metadata = dict(d.get("metadata") or {})
            if d.get("tags") is not None:
                metadata.setdefault("tags", d.get("tags"))
            metadata["applicability"] = applicability
            metadata.setdefault("external_id", d.get("id"))
            if d.get("failure_code"):
                metadata.setdefault("failure_code", d.get("failure_code"))
            if d.get("failure_codes"):
                metadata.setdefault("failure_codes", d.get("failure_codes"))

            content_type = d.get("kind", d.get("content_type", "documentation"))
            title = d.get("title", d.get("id", "Red Hat document"))
            summary = d.get("summary") or content[:500]

            # Truly idempotent document resolution by document_id or (source, source_url)
            cur = conn.execute(
                """SELECT document_id, content_hash
                FROM knowledge.documents
                WHERE document_id = %s OR (source = %s AND source_url = %s AND source_url IS NOT NULL)
                LIMIT 1""",
                (document_id, source, source_url),
            )
            existing = cur.fetchone()

            if existing is None:
                # Insert brand new document
                conn.execute(
                    """INSERT INTO knowledge.documents
                    (document_id, source, source_url, product, product_version, content_type, title, summary,
                     retrieved_at, content_hash, metadata, content)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s, %s)""",
                    (
                        document_id, source, source_url, product_name, product_ver,
                        content_type, title, summary, content_hash, json.dumps(metadata), content,
                    ),
                )
                actual_document_id = document_id
                inserted_docs += 1
                needs_chunks = True
            else:
                actual_document_id, old_hash = existing[0], existing[1]
                # Update document metadata
                conn.execute(
                    """UPDATE knowledge.documents SET
                        source = %s,
                        source_url = %s,
                        product = %s,
                        product_version = %s,
                        content_type = %s,
                        title = %s,
                        summary = %s,
                        metadata = %s,
                        content = %s,
                        content_hash = %s,
                        updated_at = now()
                    WHERE document_id = %s""",
                    (
                        source, source_url, product_name, product_ver,
                        content_type, title, summary, json.dumps(metadata), content, content_hash,
                        actual_document_id,
                    ),
                )
                inserted_docs += 1
                if old_hash != content_hash:
                    # Content changed: invalidate old chunks and embeddings
                    conn.execute("DELETE FROM knowledge.chunks WHERE document_id = %s", (actual_document_id,))
                    needs_chunks = True
                else:
                    # Content did not change: verify chunks exist
                    c_count = conn.execute("SELECT count(*) FROM knowledge.chunks WHERE document_id = %s", (actual_document_id,)).fetchone()[0]
                    if c_count == 0:
                        needs_chunks = True
                    else:
                        needs_chunks = False
                        # Update chunk metadata to keep product_version/applicability synchronized
                        chunk_meta_update = {
                            "source_url": source_url,
                            "product": product_name,
                            "product_version": product_ver,
                            "applicability": applicability,
                            "content_type": content_type,
                        }
                        conn.execute(
                            "UPDATE knowledge.chunks SET metadata = metadata || %s::jsonb WHERE document_id = %s",
                            (json.dumps(chunk_meta_update), actual_document_id),
                        )

            if needs_chunks:
                chunks = chunk_text(content, chunk_size, overlap)
                for index, chunk in enumerate(chunks):
                    chunk_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{actual_document_id}:{index}:{hashlib.sha256(chunk.encode()).hexdigest()}")
                    chunk_meta = {
                        "source_url": source_url,
                        "product": product_name,
                        "product_version": product_ver,
                        "applicability": applicability,
                        "content_type": content_type,
                        "title": title,
                        "chunk_index": index,
                        "total_chunks": len(chunks),
                    }
                    conn.execute(
                        """INSERT INTO knowledge.chunks
                        (chunk_id, document_id, chunk_index, content, metadata)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                            content = EXCLUDED.content,
                            metadata = EXCLUDED.metadata,
                            embedding = NULL""",
                        (chunk_id, actual_document_id, index, chunk, json.dumps(chunk_meta)),
                    )
                    written_chunks += 1

        conn.commit()
    return inserted_docs, written_chunks


def main() -> None:
    ap = argparse.ArgumentParser()
    source = ap.add_mutually_exclusive_group()
    source.add_argument("--urls", nargs="*", help="URLs to fetch")
    source.add_argument("--urls-file", type=Path, help="YAML manifest containing a urls list")
    source.add_argument("--input-yaml", type=Path, help="Normalized Red Hat YAML export to load directly")
    ap.add_argument("--output", type=Path, default=ROOT / "datasets" / "redhat_knowledge_scraped.yaml")
    ap.add_argument("--dsn", default=os.getenv("DATABASE_URL", DEFAULT_DSN))
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--chunk-size", type=int, default=1200)
    ap.add_argument("--chunk-overlap", type=int, default=200)
    ap.add_argument("--no-db", action="store_true", help="Only write scraped YAML; do not persist to PostgreSQL")
    args = ap.parse_args()

    if args.input_yaml:
        docs = load_normalized_yaml(args.input_yaml)
    else:
        if args.urls_file:
            urls = load_urls_file(args.urls_file)
        elif args.urls:
            urls = args.urls
        else:
            urls = DEFAULT_URLS

        docs = []
        for url in urls:
            try:
                d = scrape(url, args.timeout)
                docs.append(d)
                print(f"OK {url}")
            except Exception as exc:
                print(f"WARN {url}: {exc}")

        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            yaml.safe_dump({"redhat_knowledge": docs}, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        print(f"wrote {len(docs)} scraped documents to {args.output}")

    if args.no_db:
        print("DB persistence disabled (--no-db).")
        return

    if not docs:
        raise SystemExit("No documents available for DB ingestion")

    document_count, chunk_count = persist_documents(args.dsn, docs, args.chunk_size, args.chunk_overlap)
    print(f"PostgreSQL ingestion complete: documents={document_count}, chunks={chunk_count}")
    print("Next step: run scripts/embed_knowledge.py after the embedding endpoint is available.")


if __name__ == "__main__":
    main()
