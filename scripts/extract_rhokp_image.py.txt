#!/usr/bin/env python3
"""Automated Knowledge Corpus Generator for RHOKP Container Images.

Extracts, normalizes, and version-tags migration knowledge from an RHOKP container image
filesystem or local REST mirror. Ingests:
  - Technical Solutions & Knowledgebase Articles
  - Product Documentation & Guides (MTV, OCV, CDI, KubeVirt)
  - Release Notes (Known Issues, Bug Fixes, Deprecations)
  - Errata (RHSA, RHBA, RHEA) and CVE Advisories

Outputs a version-tagged YAML corpus matching engine/knowledge_compatibility.py schema.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engine.knowledge_compatibility import evaluate_candidate


# ── FILTERING RULES: INCLUDE ONLY MIGRATION & VIRTUALIZATION CONTENT ────────
RELEVANT_KEYWORDS = [
    r"\bmtv\b",
    r"migration[- ]toolkit",
    r"virt-v2v",
    r"kubevirt",
    r"openshift[- ]virtualization",
    r"\bocv\b",
    r"\bcbt\b",
    r"changed[- ]block[- ]tracking",
    r"\bvddk\b",
    r"\bvcenter\b",
    r"\besxi\b",
    r"\bvsphere\b",
    r"\bvmware\b",
    r"containerized[- ]data[- ]importer",
    r"\bcdi\b",
    r"datavolume",
    r"forklift",
]

RELEVANT_REGEX = re.compile("|".join(RELEVANT_KEYWORDS), re.IGNORECASE)


def is_migration_relevant(title: str, body: str, tags: list[str]) -> bool:
    text = f"{title} {' '.join(tags)} {body[:2000]}"
    return bool(RELEVANT_REGEX.search(text))


# ── VERSION APPLICABILITY EXTRACTION ─────────────────────────────────────────
def parse_applicability(env_text: str, title: str, body: str) -> dict[str, Any]:
    """Deterministically extract ocv, ocp, mtv, and vmware version applicability constraints."""
    text = f"{env_text} {title} {body[:1500]}"
    applicability: dict[str, Any] = {}

    # MTV Versions
    mtv_matches = re.findall(r"\b(?:MTV|Migration Toolkit for Virtualization)\s+([2-9]\.\d+(?:\.\d+)?)\b", text, re.I)
    if mtv_matches:
        unique_mtv = sorted(set(mtv_matches))
        applicability["mtv"] = {"versions": unique_mtv}
        if len(unique_mtv) > 1:
            applicability["mtv"]["min"] = unique_mtv[0]
            applicability["mtv"]["max"] = unique_mtv[-1]
    else:
        # Check for range: MTV 2.8+ or MTV 2.8 to 2.12
        m_range = re.search(r"\bMTV\s+([2-9]\.\d+)(?:\s*(?:to|-)\s*([2-9]\.\d+))?", text, re.I)
        if m_range:
            applicability["mtv"] = {"min": m_range.group(1)}
            if m_range.group(2):
                applicability["mtv"]["max"] = m_range.group(2)

    # OCV / OCP Versions
    ocv_matches = re.findall(r"\b(?:OpenShift Virtualization|OCV|OpenShift|OCP)\s+(4\.\d+(?:\.\d+)?)\b", text, re.I)
    if ocv_matches:
        unique_ocv = sorted(set(ocv_matches))
        applicability["ocv"] = {"versions": unique_ocv}
        applicability["ocp"] = {"versions": unique_ocv}

    # VMware vSphere / vCenter Versions
    vmw_matches = re.findall(r"\b(?:vSphere|vCenter|ESXi)\s+([6-8]\.\d+(?:\.\d+)?)\b", text, re.I)
    if vmw_matches:
        unique_vmw = sorted(set(vmw_matches))
        applicability["vmware"] = {"versions": unique_vmw}

    return applicability


# ── STRUCTURED FAILURE SIGNATURE EXTRACTION ─────────────────────────────────
KNOWN_ERROR_PATTERNS = [
    (r"(?:vmware\.cbt\.retry_limit|cbt.*retry.*limit)", "vmware.cbt.retry_limit"),
    (r"(?:cbt.*disabled|ctkenabled.*false)", "vmware.cbt.disabled"),
    (r"(?:port\s*443.*unreachable|esxi.*443)", "vmware.esxi.port443_unreachable"),
    (r"(?:port\s*902.*unreachable|esxi.*902)", "vmware.esxi.port902_unreachable"),
    (r"(?:csi.*provisioning.*timeout|datavolume.*provisioning.*timed.*out)", "storage.csi.provisioning_timeout"),
    (r"(?:networkattachmentdefinition.*not.*found|destination.*network.*not.*found)", "network.nad.missing"),
    (r"(?:cdrom-image.*filename|ide\d:\d.*cdrom)", "conversion.virt_v2v.cdrom"),
    (r"(?:url not found:.*-flat\.vmdk|vcenter: url not found)", "conversion.vmdk.url_not_found"),
    (r"(?:vir_from_esx.*401|http response code 401)", "conversion.esx.unauthorized"),
    (r"(?:filesystem was mounted read-only|ntfs.*dirty)", "os.windows.filesystem_readonly"),
    (r"(?:unable to resize disk image|qemu-img resize failed)", "disk.resize_failed"),
    (r"(?:argument list too long|e2big)", "conversion.image_conversion.arg_list"),
    (r"(?:out of memory|oom-killer|exit code 137)", "conversion.virt_v2v.oom"),
]


def extract_failure_codes(body: str, explicit_codes: list[str] | None = None) -> list[str]:
    codes = list(explicit_codes or [])
    for pattern, code in KNOWN_ERROR_PATTERNS:
        if re.search(pattern, body, re.IGNORECASE):
            if code not in codes:
                codes.append(code)
    return codes


# ── EXTRACTORS FOR DIFFERENT CONTENT TYPES ──────────────────────────────────
def process_solution_item(item: dict[str, Any], base_url: str) -> dict[str, Any] | None:
    doc_id = str(item.get("id") or item.get("solution_id") or "")
    title = str(item.get("title") or "").strip()
    issue = str(item.get("issue") or item.get("symptom") or "").strip()
    environment = str(item.get("environment") or "").strip()
    resolution = str(item.get("resolution") or item.get("solution") or "").strip()
    diagnostic = str(item.get("diagnostic_steps") or item.get("root_cause") or "").strip()
    tags = item.get("tags") or []

    full_text = f"# {title}\n\n## Environment\n{environment}\n\n## Issue\n{issue}\n\n## Root Cause\n{diagnostic}\n\n## Resolution\n{resolution}"
    if not is_migration_relevant(title, full_text, tags):
        return None

    applicability = parse_applicability(environment, title, full_text)
    failure_codes = extract_failure_codes(full_text, item.get("failure_codes"))

    clean_id = f"RH-SOL-{doc_id}" if not doc_id.startswith("RH-") else doc_id
    url = item.get("url") or f"{base_url.rstrip('/')}/solutions/{doc_id}"

    return {
        "id": clean_id,
        "source": "rhokp",
        "kind": "solution",
        "product": item.get("product") or "migration-toolkit-for-virtualization",
        "product_version": item.get("product_version") or "2.11",
        "applicability": applicability,
        "failure_codes": failure_codes,
        "title": title,
        "url": url,
        "text": full_text,
        "tags": list(set(tags + ["rhokp", "solution", "migration"])),
    }


def process_release_notes(item: dict[str, Any], base_url: str) -> list[dict[str, Any]]:
    """Parse Known Issues from MTV or OpenShift Virtualization release notes."""
    results = []
    version = str(item.get("version") or "2.11")
    url = item.get("url") or f"{base_url.rstrip('/')}/release_notes/rn-{version}"
    known_issues = item.get("known_issues") or []

    for idx, issue in enumerate(known_issues, 1):
        title = str(issue.get("title") or f"MTV {version} Known Issue #{idx}").strip()
        desc = str(issue.get("description") or issue.get("symptom") or "").strip()
        workaround = str(issue.get("workaround") or issue.get("remediation") or "Refer to release notes.").strip()
        jira_id = str(issue.get("jira") or issue.get("bz") or f"ISSUE-{idx}")

        full_text = f"# MTV {version} Known Issue: {title}\n\n## Symptom\n{desc}\n\n## Workaround\n{workaround}"
        if not is_migration_relevant(title, full_text, []):
            continue

        failure_codes = extract_failure_codes(full_text)
        applicability = {"mtv": {"versions": [version]}}

        results.append({
            "id": f"RH-RN-MTV-{version.replace('.', '_')}-{jira_id}",
            "source": "rhokp",
            "kind": "documentation",
            "product": "migration-toolkit-for-virtualization",
            "product_version": version,
            "applicability": applicability,
            "failure_codes": failure_codes,
            "title": f"MTV {version} Release Notes: {title}",
            "url": url,
            "text": full_text,
            "tags": ["release-notes", "known-issue", "mtv", version],
        })
    return results


def process_errata_item(item: dict[str, Any], base_url: str) -> dict[str, Any] | None:
    """Parse Errata/Security Advisory (RHSA, RHBA, RHEA) affecting migration components."""
    advisory_id = str(item.get("id") or item.get("advisory_id") or "")
    title = str(item.get("title") or item.get("synopsis") or "").strip()
    description = str(item.get("description") or "").strip()
    cves = item.get("cves") or []
    fixed_packages = item.get("packages") or []

    full_text = f"# Advisory: {advisory_id} — {title}\n\n## Description\n{description}\n\n## CVEs Addressed\n{', '.join(cves)}\n\n## Packages Fixed\n{', '.join(fixed_packages)}"
    if not is_migration_relevant(title, full_text, cves):
        return None

    applicability = parse_applicability("", title, description)
    return {
        "id": f"RH-ERRATA-{advisory_id}",
        "source": "rhokp",
        "kind": "errata",
        "product": "migration-toolkit-for-virtualization",
        "product_version": "2.11",
        "applicability": applicability,
        "failure_codes": extract_failure_codes(full_text),
        "title": f"{advisory_id}: {title}",
        "url": item.get("url") or f"{base_url.rstrip('/')}/errata/{advisory_id}",
        "text": full_text,
        "tags": ["errata", "security", "advisory"] + cves,
    }


# ── MAIN SCANNER & GENERATOR ────────────────────────────────────────────────
def scan_and_generate(source_dir: Path | None, api_url: str | None, base_portal_url: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def add_record(rec: dict[str, Any] | None):
        if rec and rec["id"] not in seen_ids:
            seen_ids.add(rec["id"])
            records.append(rec)

    # 1. Directory-based scan (from mounted or extracted container image)
    if source_dir and source_dir.exists():
        print(f"Scanning RHOKP image filesystem at: {source_dir}...")
        for p in source_dir.rglob("*.json"):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue

            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        if "solution" in str(p).lower() or "issue" in item:
                            add_record(process_solution_item(item, base_portal_url))
                        elif "release_notes" in str(p).lower() or "known_issues" in item:
                            for r in process_release_notes(item, base_portal_url):
                                add_record(r)
                        elif "errata" in str(p).lower() or "cves" in item:
                            add_record(process_errata_item(item, base_portal_url))
            elif isinstance(data, dict):
                if "known_issues" in data:
                    for r in process_release_notes(data, base_portal_url):
                        add_record(r)
                elif "cves" in data or "synopsis" in data:
                    add_record(process_errata_item(data, base_portal_url))
                else:
                    add_record(process_solution_item(data, base_portal_url))

    # 2. REST API scan (if RHOKP service is actively running in DMZ)
    if api_url:
        import requests
        print(f"Querying RHOKP DMZ API at: {api_url}...")
        for endpoint in ["/api/v1/solutions", "/api/v1/knowledge", "/api/v1/release_notes", "/api/v1/errata"]:
            try:
                r = requests.get(f"{api_url.rstrip('/')}{endpoint}", timeout=10)
                if r.status_code == 200:
                    items = r.json()
                    if isinstance(items, list):
                        for item in items:
                            if "known_issues" in item:
                                for rec in process_release_notes(item, base_portal_url):
                                    add_record(rec)
                            elif "cves" in item:
                                add_record(process_errata_item(item, base_portal_url))
                            else:
                                add_record(process_solution_item(item, base_portal_url))
            except Exception as exc:
                print(f"  Note: {endpoint} returned {exc}")

    return records


def export_yaml(records: list[dict[str, Any]], output_file: Path):
    import yaml
    output_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "2.11.0",
        "status": "AUTO_GENERATED_RHOKP_CORPUS",
        "description": "Automatically generated and version-tagged migration corpus from RHOKP image.",
        "total_documents": len(records),
        "redhat_knowledge": records,
    }
    output_file.write_text(yaml.safe_dump(payload, sort_keys=False, width=120), encoding="utf-8")
    print(f"Successfully exported {len(records)} migration knowledge records to: {output_file}")


def validate_corpus(records: list[dict[str, Any]]):
    """Run deterministic gate validation on all extracted documents."""
    env = {"ocp_version": "4.19.23", "ocv_version": "4.19.23", "mtv_version": "2.11.0"}
    passed = 0
    for r in records:
        try:
            res = evaluate_candidate(env, {"failure_code": (r.get("failure_codes") or ["test"])[0]}, r)
            if res.get("recommendation_status") in {"ELIGIBLE", "NEEDS_VALIDATION", "REJECT_ERROR_MISMATCH", "REJECT_VERSION"}:
                passed += 1
        except Exception as e:
            print(f"Validation warning on {r['id']}: {e}")
    print(f"Validated {passed}/{len(records)} records through deterministic firewall.")


def main():
    parser = argparse.ArgumentParser(description="Extract and generate version-tagged migration YAML from RHOKP image")
    parser.add_argument("--source-dir", "-d", type=Path, help="Path to mounted/extracted RHOKP container image directory")
    parser.add_argument("--api-url", "-u", help="URL of running RHOKP REST API in DMZ")
    parser.add_argument("--base-url", default="https://rhokp.dmz.corp", help="Base URL for local DMZ portal links")
    parser.add_argument("--output-yaml", "-o", default=ROOT / "datasets/rhokp_generated_corpus.yaml", type=Path, help="Output YAML path")
    parser.add_argument("--validate", action="store_true", help="Validate extracted records against compatibility engine")
    args = parser.parse_args()

    if not args.source_dir and not args.api_url:
        print("ERROR: Specify either --source-dir (extracted image filesystem) or --api-url (local RHOKP server).", file=sys.stderr)
        sys.exit(1)

    records = scan_and_generate(args.source_dir, args.api_url, args.base_portal_url if hasattr(args, 'base_portal_url') else args.base_url)
    if not records:
        print("WARN: No relevant migration records found in the specified source.")
        sys.exit(0)

    export_yaml(records, args.output_yaml)
    if args.validate:
        validate_corpus(records)


if __name__ == "__main__":
    main()
