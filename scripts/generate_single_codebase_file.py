#!/usr/bin/env python3
"""Generates a single consolidated file containing all repository directories and files

with commented header metadata.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path

# Explicit patterns for pattern-based secret redaction across all files
SECRET_PATTERNS = [
    # Explicit value patterns for known API key formats
    (re.compile(r"\bgsk_[A-Za-z0-9_\-]{20,}\b"), "***REDACTED_GROQ_KEY***"),
    (re.compile(r"\bsk-or-v1-[A-Za-z0-9_\-]{20,}\b"), "***REDACTED_OPENROUTER_KEY***"),
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{20,}\b"), "***REDACTED_API_KEY***"),
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), "***REDACTED_AWS_KEY***"),
    # ALL-CAPS env-style: PASSWORD=..., API_KEY: ..., AUTH_TOKEN = ...
    (re.compile(
        r"(?m)^([A-Z][A-Z0-9_]*(?:PASSWORD|PASSWD|SECRET|TOKEN|APIKEY|API_KEY|PWD)[A-Z0-9_]*)"
        r"\s*[:=]\s*(?![\w.]*(?:environ|getenv)\()(\S.*)$"),
     r"\g<1> = ***REDACTED***"),
    # Quoted literals: password="...", secret: '...'
    (re.compile(
        r"""(?i)\b(password|passwd|pwd|secret|api[_-]?key|apikey|client[_-]?secret|"""
        r"""access[_-]?token|auth[_-]?token|private[_-]?key)\b(\s*[:=]\s*)(["'])([^"'\n]{4,})(["'])"""),
     r"\g<1>\g<2>\g<3>***REDACTED***\g<5>"),
    # YAML unquoted values
    (re.compile(
        r"(?im)^([ \t-]*(?:password|passwd|pwd|secret|api[_-]?key|apikey|client[_-]?secret|"
        r"access[_-]?token|auth[_-]?token|private[_-]?key))(\s*:\s*)([A-Za-z0-9_\-./+=@]{6,})[ \t]*$"),
     r"\g<1>\g<2>***REDACTED***"),
    # DSN connection strings
    (re.compile(
        r"(?i)\b((?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|amqp)://[^:\s/@]+:)([^@\s/]+)(@)"),
     r"\g<1>***REDACTED***\g<3>"),
    # Authorization: Bearer <token>
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9\-._~+/]{8,}=*"), r"\g<1>***REDACTED***"),
    # PEM private keys
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S),
     "***REDACTED_PRIVATE_KEY***"),
]


def scrub_content(text: str) -> tuple[str, int]:
    total_redactions = 0
    for rx, repl in SECRET_PATTERNS:
        text, n = rx.subn(repl, text)
        total_redactions += n
    return text, total_redactions


def generate_single_file(root_dir: Path, output_file: Path) -> None:
    ignored_dirs = {
        '.git', '__pycache__', '.pytest_cache', '.vscode', '.gemini',
        'review_bundle', '.venv', 'venv', 'scratch', 'node_modules'
    }
    ignored_exts = {'.pyc', '.pyo', '.pyd', '.so', '.tar', '.gz', '.zip'}
    hard_excluded_files = {'.env', '.env.local', '.env.prod', '.env.production', '.env.test'}
    output_filename = output_file.name

    all_paths = sorted(root_dir.rglob('*'))
    directories = []
    files = []

    for p in all_paths:
        parts = set(p.parts)
        if any(d in parts for d in ignored_dirs):
            continue
        rel = p.relative_to(root_dir)
        if rel.name == output_filename or rel.name.endswith('.txt.bak'):
            continue
        # HARD EXCLUDE: .env files (only allow .env.example)
        if rel.name in hard_excluded_files or (rel.name.startswith('.env') and rel.name != '.env.example'):
            continue
        if p.is_dir():
            directories.append(rel)
        elif p.is_file():
            if p.suffix in ignored_exts:
                continue
            files.append(rel)

    timestamp = datetime.now(timezone.utc).isoformat()
    total_file_redactions = 0

    with open(output_file, 'w', encoding='utf-8') as out:
        # Global Header
        out.write("# " + "=" * 78 + "\n")
        out.write("# MIGRATION FAILURE AGENT (MFA) - CONSOLIDATED CODEBASE & REPOSITORY BUNDLE\n")
        out.write(f"# Generated: {timestamp}\n")
        out.write(f"# Total Directories: {len(directories)}\n")
        out.write(f"# Total Files: {len(files)}\n")
        out.write("# " + "=" * 78 + "\n\n")

        # Directories Section
        out.write("# " + "=" * 78 + "\n")
        out.write("# REPOSITORY DIRECTORY TREE STRUCTURE\n")
        out.write("# " + "=" * 78 + "\n")
        for d in directories:
            depth = len(d.parts) - 1
            indent = "  " * depth
            out.write(f"# {indent}📁 {d}/\n")
        out.write("\n")

        # Files Manifest
        out.write("# " + "=" * 78 + "\n")
        out.write("# TABLE OF CONTENTS (ALL FILES INCLUDED)\n")
        out.write("# " + "=" * 78 + "\n")
        for i, f in enumerate(files, 1):
            out.write(f"# [{i:03d}] {f}\n")
        out.write("\n")

        # File Contents
        for i, rel_path in enumerate(files, 1):
            full_path = root_dir / rel_path
            suffix = rel_path.suffix.lower()

            if suffix == '.sql':
                comment_char = "--"
            elif suffix in ('.html', '.xml'):
                comment_char = None
            else:
                comment_char = "#"

            separator = "=" * 78
            if comment_char:
                out.write(f"\n{comment_char} {separator}\n")
                out.write(f"{comment_char} [{i:03d}/{len(files):03d}] File: {rel_path}\n")
                out.write(f"{comment_char} Path: {rel_path}\n")
                out.write(f"{comment_char} Size: {full_path.stat().st_size} bytes\n")
                out.write(f"{comment_char} {separator}\n\n")
            else:
                out.write(f"\n<!-- {separator}\n")
                out.write(f"[{i:03d}/{len(files):03d}] File: {rel_path}\n")
                out.write(f"Path: {rel_path}\n")
                out.write(f"Size: {full_path.stat().st_size} bytes\n")
                out.write(f"{separator} -->\n\n")

            try:
                content = full_path.read_text(encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    content = full_path.read_text(encoding='latin-1')
                except Exception as exc:
                    content = f"# [ERROR READING FILE: {exc}]\n"

            # Scrub secrets across all included files using value patterns
            content, count = scrub_content(content)
            total_file_redactions += count

            out.write(content)
            if not content.endswith('\n'):
                out.write('\n')

    print(f"Successfully generated {output_file} ({output_file.stat().st_size} bytes, {len(files)} files, {total_file_redactions} redactions applied).")


if __name__ == '__main__':
    root = Path(__file__).resolve().parents[1]
    out_path = root / 'mfa_complete_codebase.txt'
    generate_single_file(root, out_path)

