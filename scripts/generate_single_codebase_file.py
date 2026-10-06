#!/usr/bin/env python3
"""Generates a single consolidated file containing all repository directories and files

with commented header metadata.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path


def generate_single_file(root_dir: Path, output_file: Path) -> None:
    ignored_dirs = {'.git', '__pycache__', '.pytest_cache', '.vscode', '.gemini'}
    ignored_exts = {'.pyc', '.pyo', '.pyd', '.so', '.tar', '.gz', '.zip'}
    output_filename = output_file.name

    all_paths = sorted(root_dir.rglob('*'))
    directories = []
    files = []

    for p in all_paths:
        parts = set(p.parts)
        if any(d in parts for d in ignored_dirs):
            continue
        rel = p.relative_to(root_dir)
        if rel.name == output_filename:
            continue
        if p.is_dir():
            directories.append(rel)
        elif p.is_file():
            if p.suffix in ignored_exts:
                continue
            files.append(rel)

    timestamp = datetime.now(timezone.utc).isoformat()

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

            # Scrub secrets and API keys for security governance
            if '.env' in rel_path.name:
                import re
                content = re.sub(r'(OPENROUTER_API_KEY=)(.+)', r'\1***REDACTED_API_KEY***', content)
                content = re.sub(r'(POSTGRES_PASSWORD=)(.+)', r'\1***REDACTED_PASSWORD***', content)
            import re
            content = re.sub(r'sk-[a-zA-Z0-9_\-]{20,}', 'sk-***REDACTED***', content)

            out.write(content)
            if not content.endswith('\n'):
                out.write('\n')

    print(f"Successfully generated {output_file} ({output_file.stat().st_size} bytes, {len(files)} files).")


if __name__ == '__main__':
    root = Path(__file__).resolve().parents[1]
    out_path = root / 'mfa_complete_codebase.txt'
    generate_single_file(root, out_path)
