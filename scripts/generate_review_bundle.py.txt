#!/usr/bin/env python3
"""
generate_review_bundle.py — priority-ordered, secret-scrubbed code bundle
for LLM code review.

Design decisions:
  1. Load-bearing code first (explicit tiers), docs never (by default).
     The bundle can no longer be "all markdown".
  2. `.env` is hard-excluded, and every included file is scrubbed for
     secret-looking values. Redaction deliberately over-matches: a false
     positive costs a little review context; a missed secret costs your
     OpenRouter key. All redactions are counted and reported at generation.
  3. Output is split into paste-sized parts (default ~100 KB) so each part
     fits in one chat message, plus one combined file for attachment.

Usage (run from the folder that contains engine/):
    python scripts/generate_review_bundle.py --priority-only   # ~25 core files
    python scripts/generate_review_bundle.py                   # all code, no docs
    python scripts/generate_review_bundle.py --include-skills  # + skills/*.md
    python scripts/generate_review_bundle.py --max-part-kb 60
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Priority tiers: the load-bearing files, in review order.
# ---------------------------------------------------------------------------

TIERS: dict = {
    1: [  # wiring core
        "engine/workflow/engine.py",
        "engine/workflow/graph.py",
        "engine/workflow/nodes/implementation.py",
        "engine/workflow/state.py",
        "engine/ingress/kafka.py",
        "engine/tools/investigation.py",
        "engine/contracts/models.py",
        "persistence/repository.py",
        "engine/integrations/registry.py",
        "engine/integrations/contracts.py",
    ],
    2: [  # safety-critical claims to verify
        "engine/rules/safety_gates.py",
        "engine/rules/evidence_policy.py",
        "engine/rules/classification.py",
        "engine/rules/policy_validator.py",
        "engine/rules/facts.py",
        "engine/llm/advisory.py",
        "engine/security/sanitizer.py",
        "policies/vmware.cbt.yaml",
        "policies/storage.csi.provisioning_timeout.yaml",
        "engine/integrations/local/postgres.py",
    ],
    3: [  # transport + proof the tests are real
        "scripts/run_kafka_agent.py",
        "tests/safety/test_gates.py",
        "tests/kafka/test_contract.py",
        "tests/unit/test_investigation_resilience.py",
        "tests/test_v210_graph.py",
    ],
}

# ---------------------------------------------------------------------------
# Exclusions / inclusions
# ---------------------------------------------------------------------------

EXCLUDE_DIRS = {
    ".git", "__pycache__", ".venv", "venv", "node_modules",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", ".idea", ".vscode",
    "review_bundle",    # this script's own output
    "datasets",         # corpora: large, low review value
    "docs",             # already reviewed
}
HARD_EXCLUDE_FILES = {".env"}           # never, under any flag
EXCLUDE_NAME_SUBSTRINGS = ("codebase",)  # previous bundle artifacts

INCLUDE_EXTS = {".py", ".yaml", ".yml", ".sql", ".toml", ".sh", ".cfg", ".ini"}
INCLUDE_EXACT = {"requirements.txt", ".env.example", ".gitignore", "VERSION"}

# Tail ordering: engine code consolidates right after the tiers.
DIR_WEIGHTS = [
    ("engine/", 0), ("persistence/", 1), ("policies/", 2),
    ("sql/", 3), ("db/", 4), ("config/", 5), ("harness/", 6),
    ("tests/", 7), ("scripts/", 8), ("simulator/", 9),
]

# ---------------------------------------------------------------------------
# Secret scrubbing (applied to every included file; .env excluded outright)
# ---------------------------------------------------------------------------

SECRET_PATTERNS = [
    # ALL-CAPS env-style: PASSWORD=..., API_KEY: ..., AUTH_TOKEN = ...
    # Preserves values that are clearly env-read calls, not literals.
    (re.compile(
        r"(?m)^([A-Z][A-Z0-9_]*(?:PASSWORD|PASSWD|SECRET|TOKEN|APIKEY|API_KEY|PWD)[A-Z0-9_]*)"
        r"\s*[:=]\s*(?![\w.]*(?:environ|getenv)\()(\S.*)$"),
     r"\g<1> = ***REDACTED***"),
    # quoted literals: password="hunter2", secret: 'abc123'
    (re.compile(
        r"""(?i)\b(password|passwd|pwd|secret|api[_-]?key|apikey|client[_-]?secret|"""
        r"""access[_-]?token|auth[_-]?token|private[_-]?key)\b(\s*[:=]\s*)(["'])([^"'\n]{4,})(["'])"""),
     r"\g<1>\g<2>\g<3>***REDACTED***\g<5>"),
    # unquoted YAML-style value at end of line: password: postgres
    (re.compile(
        r"(?im)^([ \t-]*(?:password|passwd|pwd|secret|api[_-]?key|apikey|client[_-]?secret|"
        r"access[_-]?token|auth[_-]?token|private[_-]?key))(\s*:\s*)([A-Za-z0-9_\-./+=@]{6,})[ \t]*$"),
     r"\g<1>\g<2>***REDACTED***"),
    # credentials inside DSNs: postgresql://user:password@host
    (re.compile(
        r"(?i)\b((?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|amqp)://[^:\s/@]+:)([^@\s/]+)(@)"),
     r"\g<1>***REDACTED***\g<3>"),
    # Authorization: Bearer <token>
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9\-._~+/]+=*"), r"\g<1>***REDACTED***"),
    # AWS access key ids
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), "***REDACTED***"),
    # PEM private key blocks
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S),
     "***REDACTED_PRIVATE_KEY***"),
]


def read_and_scrub(path: Path):
    """Return (text, redaction_count) or (None, 0) if unreadable."""
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None, 0
    total = 0
    for rx, repl in SECRET_PATTERNS:
        text, n = rx.subn(repl, text)
        total += n
    return text, total


# ---------------------------------------------------------------------------
# File selection
# ---------------------------------------------------------------------------

def find_root(cli_root):
    candidates = []
    if cli_root:
        candidates.append(Path(cli_root).resolve())
    candidates.extend([Path.cwd(), Path(__file__).resolve().parent.parent])
    for c in candidates:
        if (c / "engine").is_dir() and (c / "persistence").is_dir():
            return c
    return None


def iter_candidates(root: Path, exclude: set, args):
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        rel_p = p.relative_to(root)
        if any(seg in exclude for seg in rel_p.parts[:-1]):
            continue
        if p.name in HARD_EXCLUDE_FILES:
            continue
        if any(s in p.name.lower() for s in EXCLUDE_NAME_SUBSTRINGS):
            continue
        rel = rel_p.as_posix()
        if p.suffix == ".md":
            if args.include_docs or (args.include_skills and rel.startswith("skills/")):
                yield rel
            continue
        if p.suffix in INCLUDE_EXTS or p.name in INCLUDE_EXACT:
            yield rel


def tail_sort_key(rel: str):
    for prefix, w in DIR_WEIGHTS:
        if rel.startswith(prefix):
            return (w, rel)
    return (99, rel)


def build_ordered(root: Path, exclude: set, args):
    """Returns (ordered [(rel, tag)], missing [str], notes [str], tail_count)."""
    ordered, included, missing, notes = [], set(), [], []
    for num in (1, 2, 3):
        for rel in TIERS[num]:
            p = root / rel
            if p.is_file():
                found = rel
            else:  # fallback: locate by basename anywhere in the repo
                hits = [m for m in root.rglob(Path(rel).name) if m.is_file()]
                found = hits[0].relative_to(root).as_posix() if hits else None
            if found is None:
                missing.append(f"TIER {num}: {rel} NOT FOUND")
                continue
            if found != rel:
                notes.append(f"TIER {num}: {rel} -> resolved at {found}")
            if found not in included:
                included.add(found)
                ordered.append((found, f"TIER {num}"))
    tail = [r for r in iter_candidates(root, exclude, args) if r not in included]
    tail.sort(key=tail_sort_key)
    if not args.priority_only:
        ordered.extend((r, "tail") for r in tail)
    return ordered, missing, notes, len(tail)


# ---------------------------------------------------------------------------
# Bundle assembly
# ---------------------------------------------------------------------------

def format_file(rel: str, text: str, tag: str) -> str:
    sha = hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]
    return f"\n# ===== [{tag}] FILE: {rel} ({len(text.splitlines())} lines, sha1:{sha}) =====\n{text}\n"


def split_text(text: str, max_bytes: int):
    """Yield chunks cut at line boundaries, each <= max_bytes where possible."""
    if len(text.encode("utf-8")) <= max_bytes:
        yield text
        return
    chunk, size = [], 0
    for line in text.splitlines(keepends=True):
        ll = len(line.encode("utf-8"))
        if size + ll > max_bytes and chunk:
            yield "".join(chunk)
            chunk, size = [], 0
        chunk.append(line)
        size += ll
    if chunk:
        yield "".join(chunk)


def assemble(entries, budget: int):
    parts, cur, cur_bytes = [], {"files": [], "blocks": []}, 0

    def flush():
        nonlocal cur, cur_bytes
        if cur["files"]:
            parts.append(cur)
        cur, cur_bytes = {"files": [], "blocks": []}, 0

    chunk_budget = max(budget - 800, 4096)
    for rel, text, tag in entries:
        block = format_file(rel, text, tag)
        chunks = list(split_text(block, chunk_budget))
        for i, ch in enumerate(chunks):
            if i > 0:
                ch = f"\n# ===== [{tag}] FILE: {rel} (CONTINUED from previous part) =====\n{ch}"
            b = len(ch.encode("utf-8"))
            if cur_bytes and cur_bytes + b > budget:
                flush()
            cur["files"].append(rel if i == 0 else f"{rel} (cont.)")
            cur["blocks"].append(ch)
            cur_bytes += b
    flush()
    return parts


def write_outputs(root: Path, out_dir: Path, parts, now: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    n = len(parts)
    manifest = [f"part {i:02d}: {f}" for i, part in enumerate(parts, 1) for f in part["files"]]
    written, bodies = [], []
    for i, part in enumerate(parts, 1):
        header = (
            "# " + "=" * 78 + "\n"
            f"# MFA REVIEW BUNDLE — PART {i:02d} OF {n:02d}\n"
            f"# repo: {root.name}    generated: {now}\n"
            "# .env excluded entirely; secret-looking values scrubbed.\n"
            "# Paste parts IN ORDER into the SAME chat, one part per message.\n"
            "# " + "=" * 78 + "\n\n"
            "# FILES IN THIS PART:\n"
            + "\n".join(f"#   {f}" for f in part["files"]) + "\n\n"
        )
        if i == 1:
            header += "# FULL BUNDLE MANIFEST:\n" + "\n".join(f"# {m}" for m in manifest) + "\n\n"
        body = header + "".join(part["blocks"])
        p = out_dir / f"part_{i:02d}_of_{n:02d}.txt"
        p.write_text(body, encoding="utf-8")
        written.append((p.name, len(body.encode())))
        bodies.append(body)
    full = out_dir / "full_bundle.txt"
    full.write_text("".join(bodies), encoding="utf-8")
    return written, full


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", help="repo root (default: auto-detect)")
    ap.add_argument("--out-dir", default="review_bundle")
    ap.add_argument("--max-part-kb", type=int, default=100)
    ap.add_argument("--priority-only", action="store_true", help="only the tier files")
    ap.add_argument("--include-skills", action="store_true", help="include skills/*/skill.md")
    ap.add_argument("--include-docs", action="store_true", help="include all markdown")
    args = ap.parse_args()

    root = find_root(args.root)
    if root is None:
        print("ERROR: repo root not found (need a dir containing engine/ and persistence/). "
              "cd into the repo or pass --root.", file=sys.stderr)
        return 2

    exclude = set(EXCLUDE_DIRS) | {Path(args.out_dir).name}
    ordered, missing, notes, tail_count = build_ordered(root, exclude, args)

    entries, redactions, unreadable = [], [], []
    for rel, tag in ordered:
        text, n = read_and_scrub(root / rel)
        if text is None:
            unreadable.append(rel)
            continue
        if n:
            redactions.append((rel, n))
        entries.append((rel, text, tag))

    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    parts = assemble(entries, args.max_part_kb * 1024)
    if not parts:
        print("ERROR: no files collected — wrong root?", file=sys.stderr)
        return 2
    written, full = write_outputs(root, root / args.out_dir, parts, now)

    total_bytes = sum(sz for _, sz in written)
    print("=" * 70)
    print("MFA REVIEW BUNDLE GENERATED")
    print(f"  repo root      : {root}")
    print(f"  files included : {len(entries)} "
          f"(priority: {sum(1 for _, _, t in entries if t != 'tail')}, tail: {tail_count})")
    print(f"  total size     : {total_bytes/1024:.0f} KB -> {len(parts)} part(s)")
    for name, sz in written:
        print(f"    {root/args.out_dir/name}  ({sz/1024:.0f} KB)")
    print(f"    {full}  (single file, for attachment)")
    if missing:
        print("\n  !! PRIORITY FILES NOT FOUND (check paths):")
        for m in missing:
            print(f"     {m}")
    if notes:
        print("\n  resolved by basename:")
        for nline in notes:
            print(f"     {nline}")
    if redactions:
        print(f"\n  secrets scrubbed: {sum(n for _, n in redactions)} occurrence(s) in "
              f"{len(redactions)} file(s):")
        for rel, n in redactions:
            print(f"     {rel}: {n}")
    else:
        print("\n  secrets scrubbed: none detected")
    if unreadable:
        print("\n  unreadable (skipped): " + ", ".join(unreadable))
    print("=" * 70)
    print("NEXT: paste part_01 into the SAME chat conversation, one part per")
    print("message, in order. If your chat supports attachments, attach")
    print("full_bundle.txt instead. Never paste .env.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
