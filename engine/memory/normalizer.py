"""Failure log and message normalizer.

Extracts variable runtime tokens (UUIDs, NAA device IDs, IP addresses, pod hashes,
and resource names) to generate canonical, stable failure signatures for database indexing.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class NormalizedSignature:
    raw_text: str
    normalized_text: str
    signature_hash: str
    extracted_tokens: Dict[str, List[str]]


class FailureNormalizer:
    """Normalizes raw logs and error strings into canonical failure signatures."""

    # Regex patterns for variable tokens
    _TOKEN_PATTERNS: List[Tuple[str, re.Pattern]] = [
        # MTV / KubeVirt / OpenShift Pod Names with dynamic hash suffixes
        ("POD_NAME", re.compile(r"\b(?:virt-v2v|forklift-controller|cdi-importer|virt-launcher)-[a-z0-9]+-[a-z0-9]+\b")),
        # Dynamic PVC / DataVolume volume identifiers
        ("VOLUME_NAME", re.compile(r"\b(?:migration-[a-z0-9]+-dv-[a-z0-9]+|pvc-[0-9a-fA-F-]{36})\b")),
        # UUIDs
        ("UUID", re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")),
        # Storage NAA Device Identifiers
        ("NAA_ID", re.compile(r"\bnaa\d*\.[0-9a-fA-F]+\b", re.IGNORECASE)),
        # IPv4 Addresses
        ("IP_ADDR", re.compile(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b")),
        # MAC Addresses
        ("MAC_ADDR", re.compile(r"\b(?:[0-9a-fA-F]{2}[:-]){5}[0-9a-fA-F]{2}\b")),
        # Hexadecimal Addresses
        ("HEX_ADDR", re.compile(r"\b0x[0-9a-fA-F]+\b")),
        # Disk Device Paths
        ("DISK_DEVICE", re.compile(r"\b/dev/(?:sd[a-z]\d*|vd[a-z]\d*|nvme\d+n\d+p?\d*)\b")),
    ]

    # ISO Timestamps
    _TIMESTAMP_PATTERN = re.compile(
        r"\b\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b"
    )

    @classmethod
    def normalize(cls, raw_text: str) -> NormalizedSignature:
        if not raw_text:
            return NormalizedSignature(
                raw_text="",
                normalized_text="",
                signature_hash=hashlib.sha256(b"").hexdigest(),
                extracted_tokens={},
            )

        extracted: Dict[str, List[str]] = {}
        working = raw_text

        # Strip timestamps first
        working = cls._TIMESTAMP_PATTERN.sub("<TIMESTAMP>", working)

        # Replace variable tokens with placeholders
        for token_name, pattern in cls._TOKEN_PATTERNS:
            matches = pattern.findall(working)
            if matches:
                extracted[token_name] = matches
                working = pattern.sub(f"<{token_name}>", working)

        # Collapse whitespace
        normalized = " ".join(working.split()).strip()

        # Compute stable signature hash
        sig_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]

        return NormalizedSignature(
            raw_text=raw_text,
            normalized_text=normalized,
            signature_hash=sig_hash,
            extracted_tokens=extracted,
        )
