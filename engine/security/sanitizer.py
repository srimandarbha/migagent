"""Deterministic secret and credential sanitizer for the Migration Failure Agent.

Prevents leakage of sensitive passwords, bearer tokens, API keys, private keys,
and infrastructure credentials to external LLM providers or unencrypted audit traces.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Set, Union

SECRET_KEY_NAMES: Set[str] = {
    "password",
    "passwd",
    "pwd",
    "secret",
    "client_secret",
    "api_key",
    "apikey",
    "token",
    "access_token",
    "refresh_token",
    "auth",
    "authorization",
    "private_key",
    "certificate",
    "cert",
    "key",
}

# Compiled regex patterns for secret masking
PATTERNS = [
    # URLs with embedded user:password (e.g. postgresql://user:pass@host:5432)
    (re.compile(r'([a-zA-Z][a-zA-Z0-9+.-]*://[^:\s@]+):([^@\s/]+)(@)'), r'\1:***REDACTED_PASSWORD***\3'),
    # Bearer tokens in headers or logs
    (re.compile(r'(?i)\bbearer\s+[a-zA-Z0-9_\-\.]{15,}'), 'Bearer ***REDACTED_TOKEN***'),
    # Authorization basic/bearer headers
    (re.compile(r'(?i)\bauthorization\s*:\s*(basic|bearer)\s+[^\s,;]+'), r'Authorization: \1 ***REDACTED***'),
    # Standard JWT tokens (three base64 segments)
    (re.compile(r'\bey[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}'), '***REDACTED_JWT***'),
    # Common API key formats (sk-..., ghp_..., etc.)
    (re.compile(r'\bsk-[a-zA-Z0-9_\-]{20,}\b'), 'sk-***REDACTED_API_KEY***'),
    (re.compile(r'\bghp_[a-zA-Z0-9]{30,}\b'), 'ghp_***REDACTED_TOKEN***'),
    # Key-value secret assignments in configs/logs: password="...", secret=..., etc.
    (re.compile(r'(?i)\b(password|passwd|pwd|secret|client_secret|api_key|token)\s*[:=]\s*["\']?([^"\'\s,;&]+)["\']?'), r'\1=***REDACTED***'),
    # PEM Private keys
    (re.compile(r'-----BEGIN [A-Z ]*PRIVATE KEY-----[^-]+-----END [A-Z ]*PRIVATE KEY-----', re.DOTALL), '***REDACTED_PRIVATE_KEY***'),
]


class SecretSanitizer:
    """Recursively scrubs secrets, credentials, and tokens from text and nested data structures."""

    @staticmethod
    def sanitize_text(text: str) -> str:
        if not text or not isinstance(text, str):
            return text
        result = text
        for pattern, replacement in PATTERNS:
            result = pattern.sub(replacement, result)
        return result

    @classmethod
    def sanitize_object(cls, obj: Any) -> Any:
        if obj is None:
            return None
        if isinstance(obj, str):
            return cls.sanitize_text(obj)
        if isinstance(obj, dict):
            sanitized_dict: Dict[str, Any] = {}
            for k, v in obj.items():
                k_str = str(k).lower().strip()
                # If key matches known sensitive secret keywords and value is a scalar, redact entirely
                if not isinstance(v, (dict, list, tuple, set)) and any(sec in k_str for sec in SECRET_KEY_NAMES):
                    sanitized_dict[k] = "***REDACTED***"
                else:
                    sanitized_dict[k] = cls.sanitize_object(v)
            return sanitized_dict
        if isinstance(obj, (list, tuple, set)):
            sanitized_list = [cls.sanitize_object(item) for item in obj]
            return type(obj)(sanitized_list)
        return obj


def sanitize_text(text: str) -> str:
    """Convenience helper to scrub secrets from text."""
    return SecretSanitizer.sanitize_text(text)


def sanitize_object(obj: Any) -> Any:
    """Convenience helper to scrub secrets recursively from dictionaries, lists, or strings."""
    return SecretSanitizer.sanitize_object(obj)
