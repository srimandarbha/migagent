from __future__ import annotations

import json
import re
from typing import Any

SYSTEM_PROMPT = """You are an advisory knowledge-applicability judge for an SRE migration-failure agent.
You do not make safety, approval, execution, or final applicability decisions.
Use only the structured evidence supplied by the user. Do not invent versions, product behavior,
Red Hat statements, or historical outcomes.

Your task is to adjudicate ONLY ambiguous knowledge applicability when deterministic checks returned UNKNOWN.
Return JSON only with:
{
  "verdict": "LIKELY_APPLICABLE" | "LIKELY_NOT_APPLICABLE" | "INCONCLUSIVE",
  "confidence": 0.0-1.0,
  "basis": ["short factual observations"],
  "validation_needed": ["specific read-only validation items"]
}
An explicit deterministic version conflict or retired/conflicting source is outside your authority and
must never be converted into LIKELY_APPLICABLE.
"""


def build_messages(environment: dict[str, Any], failure: dict[str, Any], candidates: list[dict[str, Any]], history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload = {
        "environment": environment,
        "failure": failure,
        "candidates": candidates,
        "sre_history": history,
    }
    from ..security.sanitizer import sanitize_object
    sanitized_payload = sanitize_object(payload)
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(sanitized_payload, default=str)},
    ]


def parse_judge(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    raw = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL).strip()
    raw = re.sub(r'^```(?:json)?\s*', '', raw)
    raw = re.sub(r'\s*```$', '', raw)
    try:
        data = json.loads(raw)
    except Exception:
        start, end = raw.find("{"), raw.rfind("}")
        if start < 0 or end <= start:
            return {"status": "INVALID", "raw": raw}
        try:
            data = json.loads(raw[start:end + 1])
        except Exception:
            return {"status": "INVALID", "raw": raw}
    if not isinstance(data, dict):
        return {"status": "INVALID", "raw": raw}
    verdict = str(data.get("verdict", "INCONCLUSIVE")).upper()
    if verdict not in {"LIKELY_APPLICABLE", "LIKELY_NOT_APPLICABLE", "INCONCLUSIVE"}:
        verdict = "INCONCLUSIVE"
    try:
        confidence = max(0.0, min(1.0, float(data.get("confidence", 0.0))))
    except Exception:
        confidence = 0.0
    basis = data.get("basis", [])
    validation = data.get("validation_needed", [])
    if not isinstance(basis, list): basis = [str(basis)] if basis else []
    if not isinstance(validation, list): validation = [str(validation)] if validation else []
    return {
        "status": "JUDGED",
        "verdict": verdict,
        "confidence": confidence,
        "basis": [str(x) for x in basis[:8]],
        "validation_needed": [str(x) for x in validation[:8]],
    }
