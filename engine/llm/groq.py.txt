import logging
import os
import requests
from .env import load_dotenv

LOG = logging.getLogger("migration-failure-agent.llm.groq")


class GroqProvider:
    def __init__(self, api_key=None, model=None, base_url=None):
        load_dotenv()
        self.api_key = os.getenv("GROQ_API_KEY") if api_key is None else api_key
        self.model = model or os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
        self.base_url = (base_url or os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1")).rstrip("/")
        # AGENTS.md §7: Per-call timeout of 8.0s
        self.timeout = float(os.getenv("GROQ_TIMEOUT", "8.0"))

    def generate(self, messages, **kwargs):
        if not self.api_key:
            raise RuntimeError("GROQ_API_KEY is required")
        # AGENTS.md §7: Max completion tokens capped at 1024 per call
        requested_tokens = int(kwargs.get("max_tokens", 1000))
        max_tokens = min(1024, max(1, requested_tokens))
        body = {
            "model": self.model,
            "messages": messages,
            "temperature": kwargs.get("temperature", 0),
            "max_tokens": max_tokens,
        }
        if kwargs.get("response_format"):
            body["response_format"] = kwargs["response_format"]
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        try:
            r = requests.post(self.base_url + "/chat/completions", headers=headers, json=body, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            return data["choices"][0]["message"]["content"]
        except requests.RequestException as exc:
            LOG.warning("Groq provider call failed (%s); failing safe to '{}'", exc)
            # AGENTS.md §7: on timeout or provider error, fail safe to '{}'
            return "{}"

