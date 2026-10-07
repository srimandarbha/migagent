from unittest.mock import MagicMock, patch
import pytest
import requests

from engine.llm import build_llm_provider
from engine.llm.groq import GroqProvider


def test_groq_provider_missing_key():
    provider = GroqProvider(api_key="")
    with pytest.raises(RuntimeError, match="GROQ_API_KEY is required"):
        provider.generate([{"role": "user", "content": "hi"}])


def test_groq_provider_generate_and_governance():
    provider = GroqProvider(api_key="mock-groq-key-12345", model="llama-3.3-70b-versatile")
    assert provider.timeout == 8.0

    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "choices": [{"message": {"content": "Hello from Groq"}}]
    }
    mock_resp.raise_for_status = MagicMock()

    with patch("requests.post", return_value=mock_resp) as mock_post:
        # Request 2048 tokens: must be capped at 1024
        res = provider.generate([{"role": "user", "content": "hi"}], max_tokens=2048)
        assert res == "Hello from Groq"
        assert mock_post.called
        kwargs = mock_post.call_args[1]
        assert kwargs["headers"]["Authorization"] == "Bearer mock-groq-key-12345"
        assert kwargs["json"]["model"] == "llama-3.3-70b-versatile"
        assert kwargs["json"]["max_tokens"] == 1024
        assert kwargs["timeout"] == 8.0


def test_groq_provider_fail_safe_on_error():
    provider = GroqProvider(api_key="mock-groq-key-12345")
    with patch("requests.post", side_effect=requests.exceptions.Timeout("Connection timed out")):
        res = provider.generate([{"role": "user", "content": "hi"}])
        assert res == "{}"


def test_build_llm_provider_groq(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "groq")
    monkeypatch.setenv("GROQ_API_KEY", "mock-groq-key-12345")
    provider = build_llm_provider()
    assert isinstance(provider, GroqProvider)

