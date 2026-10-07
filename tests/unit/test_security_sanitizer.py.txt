import pytest
from engine.security.sanitizer import sanitize_text, sanitize_object, SecretSanitizer
from engine.llm.advisory import build_messages
from engine.contracts import AgentState


def test_sanitize_text_passwords_and_urls():
    url = "postgresql://sre_user:SuperSecretPassword123!@postgres.prod:5432/mfa_db"
    sanitized = sanitize_text(url)
    assert "SuperSecretPassword123!" not in sanitized
    assert "***REDACTED_PASSWORD***" in sanitized
    assert "postgresql://sre_user:***REDACTED_PASSWORD***@postgres.prod:5432/mfa_db" == sanitized


def test_sanitize_text_bearer_and_jwt_tokens():
    text = "Authorization failed with Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c on vCenter host"
    sanitized = sanitize_text(text)
    assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" not in sanitized
    assert "***REDACTED_JWT***" in sanitized or "***REDACTED_TOKEN***" in sanitized


def test_sanitize_text_api_keys():
    text = "Failed to call OpenRouter using sk-or-v1-abcdef0123456789abcdef0123456789"
    sanitized = sanitize_text(text)
    assert "abcdef0123456789" not in sanitized
    assert "sk-***REDACTED_API_KEY***" in sanitized


def test_sanitize_object_recursive():
    data = {
        "cluster_id": "ocp-01",
        "auth": {
            "password": "MySuperSecretPassword",
            "vcenter_user": "administrator@vsphere.local",
        },
        "nested_list": [
            "Normal log entry",
            "Error with Bearer abcdef1234567890abcdef1234567890 on pod",
        ],
        "safe_int": 42,
    }
    sanitized = sanitize_object(data)
    assert sanitized["auth"]["password"] == "***REDACTED***"
    assert sanitized["auth"]["vcenter_user"] == "administrator@vsphere.local"
    assert "Bearer ***REDACTED_TOKEN***" in sanitized["nested_list"][1]
    assert sanitized["safe_int"] == 42


def test_llm_advisory_messages_are_sanitized():
    state = AgentState(
        failure_case_id="case-sec-01",
        event={
            "event_id": "ev-sec-01",
            "message": "Migration failed: error connecting to https://vcenter.internal with password=AdminSecretPass! and token=ghp_abcdef0123456789abcdef0123456789",
            "failure_code": "vmware.credentials.unauthorized",
        },
    )
    messages = build_messages(state)
    user_content = messages[1]["content"]

    assert "AdminSecretPass!" not in user_content
    assert "ghp_abcdef0123456789abcdef0123456789" not in user_content
    assert "***REDACTED***" in user_content or "***REDACTED_TOKEN***" in user_content
