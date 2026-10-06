"""Global test configuration and fixtures."""
import pytest

from engine.rules.evidence_policy import clear_policy_cache


@pytest.fixture(autouse=True)
def reset_policy_cache():
    """Clear policy LRU cache before each test run to ensure test isolation."""
    clear_policy_cache()
    yield
    clear_policy_cache()
