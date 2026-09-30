from typing import Any, Protocol

class LLMProvider(Protocol):
    def generate(self, messages: list[dict[str, Any]], **kwargs: Any) -> str: ...
