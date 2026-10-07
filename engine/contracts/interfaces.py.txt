from typing import Any, Dict, Protocol

class CapabilityRegistry(Protocol):
    def invoke(self, capability_id: str, params: Dict[str, Any], access: str, agent: str) -> Dict[str, Any]: ...

class Tool(Protocol):
    capability_id: str
    def run(self, params: Dict[str, Any]) -> Dict[str, Any]: ...
