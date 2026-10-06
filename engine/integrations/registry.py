class RegistryError(Exception): pass
class CapabilityNotFound(RegistryError): pass
class ContractNotSupported(RegistryError): pass

class InMemoryCapabilityRegistry:
    def __init__(self, capabilities, supported_contracts=None):
        self.capabilities = dict(capabilities)
        self.supported_contracts = set(supported_contracts) if supported_contracts is not None else None

    def list_capabilities(self):
        return sorted(self.capabilities)

    def available_capabilities(self):
        return set(self.capabilities)

    def has(self, capability_id: str) -> bool:
        return capability_id in self.capabilities

    def supports_contract(self, capability_id: str, domain: str = None, signal: str = None) -> bool:
        if capability_id not in self.capabilities:
            return False
        if self.supported_contracts is not None:
            return (
                (capability_id, domain, signal) in self.supported_contracts
                or (domain, signal) in self.supported_contracts
                or capability_id in self.supported_contracts
            )
        adapter = self.capabilities.get(capability_id)
        if hasattr(adapter, "supported_contracts") and adapter.supported_contracts is not None:
            contracts = getattr(adapter, "supported_contracts")
            return (capability_id, domain, signal) in contracts or (domain, signal) in contracts
        if hasattr(adapter, "supported_signals") and adapter.supported_signals is not None:
            signals = getattr(adapter, "supported_signals")
            return (domain, signal) in signals
        return True

    def invoke(self, capability_id, params, access='read', agent='migration-failure'):
        if capability_id not in self.capabilities: raise CapabilityNotFound(capability_id)
        return self.capabilities[capability_id](params)

