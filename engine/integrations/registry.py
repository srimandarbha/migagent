class RegistryError(Exception): pass
class CapabilityNotFound(RegistryError): pass

class InMemoryCapabilityRegistry:
    def __init__(self, capabilities): self.capabilities = capabilities
    def invoke(self, capability_id, params, access='read', agent='migration-failure'):
        if capability_id not in self.capabilities: raise CapabilityNotFound(capability_id)
        return self.capabilities[capability_id](params)
