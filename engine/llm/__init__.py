from .interface import LLMProvider
from .openrouter import OpenRouterProvider
from .local import LocalOpenAICompatibleProvider

def build_llm_provider(provider=None):
    import os
    name=(provider or os.getenv('LLM_PROVIDER','none')).lower()
    if name=='openrouter': return OpenRouterProvider()
    if name in ('local','ollama'): return LocalOpenAICompatibleProvider()
    return None
