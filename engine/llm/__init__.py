from .interface import LLMProvider
from .openrouter import OpenRouterProvider
from .local import LocalOpenAICompatibleProvider
from .env import load_dotenv


def build_llm_provider(provider=None):
    load_dotenv()
    import os
    name=(provider or os.getenv('LLM_PROVIDER','none')).lower()
    if name=='openrouter': return OpenRouterProvider()
    if name in ('local','ollama'): return LocalOpenAICompatibleProvider()
    return None
