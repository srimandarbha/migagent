import logging
import os
import requests
from .env import load_dotenv

LOG = logging.getLogger("migration-failure-agent.llm.openrouter")


class OpenRouterProvider:
    def __init__(self, api_key=None, model=None, base_url=None):
        load_dotenv()
        self.api_key = api_key or os.getenv('OPENROUTER_API_KEY')
        self.model = model or os.getenv('OPENROUTER_MODEL', 'openrouter/free')
        self.base_url = (base_url or os.getenv('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1')).rstrip('/')
        # AGENTS.md §7: Per-call timeout of 8.0s
        self.timeout = float(os.getenv('OPENROUTER_TIMEOUT', '8.0'))

    def generate(self, messages, **kwargs):
        if not self.api_key:
            raise RuntimeError('OPENROUTER_API_KEY is required')
        requested_tokens = int(kwargs.get('max_tokens', 1000))
        max_tokens = min(1024, max(1, requested_tokens))
        body = {
            'model': self.model,
            'messages': messages,
            'temperature': kwargs.get('temperature', 0),
            'max_tokens': max_tokens,
        }
        if kwargs.get('response_format'):
            body['response_format'] = kwargs['response_format']
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            'HTTP-Referer': os.getenv('OPENROUTER_HTTP_REFERER', 'http://localhost'),
            'X-Title': os.getenv('OPENROUTER_X_TITLE', 'Migration Failure Agent'),
        }
        try:
            r = requests.post(self.base_url + '/chat/completions', headers=headers, json=body, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            return data['choices'][0]['message']['content']
        except requests.RequestException as exc:
            LOG.warning("OpenRouter provider call failed (%s); failing safe to '{}'", exc)
            return "{}"

