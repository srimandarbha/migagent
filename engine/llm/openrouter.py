import os, requests

class OpenRouterProvider:
    def __init__(self, api_key=None, model=None, base_url=None):
        self.api_key=api_key or os.getenv('OPENROUTER_API_KEY')
        self.model=model or os.getenv('OPENROUTER_MODEL','openrouter/free')
        self.base_url=(base_url or os.getenv('OPENROUTER_BASE_URL','https://openrouter.ai/api/v1')).rstrip('/')
    def generate(self,messages,**kwargs):
        if not self.api_key: raise RuntimeError('OPENROUTER_API_KEY is required')
        r=requests.post(self.base_url+'/chat/completions',headers={'Authorization':f'Bearer {self.api_key}','Content-Type':'application/json'},json={'model':self.model,'messages':messages,'temperature':kwargs.get('temperature',0)},timeout=120)
        r.raise_for_status(); return r.json()['choices'][0]['message']['content']
