import os, requests
from .env import load_dotenv

class LocalOpenAICompatibleProvider:
    def __init__(self, model=None, base_url=None):
        load_dotenv()
        self.model=model or os.getenv('LOCAL_LLM_MODEL','unsloth/Phi-4-mini-reasoning-GGUF:Q4_K_M')
        self.base_url=(base_url or os.getenv('LOCAL_LLM_BASE_URL','http://127.0.0.1:8080/v1')).rstrip('/')
        self.timeout=float(os.getenv('LOCAL_LLM_TIMEOUT','240'))
    def generate(self,messages,**kwargs):
        r=requests.post(self.base_url+'/chat/completions',json={'model':self.model,'messages':messages,'temperature':kwargs.get('temperature',0),'max_tokens':kwargs.get('max_tokens',1000)},timeout=self.timeout)
        r.raise_for_status(); return r.json()['choices'][0]['message']['content']
