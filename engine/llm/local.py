import os, requests

class LocalOpenAICompatibleProvider:
    def __init__(self, model=None, base_url=None):
        self.model=model or os.getenv('LOCAL_LLM_MODEL','qwen2.5:7b')
        self.base_url=(base_url or os.getenv('LOCAL_LLM_BASE_URL','http://127.0.0.1:11434/v1')).rstrip('/')
    def generate(self,messages,**kwargs):
        r=requests.post(self.base_url+'/chat/completions',json={'model':self.model,'messages':messages,'temperature':kwargs.get('temperature',0)},timeout=120)
        r.raise_for_status(); return r.json()['choices'][0]['message']['content']
