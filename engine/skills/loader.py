from pathlib import Path
class SkillNotFound(Exception): pass
class SkillRepository:
    def __init__(self, root): self.root=Path(root)
    def load(self, skill_id):
        p=self.root/skill_id/'skill.md'
        if not p.is_file(): raise SkillNotFound(skill_id)
        return p.read_text(encoding='utf-8')
    def exists(self, skill_id): return (self.root/skill_id/'skill.md').is_file()
