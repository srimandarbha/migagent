from pathlib import Path
import os


def load_dotenv(path=None):
    """Minimal .env loader. Existing environment variables always win."""
    p = Path(path or os.getenv('MFA_DOTENV_PATH', Path.cwd() / '.env'))
    if not p.is_file():
        return False
    for raw in p.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[7:].lstrip()
        if '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        os.environ[key] = value
    return True
