from pathlib import Path
import yaml

DEFAULT_DATASET = Path(__file__).resolve().parents[3] / 'datasets' / 'common_dataset.yaml'

def load_common_dataset(path=None):
    p = Path(path or DEFAULT_DATASET)
    with p.open() as f:
        return yaml.safe_load(f) or {}
