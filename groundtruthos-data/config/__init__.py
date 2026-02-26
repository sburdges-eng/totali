from pathlib import Path
import yaml

CONFIG_DIR = Path(__file__).parent
SOURCES_PATH = CONFIG_DIR / "sources.yaml"


def load_sources() -> dict:
    """Load dataset source configurations."""
    with open(SOURCES_PATH) as f:
        return yaml.safe_load(f)
