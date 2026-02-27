import yaml
from pathlib import Path
from typing import Any, Dict

def load_config(config_path: str) -> Dict[str, Any]:
    """Load training configuration from a YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    return config or {}
