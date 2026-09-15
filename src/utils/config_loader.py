from pathlib import Path
from typing import Any, Dict
import yaml


def load_config(config_path: str = "config/pipeline_config.yaml") -> Dict[str, Any]:
    """Loads YAML configuration file into a dictionary."""
    # Resolve relative to project root directly
    root_dir = Path(__file__).resolve().parents[2]
    target_path = Path(config_path)

    if not target_path.is_absolute():
        target_path = root_dir / config_path

    if not target_path.exists():
        raise FileNotFoundError(f"Config file not found at {target_path}")

    with open(target_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    cfg = load_config()
    print("Configuration successfully loaded:")
    print(f"App: {cfg['app']['name']}")
    print(f"Batch Size: {cfg['ingestion']['per_page']} assets")