import os

import yaml

def get_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"No config found at {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)
