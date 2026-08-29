from pathlib import Path

import yaml

def load_config(config_path: str) -> dict:
    """
    Load an environment-specific YAML configuration.

    Keeping configuration outside the application allows us to
    use the same code in dev, staging, and production.
    """
    path = Path(config_path)
    
    if not path.exists():
        raise FileNotFoundError(
            f"Cofiguration file not found: {path}"
        )
    
    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    
    if not config:
        raise ValueError(
            f"Configuration file is empty: {path}"
        )
    
    return config