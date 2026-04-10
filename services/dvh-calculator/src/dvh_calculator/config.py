"""Load application settings and YAML config for the DVH calculator."""

import os
from pathlib import Path

from imaging_common import load_settings, load_yaml_config

CONFIG_PATH = Path(os.getenv("CONFIG_PATH", str(Path(__file__).parents[2] / "config" / "config.yaml")))
settings = load_settings(CONFIG_PATH)
yaml_config = load_yaml_config(CONFIG_PATH)
