"""Load config.yaml (bundled with the package, overridable via SIM_CONFIG)."""

import os
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG = Path(__file__).with_name("config.yaml")


def load_config(path: Path | None = None) -> dict[str, Any]:
    path = path or Path(os.environ.get("SIM_CONFIG", DEFAULT_CONFIG))
    with path.open() as f:
        return yaml.safe_load(f)


def section(cfg: dict[str, Any], name: str) -> dict[str, Any]:
    """A config section merged over the shared `business` settings."""
    return {**cfg["business"], **cfg[name]}
