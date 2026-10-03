"""CLI entry point: python -m simulator <command>."""

import logging
from pathlib import Path
from typing import Annotated

import typer

from simulator.config import load_config

app = typer.Typer(no_args_is_help=True, add_completion=False)

ConfigOption = Annotated[
    Path | None,
    typer.Option("--config", help="Path to config.yaml (default: bundled, or $SIM_CONFIG)"),
]


@app.callback()
def main() -> None:
    """FMCG business simulator."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s level=%(levelname)s logger=%(name)s %(message)s",
    )


@app.command()
def seed(config: ConfigOption = None) -> None:
    """Load deterministic master data and historical orders (no-op if already seeded)."""
    from simulator.seed import run_seed

    run_seed(load_config(config)["seed"])


if __name__ == "__main__":
    app()
