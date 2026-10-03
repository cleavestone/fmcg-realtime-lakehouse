"""CLI entry point: python -m simulator <seed | run | scenario>."""

import logging
from pathlib import Path
from typing import Annotated

import typer

from simulator.config import load_config, section

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

    run_seed(section(load_config(config), "seed"))


@app.command()
def run(
    rate: Annotated[
        float | None, typer.Option(envvar="SIM_RATE", help="Mean events per second")
    ] = None,
    duration: Annotated[
        float | None, typer.Option(help="Stop after this many seconds (default: run forever)")
    ] = None,
    seed: Annotated[
        int | None, typer.Option(envvar="SIM_SEED", help="Random seed for event choices")
    ] = None,
    config: ConfigOption = None,
) -> None:
    """Run the live business: weighted events, one transaction each, until stopped."""
    from simulator.runner import Runner

    cfg = section(load_config(config), "simulator")
    Runner(
        cfg,
        rate=rate if rate is not None else cfg["rate"],
        duration=duration,
        seed=seed if seed is not None else cfg["random_seed"],
    ).run()


@app.command()
def scenario(
    name: Annotated[str, typer.Argument(help="Scenario name")],
    config: ConfigOption = None,
) -> None:
    """Run a deterministic scenario and print the ids touched and expected end state."""
    from simulator.scenarios import SCENARIOS, run_scenario

    if name not in SCENARIOS:
        raise typer.BadParameter(f"unknown scenario {name!r}; choose from {', '.join(SCENARIOS)}")
    cfg = load_config(config)
    # Scenario output is for humans: keep library logging quiet.
    logging.getLogger().setLevel(logging.WARNING)
    ok = run_scenario(name, section(cfg, "simulator"), cfg["scenarios"])
    raise typer.Exit(0 if ok else 1)


if __name__ == "__main__":
    app()
