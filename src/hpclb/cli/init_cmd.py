"""The hpclb init command."""

from __future__ import annotations

import pathlib

import tomlkit
import typer
from typing_extensions import Annotated

from hpclb import project
from hpclb.cli.app import app
from hpclb.cli.params import path_is_not_project

__all__ = ["init"]


def get_self_depstring() -> str:
    """Return the package name, or top level file url if installed editably."""
    this_file = pathlib.Path(__file__)
    if this_file.parent.parent.name == "site-packages":
        return __package__ or "hpclb"
    return str(this_file.parent.parent.parent.absolute())


@app.command()
def init(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False, dir_okay=True, writable=True, parser=path_is_not_project
        ),
    ],
    name: Annotated[str, typer.Option(prompt=True)],
    offline: Annotated[bool, typer.Option()] = False,
) -> None:
    """Start a new computer simulation project labbook in PATH."""
    this = project.Project(path)
    if not this.path.exists():
        this.path.mkdir(parents=True)

    if offline:
        this.offline_mode = True

    print(f"Initializing new project '{name}' in {path}")
    pyproject_file = path / "pyproject.toml"
    if not (pyproject_file).exists():
        print(" - setting up a python environment")
        this.uv.init()

    print(" - writing the initial hpclb config")
    this.config = project.Config(name=name)

    print(" - adding minimum python dependencies")
    this.uv.add([get_self_depstring(), "taskipy"])

    print(" - adding cli wrappers and shortcuts")
    env_file = path / ".env"
    env_file.write_text("AIIDA_PATH=.aiida")
    activate_script_path = this.path.absolute().resolve() / ".venv" / "bin" / "activate"
    pyproject = tomlkit.loads(pyproject_file.read_text())
    pyproject.setdefault("tool", {}).setdefault("taskipy", {})["tasks"] = {
        "verdi": "uv run --offline --env-file=.env verdi",
        "upgrade-hpclb": (
            f"uv add --no-cache --upgrade-package hpc-labbook {get_self_depstring()} "
            "&& task verdi daemon stop && task verdi daemon start 4"
        ),
        "activate": (
            f"echo 'source {activate_script_path}; "
            f"export AIIDA_PATH={this.aiida_dir.absolute().resolve()}'"
        ),
    }
    pyproject_file.write_text(tomlkit.dumps(pyproject))

    print(" - setting up the AiiDA profile")
    this.uv.add(["aiida-core", "aiida-pythonjob"])
    this.verdi(["presto"])
    typer.Exit(0)
