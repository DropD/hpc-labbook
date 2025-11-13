"""The hpclb init command."""

from __future__ import annotations

import pathlib

import tomlkit
import typer
from typing_extensions import Annotated

import hpclb
from hpclb import project
from hpclb.cli import comms
from hpclb.cli.app import app
from hpclb.cli.params import path_is_not_project

__all__ = ["init"]


def get_self_depstring() -> str:
    """Return the package name, or top level file url if installed editably."""
    toplevel = pathlib.Path(hpclb.__file__)
    if toplevel.parent.parent.name == "site-packages":
        return __package__ or "hpclb"
    return str(toplevel.parent.parent.parent.absolute())


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
    this.offline_mode = offline
    ucomm = comms.Communicator()
    if not this.path.exists():
        this.path.mkdir(parents=True)

    status = ucomm.task(f"initializing new project '{name}' in {path}")
    status.start()
    pyproject_file = path / "pyproject.toml"
    if not (pyproject_file).exists():
        status.update("setting up a python environment")
        res = this.uv.init()
        ucomm.report_on_subprocess(res, "set up a python environment")
        status.update(f"initializing new project {name} in {path}")

    this.config = project.Config(name=name)
    ucomm.report_success("created hpclb config")

    res = this.uv.add(["uv", get_self_depstring(), "taskipy"])
    ucomm.report_on_subprocess(res, "added minimum python dependencies")
    if res.returncode != 0:
        status.stop()
        raise typer.Exit(code=1)

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
    ucomm.report_success("add cli wrappers and shortcuts")

    status.update("setting up the AiiDA profile")
    res = this.uv.add(["aiida-core", "aiida-pythonjob"])
    ucomm.report_on_subprocess(res, "add minimum python dependencies")
    if res.returncode != 0:
        status.stop()
        raise typer.Exit(code=1)
    res = this.verdi(["presto"])
    ucomm.report_on_subprocess(res, "create an AiiDA profile")
    status.stop()
    typer.Exit(0)
