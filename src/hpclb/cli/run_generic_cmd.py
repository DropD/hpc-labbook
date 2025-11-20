"""
hpclb run-generic CLI command.

Reads a generic calculation spec from a manifest yaml file
and submits it to the AiiDA engine.
"""

from __future__ import annotations

import os
import pathlib

import typer
from aiida.manage.configuration import load_profile
from typing_extensions import Annotated

from hpclb import project
from hpclb.cli import params
from hpclb.cli.app import app


@app.command("run-generic")
def run_generic(
    path: Annotated[
        pathlib.Path,
        typer.Argument(file_okay=True, dir_okay=False, parser=params.path_is_project),
    ],
    spec: Annotated[
        pathlib.Path,
        typer.Argument(file_okay=True, dir_okay=False),
    ],
) -> None:
    """Run a "Generic" job on a compute resource in your project."""
    this = project.Project(path)
    os.environ["AIIDA_PATH"] = str(this.aiida_dir)
    load_profile(allow_switch=True)
    job = this.load_spec(spec)
    job.groups.append(this.config.name)
    node = job.submit()
    print(node.pk)
