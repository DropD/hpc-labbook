"""hpclb jobs CLI to launch the process browser."""

from __future__ import annotations

import os
import pathlib

import typer
from typing_extensions import Annotated

from hpclb import project
from hpclb.cli import app, params
from hpclb.tui import ProcessBrowser


@app.app.command("jobs")
def browse_jobs(
    path: Annotated[
        pathlib.Path,
        typer.Argument(file_okay=True, dir_okay=False, parser=params.path_is_project),
    ],
) -> None:
    """Launch the process browser."""
    this = project.Project(path)
    os.environ.setdefault("AIIDA_PATH", str(this.aiida_dir.absolute().resolve()))
    tui = ProcessBrowser()
    tui.run()
