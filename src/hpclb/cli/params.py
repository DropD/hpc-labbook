"""Parameter types for the hpclb commandline."""

from __future__ import annotations

import pathlib
import typing

import rich.console
import typer

from hpclb import project
from hpclb.cli import comms

if typing.TYPE_CHECKING:
    pass


__all__ = [
    "path_is_not_project",
    "path_is_project",
    "project_site_absent",
    "project_site_present",
]


def path_is_not_project(param: pathlib.Path | str) -> pathlib.Path:
    """Validate the given path parameter is not already an initialized hpclb project."""
    console = rich.console.Console()
    this = project.Project(pathlib.Path(param))
    if this.config_file.exists():
        console.print("Project is already initialized.", style="red")
        raise typer.Exit(code=2)
    return this.path


def path_is_project(path: pathlib.Path | str) -> pathlib.Path:
    """Check the path contains a project config file."""
    console = rich.console.Console()
    this = project.Project(pathlib.Path(path))
    if not this.config_file.exists():
        console.print(f"{path} is not a hpclb project.", style="red")
        raise typer.Exit(code=2)
    return this.path


def exit_on_uninitialized_project(
    proj: project.Project, console: rich.console.Console
) -> None:
    """Exit with a helpful error message on an uninitialized project."""
    if not proj.config_file.exists():
        console.print(
            (f"{proj.path.absolute().resolve()} is not an initialized hpclb project."),
            style="red",
        )
        console.print(
            f"initialize it with:\nhpclb init {proj.path.absolute().resolve()}",
            style="grey",
        )
        typer.Exit(code=2)


def project_site_present(
    site_name: str,
) -> typing.Callable[[pathlib.Path | str], pathlib.Path]:
    """Build a typer path param parser for a project with the given site present."""

    def validator(path: pathlib.Path | str) -> pathlib.Path:
        """Check the project contains the given site dir."""
        this = project.Project(pathlib.Path(path))
        ucomm = comms.Communicator()

        exit_on_uninitialized_project(this, ucomm.console)

        if not this.site_dir(site_name).exists():
            ucomm.next_step(
                f"""
                site '{site_name}' not added yet, add it with 

                ```bash
                hpclb add-site {site_name} {this.path}
                ```
                """
            )
            raise typer.Exit(code=2)
        return this.path

    return validator


def project_site_absent(
    site_name: str,
) -> typing.Callable[[pathlib.Path | str], pathlib.Path]:
    """Build a typer path param parser for a project without the given site."""

    def validator(path: pathlib.Path | str) -> pathlib.Path:
        """Check the project does not contain the given site dir."""
        this = project.Project(pathlib.Path(path))
        console = rich.console.Console()

        exit_on_uninitialized_project(this, console)

        if this.site_dir(site_name).exists():
            console.print(f"site '{site_name}' already present.", style="red")
            raise typer.Exit(code=2)
        return this.path

    return validator
