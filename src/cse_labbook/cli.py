"""
The labbook CLI.

Commands:
- init: create a new project
"""
from __future__ import annotations

import enum
import dataclasses
import pathlib
import subprocess
import shlex
import typing
import os

import click
import typer
import yaml
from typing_extensions import Annotated, Self

app = typer.Typer()
app.add_typer(add_site := typer.Typer(), name="add-site")


class ProjectPath(click.Path):
    """A path that can not contain the project file."""

    name = "HPCLBProjectPath"

    def convert(
        self: Self, value: str, param: click.ParamType, ctx: click.Context
    ) -> pathlib.Path:
        """Check for presence of project file on tope of path validation."""
        result = pathlib.Path(super().convert(value, param, ctx))

        config_file = result / "hpclb.yaml"
        if result.exists() and config_file in result.iterdir():
            msg = "Project is already initialized."
            raise click.BadParameter(msg)
        return result


class Site(enum.StrEnum):
    """Available sites."""
    CSCS = enum.auto()


@dataclasses.dataclass
class Uv:
    extra_env: dict[str, str] = dataclasses.field(default_factory=dict)
    cwd: pathlib.Path = dataclasses.field(default_factory=pathlib.Path)

    def run(self, args: list[str], **opts: typing.Any) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["uv", *args],
            env={"PATH": os.environ["PATH"]} | self.extra_env,
            cwd=self.cwd,
            **opts
        )

    def runrun(self, args:list[str], **opts: typing.Any) -> subprocess.CompletedProcess:
        return self.run(["run", *args], **opts)


@app.command()
def init(
    path: Annotated[
        ProjectPath,
        typer.Argument(
            click_type=ProjectPath(file_okay=False, dir_okay=True, writable=True)
        ),
    ],
    name: Annotated[str, typer.Option(prompt=True)],
) -> None:
    """Start a new computer simulation project labbook in PATH."""
    if not path.exists():
        path.mkdir(parents=True)

    glob_uv = Uv()

    print("-> setting up the uv project")
    if not (path / "pyproject.toml").exists():
        glob_uv.run(["init", "--no-package", str(path)])

    config_file = path / "hpclb.yaml"
    aiida_dir = path / ".aiida"
    loc_uv = Uv(extra_env = {"AIIDA_PATH": str(aiida_dir.absolute())}, cwd=path)

    config = {"name": name}

    print("-> writing config file")
    config_file.write_text(yaml.safe_dump(config))

    print("-> adding self to project")
    hpclb_path = pathlib.Path(__file__).parent.parent.parent
    loc_uv.run(["add", str(hpclb_path)])

    print("-> setting up basic AiiDA profile")
    loc_uv.run(["add", "aiida-core"])
    loc_uv.runrun(["verdi", "presto"])


def validate_cscs_site_dir_not_present(path: pathlib.Path) -> pathlib.Path:
    if (pathlib.Path(path) / "cscs").exists():
        raise typer.Exit("site 'cscs' already present.")
    return path


@add_site.command()
def cscs(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False, dir_okay=True, writable=True, parser=validate_cscs_site_dir_not_present
        ),
    ],
    work_path: Annotated[pathlib.Path, typer.Option(prompt=True, exists=False, file_okay=False, dir_okay=True)],
):
    """Add support for running on CSCS ALPS."""
    config_file = path / "hpclb.yaml"
    config = yaml.safe_load(config_file.read_text())
    config.setdefault("sites", []).append("cscs") 

    aiida_dir = path / ".aiida"
    loc_uv = Uv(extra_env = {"AIIDA_PATH": str(aiida_dir.absolute())}, cwd=path)
    site_dir = path / "cscs"
    # TODO(ricoh): split out into an entry point interface or command group

    print("-> preparing compute resource descriptions for site: CSCS")
    site_dir.mkdir()

    # TODO(ricoh): setup and possibly configure clusters
    # run verdi with "AIIDA_PATH" set to the project dir
    loc_uv.run(["add", "aiida-firecrest @ git+https://github.com/aiidateam/aiida-firecrest.git"])
    # TODO(ricoh): do not assume this is being run from the repo in the future
    fcurls = {
        "santis": "https://api.svc.cscs.ch/cs/firecrest/v2",
        "daint": "https://api.svc.cscs.ch/hpc/firecrest/v2",
        "clariden": "https://api.svc.cscs.ch/ml/firecrest/v2",
    }
    for vcluster in ["santis", "daint", "clariden"]:
        setup = site_dir / f"{vcluster}.setup.yaml"
        setup.write_text(yaml.safe_dump({
            "append_text": "",
            "default_memory_per_machine": 128000000,
            "description": vcluster,
            "hostname": f"{vcluster}.cscs.ch",
            "label": vcluster,
            "mpiprocs_per_machine": 72,
            "mpirun_command": f"srun -n {{tot_num_mpiprocs}} --ntasks-per-node {{num_mpiprocs_per_machine}}",
            "prepend_text": "",
            "scheduler": "firecrest",
            "shebang": "#!/bin/bash -l",
            "transport": "firecrest",
            "use_double_quotes": False,
            "work_dir": f"{work_path}/hpclb/work"
        }))
        config = site_dir / f"{vcluster}.config.yaml"
        config.write_text(yaml.safe_dump({
            "compute_resource": vcluster,
            "temp_directory": f"{work_path}/hpclb/f7temp",
            "token_uri": "https://auth.cscs.ch/auth/realms/firecrest-clients/protocol/openid-connect/token",
            "url": fcurls[vcluster]
        }))
        loc_uv.runrun([
            "verdi", "computer", "setup", "-n", "--config",
            str(setup.absolute())
        ])
