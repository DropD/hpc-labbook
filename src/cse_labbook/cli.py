"""
The labbook CLI.

Commands:
- init: create a new project
"""

from __future__ import annotations

import dataclasses
import enum
import os
import pathlib
import subprocess
import textwrap
import typing

import click
import typer
import yaml
from typing_extensions import Annotated, Self

app = typer.Typer()
app.add_typer(add_site := typer.Typer(), name="add-site")
app.add_typer(auth_site := typer.Typer(), name="auth-site")


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
    """
    Convenience wrapper around subprocess.run for running uv.

    Depending on the env and cwd settings an instance can be constructed to run
    outside projects (e.g. 'uv init') or inside projects (e.g. 'uv add', 'uv run').
    """

    extra_env: dict[str, str] = dataclasses.field(default_factory=dict)
    cwd: pathlib.Path = dataclasses.field(default_factory=pathlib.Path)

    def run(
        self: Self,
        args: list[str],
        **kwargs: typing.Any,  # noqa: ANN401  # just passing through
    ) -> subprocess.CompletedProcess:
        """Run 'uv *' as a sub process."""
        return subprocess.run(  # noqa: S603  # No user input is to be run through this
            ["uv", *args],  # noqa: S607  # If this is not safe, running the hpclb cli isn't either.
            env={"PATH": os.environ["PATH"]} | self.extra_env,
            cwd=self.cwd,
            **kwargs,
        )

    def runrun(
        self: Self,
        args: list[str],
        **kwargs: typing.Any,  # noqa: ANN401  # just passing through
    ) -> subprocess.CompletedProcess:
        """Run 'uv run *' as a sub process."""
        return self.run(["run", *args], **kwargs)


def get_self_depstring() -> str:
    """Return the package name, or top level file url if installed editably."""
    this_file = pathlib.Path(__file__)
    if this_file.parent.parent.name == "site-packages":
        return __package__ or "cse_labbook"
    return str(this_file.parent.parent.parent.absolute())


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
        glob_uv.run(["init", "--no-workspace", "--no-package", str(path)])

    config_file = path / "hpclb.yaml"
    aiida_dir = path / ".aiida"
    loc_uv = Uv(extra_env={"AIIDA_PATH": str(aiida_dir.absolute())}, cwd=path)

    config = {"name": name}

    print("-> writing config file")
    config_file.write_text(yaml.safe_dump(config))

    print("-> adding self to project")
    loc_uv.run(["add", get_self_depstring()])

    print("-> setting up basic AiiDA profile")
    loc_uv.run(["add", "aiida-core"])
    loc_uv.runrun(["verdi", "presto"])


def validate_is_project(path: pathlib.Path) -> pathlib.Path:
    config_file = pathlib.Path(path) / "hpclb.yaml"
    if not config_file.exists:
        msg = f"{path} is not a hpclb project."
        raise typer.Exit(msg)
    return path


def validate_cscs_site_dir_not_present(path: pathlib.Path) -> pathlib.Path:
    """Check the path does not contain a 'cscs' subdirectory."""
    path = validate_is_project(path)

    if (path / "cscs").exists():
        msg = "site 'cscs' already present."
        raise typer.Exit(msg)
    return path


@add_site.command("cscs")
def cscs_add(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            parser=validate_cscs_site_dir_not_present,
        ),
    ],
    work_path: Annotated[
        pathlib.Path,
        typer.Option(
            prompt=True,
            exists=False,
            file_okay=False,
            dir_okay=True,
            help=(
                "By default hpclb will create a subdirectory "
                "in this location to contain all it's work."
            ),
        ),
    ] = pathlib.Path("/capstor/scratch/{username}"),
) -> None:
    """Add support for running on CSCS ALPS."""
    config_file = path / "hpclb.yaml"
    config = yaml.safe_load(config_file.read_text())
    config.setdefault("sites", []).append("cscs")

    aiida_dir = path / ".aiida"
    loc_uv = Uv(extra_env={"AIIDA_PATH": str(aiida_dir.absolute())}, cwd=path)
    site_dir = path / "cscs"

    print("-> preparing compute resource descriptions for site: CSCS")
    site_dir.mkdir()

    loc_uv.run(
        [
            "add",
            "aiida-firecrest @ git+https://github.com/aiidateam/aiida-firecrest.git",
        ]
    )
    fcurls = {
        "santis": "https://api.svc.cscs.ch/cs/firecrest/v2",
        "daint": "https://api.svc.cscs.ch/hpc/firecrest/v2",
        "clariden": "https://api.svc.cscs.ch/ml/firecrest/v2",
    }
    for vcluster in ["santis", "daint", "clariden"]:
        setup = site_dir / f"{vcluster}.setup.yaml"
        setup.write_text(
            yaml.safe_dump(
                {
                    "append_text": "",
                    "default_memory_per_machine": 128000000,
                    "description": vcluster,
                    "hostname": f"{vcluster}.cscs.ch",
                    "label": vcluster,
                    "mpiprocs_per_machine": 72,
                    "mpirun_command": (
                        "srun -n {tot_num_mpiprocs} "
                        "--ntasks-per-node {num_mpiprocs_per_machine}"
                    ),
                    "prepend_text": "",
                    "scheduler": "firecrest",
                    "shebang": "#!/bin/bash -l",
                    "transport": "firecrest",
                    "use_double_quotes": False,
                    "work_dir": str(work_path / "hpclb" / "work"),
                }
            )
        )
        config = site_dir / f"{vcluster}.auth.yaml"
        config.write_text(
            yaml.safe_dump(
                {
                    "compute_resource": vcluster,
                    "temp_directory": str(work_path / "hpclb" / "f7temp"),
                    "token_uri": "https://auth.cscs.ch/auth/realms/firecrest-clients/protocol/openid-connect/token",
                    "url": fcurls[vcluster],
                }
            )
        )
        loc_uv.runrun(
            ["verdi", "computer", "setup", "-n", "--config", str(setup.absolute())]
        )


def validate_f7t_app_exists(value: bool) -> bool:
    """Exit with a helpful message if user has no firecrest app."""
    if not value:
        msg = print(textwrap.dedent(
            """
            You will need a FirecREST application set up to access CSCS ALPS vClusters.
            - Get started: https://docs.cscs.ch/services/devportal/#getting-started
            - Learn more about FirecREST: https://docs.cscs.ch/access/firecrest/
            """
        ))
        raise typer.Exit(msg)

    return value


def validate_cscs_site_dir_present(path: pathlib.Path) -> pathlib.Path:
    """Check the path does not contain a 'cscs' subdirectory."""
    path = validate_is_project(path)

    if not (path / "cscs").exists():
        msg = f"site 'cscs' not added yet, add it with 'hpclb add-site cscs {path}'."
        raise typer.Exit(msg)
    return path


class VCluster(enum.StrEnum):
    CLARIDEN = enum.auto()
    DAINT = enum.auto()
    SANTIS = enum.auto()


@auth_site.command("cscs")
def cscs_auth(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            parser=validate_cscs_site_dir_present,
        ),
    ],
    firecrest: Annotated[
        bool, typer.Option(
            prompt="Do you have an existing FirecREST application?",
            help="Do you have an existing FirecREST application?",
            parser=validate_f7t_app_exists,
        )
    ],
    vcluster: Annotated[
        list[VCluster], typer.Option()
    ],
    client_id: Annotated[str, typer.Option()],
    client_secret: Annotated[str, typer.Option()],
    billing_account: Annotated[str, typer.Option()],
) -> None:
    """Authenticate to CSCS via FirecREST."""
    if not firecrest:
        raise ValueError

    site_dir = path / "cscs"
    aiida_dir = path / ".aiida"

    loc_uv = Uv(extra_env={"AIIDA_PATH": str(aiida_dir.absolute())}, cwd=path)

    for vc in vcluster:
        config_file = site_dir / f"{vc.value.lower()}.auth.yaml"
        loc_uv.runrun([
            "verdi",
            "computer",
            "configure",
            "firecrest",
            vc.value.lower(),
            "-n",
            "--config",
            str(config_file.absolute()),
            "--client-id",
            client_id,
            "--client-secret",
            client_secret,
            "--billing-account",
            billing_account
        ])
        loc_uv.runrun(["verdi", "computer", "test", vc.value.lower()])
