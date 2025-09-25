"""
The labbook CLI.

Commands:
- init: create a new project
- add-site: add infrastructure for a compute site (only CSCS included)
- auth-site: authenticate to a compute site's resources
"""

from __future__ import annotations

import enum
import pathlib
import textwrap
import tomllib
import typing
import uuid

import click
import platformdirs
import typer
import yaml
from typing_extensions import Annotated, Self

from cse_labbook import project

if typing.TYPE_CHECKING:
    import os


__all__ = ["app", "get_user_data_dir"]

USER_DATA_DIR = pathlib.Path(platformdirs.user_data_dir("hpclb", "ricoh"))

app = typer.Typer()
app.add_typer(add_site := typer.Typer(), name="add-site")
app.add_typer(auth_site := typer.Typer(), name="auth-site")


def get_user_data_dir() -> pathlib.Path:
    """Retrieve data dir, ensuring it exists."""
    if not USER_DATA_DIR.exists():
        USER_DATA_DIR.mkdir(parents=True)
    return USER_DATA_DIR


class ProjectPath(click.Path):
    """A path that can not contain the project file."""

    name = "HPCLBProjectPath"

    def convert(
        self: Self,
        value: str | os.PathLike[str],
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> pathlib.Path:
        """Check for presence of project file on tope of path validation."""
        _ = super().convert(value, param, ctx)
        result = pathlib.Path(value)

        config_file = result / "hpclb.yaml"
        if result.exists() and config_file in result.iterdir():
            msg = "Project is already initialized."
            raise click.BadParameter(msg)
        return result


def get_self_depstring() -> str:
    """Return the package name, or top level file url if installed editably."""
    this_file = pathlib.Path(__file__)
    if this_file.parent.parent.name == "site-packages":
        return __package__ or "cse_labbook"
    return str(this_file.parent.parent.parent.absolute())


@app.command()
def init(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            click_type=ProjectPath(file_okay=False, dir_okay=True, writable=True)
        ),
    ],
    name: Annotated[str, typer.Option(prompt=True)],
) -> None:
    """Start a new computer simulation project labbook in PATH."""
    this = project.Project(path)
    if not this.path.exists():
        this.path.mkdir(parents=True)

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
    pyproject = tomllib.loads(pyproject_file.read_text())
    pyproject["tool"]["taskipy"]["tasks"] = {
        "verdi": "uv run --env-file=.env",
        "upgrade-hpclb": (
            f"uv add --no-cache --upgrade-package cse-labbook {get_self_depstring()} "
            "&& task verdi daemon stop && task verdi daemon start 4"
        ),
    }

    print(" - setting up the AiiDA profile")
    this.uv.add(["aiida-core"])
    this.verdi(["presto"])


def validate_is_project(path: pathlib.Path) -> pathlib.Path:
    """Check the path contains a project config file."""
    proj = project.Project(pathlib.Path(path))
    if not proj.config_file.exists():
        print(f"{path} is not a hpclb project.")
        raise typer.Exit(code=1)
    return path


def validate_cscs_site_dir_not_present(path: pathlib.Path) -> pathlib.Path:
    """Check the path does not contain a 'cscs' subdirectory."""
    path = validate_is_project(path)
    proj = project.Project(pathlib.Path(path))

    if proj.site_dir("cscs").exists():
        print("site 'cscs' already present.")
        raise typer.Exit(code=1)
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
    username: Annotated[str, typer.Option(prompt=True)],
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
    ] = pathlib.Path("/capstor/scratch/cscs/{username}"),
) -> None:
    """Add support for running on CSCS ALPS."""
    this = project.Project(path)
    project_config = this.config
    project_config.sites["cscs"] = project.Site(docs="https://docs.cscs.ch")
    print(f"adding compute site CSCS to project '{project_config.name}'")
    print(" - updating config")
    this.config = project_config

    work_path = pathlib.Path(str(work_path).format(username=username))

    print(" - preparing compute resource descriptions")
    cscs_dir = this.site_dir("cscs")
    cscs_dir.mkdir()

    this.uv.add(
        [
            "aiida-firecrest @ git+https://github.com/aiidateam/aiida-firecrest.git",
        ]
    )
    fcurls = {
        "santis": "https://api.cscs.ch/cw/firecrest/v2",
        "daint": "https://api.cscs.ch/hpc/firecrest/v2",
        "clariden": "https://api.cscs.ch/ml/firecrest/v2",
    }
    vclusters = ["clariden", "daint", "santis"]
    print(f" - adding compute resources {vclusters} to AiiDA")
    for vcluster in vclusters:
        setup = cscs_dir / f"{vcluster}.setup.yaml"
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
        config = cscs_dir / f"{vcluster}.auth.yaml"
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
        this.verdi(["computer", "setup", "-n", "--config", str(setup.absolute())])
    print(
        "Ready to authenticate to your CSCS "
        "compute resources with 'hpclb auth-site cscs'"
    )


def validate_f7t_app_exists(value: bool) -> bool:
    """Exit with a helpful message if user has no firecrest app."""
    if not value:
        print(
            textwrap.dedent(
                """
            You will need a FirecREST application set up to access CSCS ALPS vClusters.
            - Get started: https://docs.cscs.ch/services/devportal/#getting-started
            - Learn more about FirecREST: https://docs.cscs.ch/access/firecrest/
            """
            )
        )
        raise typer.Exit(code=0)

    return value


def validate_cscs_site_dir_present(path: pathlib.Path) -> pathlib.Path:
    """Check the path does not contain a 'cscs' subdirectory."""
    path = validate_is_project(path)
    proj = project.Project(pathlib.Path(path))

    if not proj.site_dir("cscs").exists():
        print(f"site 'cscs' not added yet, add it with 'hpclb add-site cscs {path}'.")
        raise typer.Exit(code=1)
    return path


class VCluster(enum.StrEnum):
    """ALPS vClusters."""

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
        bool,
        typer.Option(
            prompt="Do you have an existing FirecREST application?",
            help="Do you have an existing FirecREST application?",
            parser=validate_f7t_app_exists,
        ),
    ],
    vcluster: Annotated[list[VCluster], typer.Option(default_factory=list)],
    client_id: Annotated[str, typer.Option(prompt=True)],
    client_secret: Annotated[str, typer.Option(prompt=True)],
    billing_account: Annotated[str, typer.Option(prompt=True)],
) -> None:
    """Authenticate to CSCS via FirecREST."""
    if not firecrest:
        raise ValueError

    this = project.Project(path)
    project_config = this.config
    print(f"Authenticating to compute site CSCS for project '{project_config.name}'")

    if not vcluster:
        vcluster_str = typer.prompt(
            "Comma separated list of vclusters ('clariden', 'daint', 'santis') or"
            " 'all' ['all']"
        )
        if vcluster_str == "all":
            vcluster = [VCluster.CLARIDEN, VCluster.DAINT, VCluster.SANTIS]
        else:
            vcluster = [
                VCluster.__members__[name.strip().upper()]
                for name in vcluster_str.split(",")
            ]

    auth_id = uuid.uuid4().hex
    secret_file = get_user_data_dir() / f"{auth_id}.f7t"
    current_auth = project.Auth(
        client_id=client_id,
        billing_account=billing_account,
        client_secret=str(secret_file),
    )

    machines = project_config.sites["cscs"].machines
    retcodes = []
    for vc in vcluster:
        print(f" - authenticating to {vc.value.lower()}")
        config_file = this.site_dir("cscs") / f"{vc.value.lower()}.auth.yaml"
        config_proc = this.verdi(
            [
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
                billing_account,
            ]
        )
        retcodes.append(config_proc.returncode)
        if config_proc.returncode != 0:
            print(f"Something went wrong authenticating to {vc}. See ouptut below:\n")
            print(config_proc.stdout)
        else:
            machines[vc.value.lower()] = project.Machine(auth=auth_id)

        this.verdi(["computer", "test", vc.value.lower()], stdout=None, stderr=None)

    print(" - updating project config")
    if any(rc == 0 for rc in retcodes):
        project_config.sites["cscs"].auths[auth_id] = current_auth
        this.config = project_config
        secret_file.write_text(client_secret)
