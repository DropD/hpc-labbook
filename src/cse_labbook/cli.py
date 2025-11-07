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
import typing
import uuid

import click
import platformdirs
import tomlkit
import typer
import yaml
from typing_extensions import Annotated, Self

from cse_labbook import project

if typing.TYPE_CHECKING:
    import os


__all__ = ["app", "get_user_data_dir"]

USER_DATA_DIR = pathlib.Path(platformdirs.user_data_dir("hpclb", "ricoh"))

app = typer.Typer(name="hpclb")
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
    pyproject = tomlkit.loads(pyproject_file.read_text())
    pyproject.setdefault("tool", {}).setdefault("taskipy", {})["tasks"] = {
        "verdi": "uv run --offline --env-file=.env verdi",
        "upgrade-hpclb": (
            f"uv add --no-cache --upgrade-package cse-labbook {get_self_depstring()} "
            "&& task verdi daemon stop && task verdi daemon start 4"
        ),
    }
    pyproject_file.write_text(tomlkit.dumps(pyproject))

    print(" - setting up the AiiDA profile")
    this.uv.add(["aiida-core"])
    this.verdi(["presto"])
    typer.Exit(0)


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


def validate_f7t_site_dir_not_present(path: pathlib.Path) -> pathlib.Path:
    """Check the path does not contain a 'cscs' subdirectory."""
    path = validate_is_project(path)
    proj = project.Project(pathlib.Path(path))

    if proj.site_dir("f7ttest").exists():
        print("site 'f7ttest' already present.")
        raise typer.Exit(code=1)
    return path


@add_site.command("f7ttest")
def f7ttest_add(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            parser=validate_f7t_site_dir_not_present,
        ),
    ],
    offline: Annotated[bool, typer.Option()] = False,
) -> None:
    """Add support for the firecrest2 test container cluster."""
    this = project.Project(path)
    this.offline_mode = offline
    project_config = this.config
    project_config.sites["f7ttest"] = project.Site(
        docs="https://github.com/eth-cscs/firecrest-v2?tab=readme-ov-file#running-firecrest-v2-with-docker-compose"
    )
    print(
        f"adding local FirecREST v2 container test cluster to project '{project_config.name}'"
    )
    print(" - updating config")
    this.config = project_config

    print(" - preparing compute resource descriptions")
    f7ttest_dir = this.site_dir("f7ttest")
    f7ttest_dir.mkdir()

    this.uv.add(
        [
            "aiida-firecrest @ git+https://github.com/aiidateam/aiida-firecrest.git",
        ]
    )
    print(" - adding compute resource to AiiDA")
    work_path = pathlib.Path("/home/fireuser")
    setup = f7ttest_dir / "f7ttest.setup.yaml"
    setup.write_text(
        yaml.safe_dump(
            {
                "append_text": "",
                "default_memory_per_machine": 2000000,
                "description": "local test cluster",
                "hostname": "localhost",
                "label": "f7ttest",
                "mpiprocs_per_machine": 1,
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
    config = f7ttest_dir / "f7ttest.auth.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "compute_resource": "cluster-slurm-api",
                "temp_directory": str(work_path / "hpclb" / "f7temp"),
                "token_uri": "http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token",
                "url": "http://localhost:8000",
            }
        )
    )
    this.verdi(["computer", "setup", "-n", "--config", str(setup.absolute())])
    print(
        "Ready to authenticate to your FirecREST test cluster "
        "with 'hpclb auth-site f7ttest'. Please make sure you have "
        "started the test cluster with 'docker compose up' or equivalent."
    )


def validate_f7t_site_dir_present(path: pathlib.Path) -> pathlib.Path:
    """Check the path does not contain a 'cscs' subdirectory."""
    path = validate_is_project(path)
    proj = project.Project(pathlib.Path(path))

    if not proj.site_dir("f7ttest").exists():
        print(
            f"site 'f7ttest' not added yet, add it with 'hpclb add-site cscs {path}'."
        )
        raise typer.Exit(code=1)
    return path


@auth_site.command("f7ttest")
def f7ttest_auth(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            parser=validate_f7t_site_dir_present,
        ),
    ],
    offline: Annotated[bool, typer.Option()] = False,
) -> None:
    """Authenticate to CSCS via FirecREST."""
    this = project.Project(path)
    this.offline_mode = offline
    project_config = this.config
    print(
        "Authenticating to local FirecREST v2 container "
        f"cluster for project '{project_config.name}'"
    )

    auth_id = uuid.uuid4().hex
    secret_file = get_user_data_dir() / f"{auth_id}.f7t"
    current_auth = project.Auth(
        client_id="firecrest-test-clien",
        billing_account="myproject",
        client_secret=str(secret_file),
    )

    machines = project_config.sites["f7ttest"].machines
    client_secret = "wZVHVIEd9dkJDh9hMKc6DTvkqXxnDttk"  # noqa: S105  # this is for a local test cluster

    print(" - authenticating to the test cluster")
    config_file = this.site_dir("f7ttest") / "f7ttest.auth.yaml"
    config_proc = this.verdi(
        [
            "computer",
            "configure",
            "firecrest",
            "f7ttest",
            "-n",
            "--config",
            str(config_file.absolute()),
            "--client-id",
            "firecrest-test-client",
            "--client-secret",
            client_secret,
            "--billing-account",
            "myproject",
        ],
        encoding="utf-8",
    )
    if config_proc.returncode != 0:
        print(
            (
                "Something went wrong authenticating to the "
                "test cluster. See ouptut below:\n",
            )
        )
        print(config_proc.stdout)
        print(config_proc.stderr)
    else:
        machines["f7ttest"] = project.Machine(auth=auth_id)

    this.verdi(["computer", "test", "f7ttest"], stdout=None, stderr=None)

    print(" - updating project config")
    if config_proc.returncode == 0:
        project_config.sites["f7ttest"].auths[auth_id] = current_auth
        this.config = project_config
        secret_file.write_text(client_secret)


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


@app.command("run-generic")
def run_generic(
    path: Annotated[
        pathlib.Path,
        typer.Argument(file_okay=True, dir_okay=False, parser=validate_is_project),
    ],
    spec: Annotated[
        pathlib.Path,
        typer.Argument(file_okay=True, dir_okay=False),
    ],
) -> None:
    import os

    from aiida import engine, orm
    from aiida.manage.configuration import (
        load_profile,
    )

    from cse_labbook.aiida import calcjob

    this = project.Project(path)
    os.environ["AIIDA_PATH"] = str(this.aiida_dir)
    p = load_profile(allow_switch=True)
    job = this.load_spec(spec)
    builder = calcjob.GenericCalculation.get_builder()
    builder.code = orm.load_code(job.code)
    if not builder.code.computer:
        builder.metadata.computer = orm.load_computer(job.computer)
    builder.workdir = orm.JsonableData(job.workdir)
    if job.args:
        builder.cmdline_params = job.args
    for name, path in job.uploads.items():
        builder.uploaded[name] = orm.SinglefileData(path)
    builder.metadata.options.resources = job.resources

    if job.uenv:
        current_custom_scheduler_commands: str = (
            builder.metadata.options.custom_scheduler_commands  # type: ignore[attr-defined]
        )
        lines = current_custom_scheduler_commands.splitlines()
        uenv_line = f"#SBATCH --uenv={job.uenv.name}"
        if job.uenv.view:
            uenv_line = f"{uenv_line} --view={job.uenv.view}"
        lines.append(uenv_line)
        builder.metadata.options.custom_scheduler_commands = "\n".join(lines)  # type: ignore[attr-defined]

    builder.metadata.label = job.label
    builder.metadata.description = job.description
    builder.metadata.options.environment_variables = job.envvars
    builder.metadata.options.prepend_text = "\n".join(job.setup_script)
    builder.metadata.options.append_text = "\n".join(job.cleanup_script)
    builder.metadata.options.queue_name = job.queue
    builder.metadata.options.withmpi = job.withmpi

    node = engine.submit(builder)
    print(node.pk)

    group, _ = orm.groups.Group.collection.get_or_create(label=this.config.name)

    group.add_nodes(node)

    extras = orm.extras.EntityExtras(node)
    for key, value in job.extras.items():
        extras.set(key, value)
