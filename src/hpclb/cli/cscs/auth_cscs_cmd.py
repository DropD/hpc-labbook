"""
hpclb auth-site cscs subcommand.

Authenticates the current project to CSCS ALPS clusters automatically.

A FireCREST application is required. Let's you choose which cluster to authenticate to.
"""

from __future__ import annotations

import enum
import pathlib
import textwrap
import uuid

import typer
from typing_extensions import Annotated

from hpclb import project
from hpclb.cli import comms, params
from hpclb.cli.app import auth_site
from hpclb.cli.cscs.constants import SITE_DIR_NAME
from hpclb.cli.userdata import get_user_data_dir


class VCluster(enum.StrEnum):
    """ALPS vClusters."""

    CLARIDEN = enum.auto()
    DAINT = enum.auto()
    SANTIS = enum.auto()


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


@auth_site.command("cscs")
def auth_cscs(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            parser=params.project_site_present(SITE_DIR_NAME),
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
    this = project.Project(path)
    ucomm = comms.Communicator()
    project_config = this.config
    status = ucomm.task(
        f"Authenticating to compute site CSCS for project '{project_config.name}'"
    )
    status.start()

    if not firecrest:
        raise typer.Exit(code=2)

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
        if config_proc.returncode == 0:
            machines[vc.value.lower()] = project.Machine(auth=auth_id)
        ucomm.report_on_subprocess(config_proc, f"authenticate to {vc.value.lower()}")

        this.verdi(["computer", "test", vc.value.lower()], stdout=None, stderr=None)

    if any(rc == 0 for rc in retcodes):
        project_config.sites["cscs"].auths[auth_id] = current_auth
        this.config = project_config
        secret_file.write_text(client_secret)
        ucomm.report_fail("update project config")
    status.stop()
