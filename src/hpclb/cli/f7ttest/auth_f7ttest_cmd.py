"""
hpclb auth-site f7ttest subcommand.

Authenticates the current project to the FirecREST v2 test cluster automatically.

All of this assumes the test cluster is up and running locally
and all the services are reachable through the default ports on localhost.
"""

from __future__ import annotations

import pathlib
import uuid

import typer
from typing_extensions import Annotated

from hpclb import project
from hpclb.cli import comms, params
from hpclb.cli.app import auth_site
from hpclb.cli.f7ttest.constants import SITE_DIR_NAME
from hpclb.cli.userdata import get_user_data_dir


@auth_site.command("f7ttest")
def auth_f7ttest(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            parser=params.project_site_present(SITE_DIR_NAME),
        ),
    ],
    offline: Annotated[bool, typer.Option()] = False,
) -> None:
    """Authenticate to CSCS via FirecREST."""
    this = project.Project(path)
    this.offline_mode = offline
    ucomm = comms.Communicator()
    project_config = this.config
    status = ucomm.task(
        "Authenticating to local FirecREST v2 container "
        f"cluster for project '{project_config.name}'"
    )
    status.start()

    auth_id = uuid.uuid4().hex
    secret_file = get_user_data_dir() / f"{auth_id}.f7t"
    current_auth = project.Auth(
        client_id="firecrest-test-clien",
        billing_account="myproject",
        client_secret=str(secret_file),
    )

    machines = project_config.sites[SITE_DIR_NAME].machines
    client_secret = "wZVHVIEd9dkJDh9hMKc6DTvkqXxnDttk"  # noqa: S105  # this is for a local test cluster

    config_file = this.site_dir(SITE_DIR_NAME) / "f7ttest.auth.yaml"
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
    ucomm.report_on_subprocess(config_proc, "authenticate to the test cluster")
    if config_proc.returncode == 0:
        machines[SITE_DIR_NAME] = project.Machine(auth=auth_id)

    this.verdi(["computer", "test", "f7ttest"], stdout=None, stderr=None)

    if config_proc.returncode == 0:
        project_config.sites[SITE_DIR_NAME].auths[auth_id] = current_auth
        this.config = project_config
        secret_file.write_text(client_secret)
        ucomm.report_success("update project config")
    status.stop()
