"""
hpclb add-site f7ttest subcommand.

Adds a directory with template yaml files for adding the test cluster
to the project as AiiDA computers. Then let's you pick which ones to add.

All of this assumes the test cluster is up and running locally
and all the services are reachable through the default ports on localhost.
"""

from __future__ import annotations

import pathlib

import typer
import yaml
from typing_extensions import Annotated

from hpclb import project
from hpclb.cli import comms, params
from hpclb.cli.app import add_site
from hpclb.cli.f7ttest.constants import SITE_DIR_NAME


@add_site.command("f7ttest")
def add_f7ttest(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            parser=params.project_site_absent(SITE_DIR_NAME),
        ),
    ],
    offline: Annotated[bool, typer.Option()] = False,
) -> None:
    """Add support for the firecrest2 test container cluster."""
    this = project.Project(path)
    this.offline_mode = offline
    ucomm = comms.Communicator()
    project_config = this.config
    project_config.sites[SITE_DIR_NAME] = project.Site(
        docs="https://github.com/eth-cscs/firecrest-v2?tab=readme-ov-file#running-firecrest-v2-with-docker-compose"
    )
    status = ucomm.task(
        "adding local FirecREST v2 container test "
        f"cluster to project '{project_config.name}'"
    )
    status.start()
    this.config = project_config
    ucomm.report_success("update config")

    f7ttest_dir = this.site_dir(SITE_DIR_NAME)
    f7ttest_dir.mkdir()

    res = this.uv.add(
        [
            "aiida-firecrest @ git+https://github.com/aiidateam/aiida-firecrest.git",
            *(["--frozen"] if offline else []),
        ]
    )
    ucomm.report_on_subprocess(res, "add aiida-firecrest dependency")
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
    ucomm.report_success("prepare compute resource description")
    res = this.verdi(["computer", "setup", "-n", "--config", str(setup.absolute())])
    ucomm.report_on_subprocess(res, "add compute resource to AiiDA")
    ucomm.next_step(
        f"""
        **Ready to authenticate to your FirecREST test cluster with**
        ```bash
        hpclb auth-site f7ttest {this.path}
        ````
        Please **make sure** you have started the test cluster with `docker compose up`
        or equivalent."""
    )
    status.stop()
