"""
hpclb add-site cscs subcommand.

Adds a directory with template yaml files for adding CSCS clusters
to the project as AiiDA computers. Then uses them to add the resources.
"""

from __future__ import annotations

import pathlib

import typer
import yaml
from typing_extensions import Annotated

from hpclb import project
from hpclb.cli import comms, params
from hpclb.cli.app import add_site
from hpclb.cli.cscs.constants import SITE_DIR_NAME


@add_site.command("cscs")
def add_cscs(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            parser=params.project_site_absent(SITE_DIR_NAME),
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
                "Allowed placeholders: {username}."
            ),
        ),
    ] = pathlib.Path("/capstor/scratch/cscs/{username}"),
    offline: Annotated[bool, typer.Option()] = False,
) -> None:
    """Add support for running on CSCS ALPS."""
    this = project.Project(path)
    this.offline_mode = offline
    ucomm = comms.Communicator()
    project_config = this.config
    project_config.sites[SITE_DIR_NAME] = project.Site(docs="https://docs.cscs.ch")
    status = ucomm.task(f"adding compute site CSCS to project '{project_config.name}'")
    status.start()
    this.config = project_config
    ucomm.report_success("updated config")

    work_path = pathlib.Path(str(work_path).format(username=username))

    cscs_dir = this.site_dir(SITE_DIR_NAME)
    cscs_dir.mkdir()

    res = this.uv.add(
        [
            "aiida-firecrest @ git+https://github.com/aiidateam/aiida-firecrest.git",
            *(["--frozen"] if offline else []),
        ]
    )
    ucomm.report_on_subprocess(res, "add aiida-firecrest dependency")
    fcurls = {
        "santis": "https://api.svc.cscs.ch/cw/firecrest/v2",
        "daint": "https://api.svc.cscs.ch/hpc/firecrest/v2",
        "clariden": "https://api.svc.cscs.ch/ml/firecrest/v2",
    }
    ucomm.report_success(
        f"prepared compute resource descriptions in {this.site_dir(SITE_DIR_NAME)}"
    )
    vclusters = ["clariden", "daint", "santis"]
    status.update(f"adding compute resources {vclusters} to AiiDA")
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
        res = this.verdi(["computer", "setup", "-n", "--config", str(setup.absolute())])
        ucomm.report_on_subprocess(res, f"add '{vcluster}' compute resource")
    status.stop()
    ucomm.next_step(
        f"""
        **Ready to authenticate to your CSCS compute resources with**

        ```bash
        hpclb auth-site cscs {this.path}
        ```
        """
    )
