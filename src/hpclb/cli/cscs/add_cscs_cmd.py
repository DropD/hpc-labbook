"""
hpclb add-site cscs subcommand.

Adds a directory with template yaml files for adding CSCS clusters
to the project as AiiDA computers. Then let's you pick which ones to add.
"""

from __future__ import annotations

import pathlib

import typer
import yaml
from typing_extensions import Annotated

from hpclb import project
from hpclb.cli import params
from hpclb.cli.app import add_site


@add_site.command("cscs")
def add_cscs(
    path: Annotated[
        pathlib.Path,
        typer.Argument(
            file_okay=False,
            dir_okay=True,
            writable=True,
            parser=params.path_site_absent("cscs"),
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
