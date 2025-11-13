"""
hpclb run-generic CLI command.

Reads a generic calculation spec from a manifest yaml file
and submits it to the AiiDA engine.
"""

from __future__ import annotations

import os
import pathlib
import typing

import typer
from aiida import engine, orm
from aiida.manage.configuration import load_profile
from typing_extensions import Annotated

from hpclb import project
from hpclb.aiida import calcjob
from hpclb.cli import params
from hpclb.cli.app import app


@app.command("run-generic")
def run_generic(
    path: Annotated[
        pathlib.Path,
        typer.Argument(file_okay=True, dir_okay=False, parser=params.path_is_project),
    ],
    spec: Annotated[
        pathlib.Path,
        typer.Argument(file_okay=True, dir_okay=False),
    ],
) -> None:
    """Run a "Generic" job on a compute resource in your project."""
    this = project.Project(path)
    os.environ["AIIDA_PATH"] = str(this.aiida_dir)
    load_profile(allow_switch=True)
    job = this.load_spec(spec)
    builder: typing.Any = calcjob.GenericCalculation.get_builder()
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
