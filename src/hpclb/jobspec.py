"""Data structures describing hpclb jobs."""

from __future__ import annotations

import dataclasses
import pathlib
import typing

from aiida import engine, orm
from typing_extensions import Self

from hpclb.aiida import calcjob, data

if typing.TYPE_CHECKING:
    from aiida.engine.processes import ProcessBuilder


class ComputerNotFoundError(Exception):
    """No computer found on user input job spec."""

    def __str__(self: Self) -> str:
        """Format error message."""
        return (
            "Job Specification did not contain a "
            "computer, either directly or in the code."
        )


@dataclasses.dataclass
class Uenv:
    """An ALPS user environment, image must have been pulled previously."""

    name: str
    view: str = ""


@dataclasses.dataclass
class Generic:
    """Represent Generic CalcJob."""

    code: str
    workdir: data.TargetDir
    label: str
    description: str
    queue: str
    envvars: dict[str, str] = dataclasses.field(default_factory=dict)
    extras: dict[str, str] = dataclasses.field(default_factory=dict)
    setup_script: list[str] = dataclasses.field(default_factory=list)
    cleanup_script: list[str] = dataclasses.field(default_factory=list)
    resources: dict[str, int] = dataclasses.field(default_factory=dict)
    uploads: dict[str, pathlib.Path] = dataclasses.field(default_factory=dict)
    args: list[str] = dataclasses.field(default_factory=list)
    withmpi: bool = True
    computer: str | None = None
    uenv: Uenv | None = None
    groups: list[str] = dataclasses.field(default_factory=list)

    def load_code(self: Self) -> orm.Code:
        """Load the code from AiiDA DB."""
        return orm.load_code(self.code)

    def load_computer(self: Self) -> orm.Computer:
        """Load the computer from AiiDA DB."""
        if self.computer:
            return orm.load_computer(self.computer)
        if comp_from_code := self.load_code().computer:
            return comp_from_code
        raise ComputerNotFoundError

    def to_builder(self: Self) -> ProcessBuilder:
        """Create a GenericCalcjob builder from this instance."""
        builder: typing.Any = calcjob.GenericCalculation.get_builder()
        builder.code = self.load_code()
        if not builder.code.computer:
            builder.metadata.computer = self.load_computer()
        builder.workdir = orm.JsonableData(self.workdir)
        if self.args:
            builder.cmdline_params = self.args
        for name, path in self.uploads.items():
            builder.uploaded[name] = orm.SinglefileData(path)
        builder.metadata.options.resources = self.resources

        if self.uenv:
            current_custom_scheduler_commands: str = (
                builder.metadata.options.custom_scheduler_commands  # type: ignore[attr-defined]
            )
            lines = current_custom_scheduler_commands.splitlines()
            uenv_line = f"#SBATCH --uenv={self.uenv.name}"
            if self.uenv.view:
                uenv_line = f"{uenv_line} --view={self.uenv.view}"
            lines.append(uenv_line)
            builder.metadata.options.custom_scheduler_commands = "\n".join(lines)  # type: ignore[attr-defined]

        builder.metadata.label = self.label
        builder.metadata.description = self.description
        builder.metadata.options.environment_variables = self.envvars
        builder.metadata.options.prepend_text = "\n".join(self.setup_script)
        builder.metadata.options.append_text = "\n".join(self.cleanup_script)
        builder.metadata.options.queue_name = self.queue
        builder.metadata.options.withmpi = self.withmpi
        return builder

    def submit(self: Self) -> orm.CalcJobNode:
        """Submit the job and add groups and annotations."""
        node = typing.cast(orm.CalcJobNode, engine.submit(self.to_builder()))
        for group_name in self.groups:
            group, _ = orm.groups.Group.collection.get_or_create(label=group_name)
            group.add_nodes(node)
        for key, value in self.extras.items():
            node.base.extras.set(key, value)
        return node
