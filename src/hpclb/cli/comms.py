"""User communication utils for the hpclb commandline."""

from __future__ import annotations

import dataclasses
import textwrap
import typing

import rich.console
import rich.markdown

if typing.TYPE_CHECKING:
    import subprocess as sp

__all__ = ["Communicator"]


@dataclasses.dataclass
class Communicator:
    """Standardize user communication from the hpclb cli."""

    console: rich.console.Console = dataclasses.field(
        default_factory=rich.console.Console
    )

    def task(self, msg: str) -> rich.console.Status:
        """Communicate a longe running task is being carried out."""
        return self.console.status(msg)

    def report_success(self, msg: str) -> None:
        """Communicate something was successfully completed."""
        self.console.print(textwrap.indent(msg, prefix=" ✅ "))

    def report_fail(self, msg: str) -> None:
        """Communicate something was successfully completed."""
        self.console.print(textwrap.indent(msg, prefix=" ❌ "))

    def next_step(self, msg: str) -> None:
        """Communicate that there is a likely followup step."""
        self.console.print(rich.markdown.Markdown(textwrap.dedent(msg)))

    def report_on_subprocess(self, completed: sp.CompletedProcess, msg: str) -> None:
        """Communicate success or faillure on a completed subprocess."""
        if completed.returncode == 0:
            self.report_success(msg)
        else:
            self.report_fail(f"{msg}, details below:")
            self.console.print(completed.stdout)
            self.console.print(completed.stderr)
