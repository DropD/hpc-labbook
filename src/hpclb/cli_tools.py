"""API wrappers around CLI tools used under the hood."""

from __future__ import annotations

import dataclasses
import os
import pathlib
import subprocess
import typing

from typing_extensions import Self

__all__ = ["Uv", "Verdi"]


@dataclasses.dataclass
class CliToolMixin:
    """Common lowlevel methods for CLI tools."""

    def run_subprocess(
        self: Self,
        args: list[str],
        **kwargs: typing.Any,  # noqa: ANN401  # just passing through
    ) -> subprocess.CompletedProcess:
        """Directly run the subprocess with minimum wrapping."""
        return subprocess.run(  # noqa: S603  # This is meant to run commands for the user on the user's machine.
            [self.name, *args], **self.populate_default_kwargs(kwargs)
        )

    def __call__(
        self: Self,
        args: list[str],
        **kwargs: typing.Any,  # noqa: ANN401  # just passing through
    ) -> subprocess.CompletedProcess:
        """Run the tool in a generic way."""
        return self.run_subprocess(args, **kwargs)

    def populate_default_kwargs(
        self, popen_options: dict[str, typing.Any]
    ) -> dict[str, typing.Any]:
        popen_options.setdefault("capture_output", True)
        popen_options.setdefault("encoding", "utf-8")
        popen_options["env"] = self.env | popen_options.setdefault("env", {})
        popen_options.setdefault("cwd", self.cwd)
        return popen_options

    @property
    def env(self) -> dict[str, str]:
        return {"PATH": os.environ["PATH"]}

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def cwd(self) -> pathlib.Path | None:
        return None


@dataclasses.dataclass
class PythonCliTool(CliToolMixin):
    """Run python tools under UV."""

    project: pathlib.Path

    @property
    def cwd(self: Self) -> pathlib.Path:
        return self.project

    def run_subprocess(
        self: Self,
        args: list[str],
        **kwargs: typing.Any,  # noqa: ANN401  # just passing through
    ) -> subprocess.CompletedProcess:
        return subprocess.run(  # noqa: S603  # the whole point of this is to limit the commands that are run.
            ["uv", "run", self.name, *args],  # noqa: S607  # if this is insecure then running hpclb is insecure in the first place.
            **self.populate_default_kwargs(kwargs),
        )


@dataclasses.dataclass
class Uv(CliToolMixin):
    """
    Convenience wrapper around subprocess.run for running uv.

    Depending on the env and cwd settings an instance can be constructed to run
    outside projects (e.g. 'uv init') or inside projects (e.g. 'uv add', 'uv run').
    """

    project: pathlib.Path = dataclasses.field(default_factory=pathlib.Path)
    offline: bool = False

    def run_subprocess(
        self: Self,
        args: list[str],
        **kwargs: typing.Any,  # noqa: ANN401  # just passing through
    ) -> subprocess.CompletedProcess:
        """Directly run the subprocess with minimum wrapping."""
        if self.offline:
            args = ["--offline", *args]
        return super().run_subprocess(args, **kwargs)

    @property
    def name(self: Self) -> str:
        """The name for the 'uv' command."""
        return "uv"

    @property
    def cwd(self: Self) -> pathlib.Path:
        """The default working directory for this 'uv' instance."""
        return self.project

    def init(self: Self) -> subprocess.CompletedProcess:
        """Run 'uv init' to initialize a project."""
        return self.run_subprocess(
            ["init", "--no-workspace", "--no-package", str(self.project)], cwd=None
        )

    def add(self: Self, args: list[str]) -> subprocess.CompletedProcess:
        """Run 'uv add' in the context of a project."""
        if self.offline and "--frozen" not in args:
            args = ["--frozen", *args]
        return self.run_subprocess(["add", *args])

    def run(
        self: Self,
        args: list[str],
        **kwargs: typing.Any,  # noqa: ANN401  # just passing through
    ) -> subprocess.CompletedProcess:
        """Run 'uv run *' as a sub process."""
        return self.run_subprocess(["run", *args], **kwargs)


@dataclasses.dataclass
class Verdi(PythonCliTool):
    """Run verdi in the context of a project."""

    @property
    def name(self: Self) -> str:
        """Name of the 'verdi' cli."""
        return f"{self.project}/.venv/bin/verdi"

    @property
    def env(self: Self) -> dict[str, str]:
        """
        Default environment for VERDI to run in.

        This ensures the AIIDA_PATH is always set to the project's.
        """
        return {"PATH": os.environ["PATH"]} | {
            "AIIDA_PATH": str((self.project / ".aiida").absolute())
        }
