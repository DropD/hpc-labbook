"""Test the init subcommand."""

from __future__ import annotations

import pathlib
import re
import typing

from hpclb import cli, project

if typing.TYPE_CHECKING:
    from typer.testing import CliRunner


def test_basic_init(tmp_path: pathlib.Path, runner: CliRunner) -> None:
    """Make sure basic usage of the init subcommand behaves as expected."""
    project_path = tmp_path / "project"
    res = runner.invoke(
        cli.app, ["init", "--name", "basic_init", str(project_path), "--offline"]
    )

    assert res.exit_code == 0, res.output
    created = project.Project(project_path)

    assert created.config_file == project_path / "hpclb.yaml"
    assert created.config_file.exists()
    assert (project_path / "pyproject.toml").exists()
    profile_list = created.verdi(["profile", "list"]).stdout.splitlines()
    assert profile_list[0].split()[-1] == str(project_path / ".aiida")
    assert len(profile_list) == 2  # config dir line + presto


def test_init_existing_empty(tmp_path: pathlib.Path, runner: CliRunner) -> None:
    """Check no error when the project path exists but is empty."""
    project_path = tmp_path
    project_path.mkdir(exist_ok=True)
    res = runner.invoke(
        cli.app, ["init", "--name", "empty_init", str(project_path), "--offline"]
    )
    assert res.exit_code == 0, res.output


def test_init_existing_nonempty(tmp_path: pathlib.Path, runner: CliRunner) -> None:
    """Check does not overwrite existing project config."""
    (tmp_path / "hpclb.yaml").touch()
    res = runner.invoke(
        cli.app, ["init", "--name", "existing_init", str(tmp_path), "--offline"]
    )
    assert res.exit_code == 2
    assert re.findall(
        r"Project is already initialized",
        res.output,
        re.MULTILINE,
    )
