"""Test the init subcommand."""

import pathlib
import re

import pytest
from typer.testing import CliRunner

from cse_labbook import cli, project


@pytest.fixture(scope="session")
def runner() -> CliRunner:
    """One cli runner is enough."""
    return CliRunner()


def test_basic_init(tmp_path: pathlib.Path, runner: CliRunner) -> None:
    """Make sure basic usage of the init subcommand behaves as expected."""
    project_path = tmp_path / "project"
    res = runner.invoke(
        cli.app, ["init", "--name", "basic_init", str(project_path), "--offline"]
    )

    assert res.exit_code == 0
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
    assert res.exit_code == 0


def test_init_existing_nonempty(tmp_path: pathlib.Path, runner: CliRunner) -> None:
    """Check does not overwrite existing project config."""
    (tmp_path / "hpclb.yaml").touch()
    res = runner.invoke(
        cli.app, ["init", "--name", "existing_init", str(tmp_path), "--offline"]
    )
    assert res.exit_code == 2
    assert re.findall(
        r"Invalid value for 'PATH': Project is already initialized",
        res.output,
        re.MULTILINE,
    )
