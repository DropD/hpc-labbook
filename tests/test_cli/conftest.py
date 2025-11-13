"""Fixtures for CLI tests."""

from __future__ import annotations

import pathlib

import pytest
import typer.testing

from hpclb import cli, project


@pytest.fixture(scope="session")
def runner() -> typer.testing.CliRunner:
    """One cli runner is enough."""
    return typer.testing.CliRunner()


@pytest.fixture
def empty_project_offline(
    tmp_path: pathlib.Path, runner: typer.testing.CliRunner
) -> project.Project:
    """Provide an initialized project with nothing else going on yet."""
    runner.invoke(
        cli.app, ["init", "--name", tmp_path.name, str(tmp_path), "--offline"]
    )
    return project.Project(tmp_path)


@pytest.fixture
def empty_project(
    tmp_path: pathlib.Path, runner: typer.testing.CliRunner
) -> project.Project:
    """Provide an initialized project with nothing else going on yet."""
    runner.invoke(cli.app, ["init", "--name", tmp_path.name, str(tmp_path)])
    return project.Project(tmp_path)


@pytest.fixture
def f7ttest_project_offline(
    empty_project_offline: project.Project, runner: typer.testing.CliRunner
) -> project.Project:
    """Provide a project with f7ttest ready to authenticate."""
    proj = empty_project_offline
    runner.invoke(cli.app, ["add-site", "f7ttest", str(proj.path), "--offline"])
    return proj
