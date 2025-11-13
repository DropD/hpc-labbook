"""Test the init subcommand."""

from __future__ import annotations

import dataclasses
import re
import typing

import pytest

from hpclb import cli, project

if typing.TYPE_CHECKING:
    from typer.testing import CliRunner


@dataclasses.dataclass
class SiteCase:
    """Combine the data needed to run a test for a specific case."""

    name: str
    options: list[str] = dataclasses.field(default_factory=list)


SITE_CASES = [SiteCase("cscs", ["--username", "testuser"]), SiteCase("f7ttest")]


def site_case_id(case: SiteCase) -> str:
    """Extract a meaninful id from a site case."""
    return case.name


@pytest.mark.parametrize("site", SITE_CASES, ids=site_case_id)
def test_add_site(
    site: SiteCase, empty_project_offline: project.Project, runner: CliRunner
) -> None:
    """Make sure adding CSCS site specs to an empty project succeeds."""
    proj = empty_project_offline
    res = runner.invoke(
        cli.app, ["add-site", site.name, str(proj.path), *site.options, "--offline"]
    )
    assert res.exit_code == 0, res.output
    assert proj.site_dir(site.name).exists()  # make sure created
    assert list(proj.site_dir(site.name).iterdir())  # make sure not empty


@pytest.mark.parametrize("site", SITE_CASES, ids=site_case_id)
def test_readd_site(
    site: SiteCase, empty_project_offline: project.Project, runner: CliRunner
) -> None:
    """Make sure site dir is not added twice."""
    proj = empty_project_offline
    (proj.path / site.name).mkdir()
    res = runner.invoke(
        cli.app, ["add-site", site.name, str(proj.path), *site.options, "--offline"]
    )
    assert res.exit_code == 2, res.output
    assert re.findall(
        rf"site '{site.name}' already present", res.output, re.MULTILINE
    ), res.output
