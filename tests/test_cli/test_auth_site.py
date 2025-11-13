"""
Test the f7ttest authentication.

CSCS authentication can not be tested automatically for obvious reasons.
"""

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


SITE_CASES = [SiteCase("cscs"), SiteCase("f7ttest", ["--offline"])]


@pytest.mark.cluster
def test_authenticate_f7ttest(
    f7ttest_project_offline: project.Project, runner: CliRunner
) -> None:
    """Check authenticating works if used as intended."""
    proj = f7ttest_project_offline
    res = runner.invoke(cli.app, ["auth-site", "f7ttest", str(proj.path), "--offline"])
    assert res.exit_code == 0, res.output


@pytest.mark.parametrize("site", SITE_CASES)
def test_authenticate_f7ttest_on_empty(
    site: SiteCase, empty_project_offline: project.Project, runner: CliRunner
) -> None:
    """Check failure mode for calling auth before add."""
    proj = empty_project_offline
    res = runner.invoke(
        cli.app, ["auth-site", site.name, str(proj.path), *site.options]
    )
    assert res.exit_code == 2, res.output
    assert re.findall(rf"site '{site.name}' not added yet", res.output, re.MULTILINE), (
        res.output
    )
