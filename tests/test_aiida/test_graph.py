"""Test the graph workchain."""

from __future__ import annotations

import pathlib
import typing
from typing import Any, Callable, Iterator

import aiida
import aiida.engine
import aiida.orm
import pytest
from typer.testing import CliRunner

import cse_labbook as hpclb
from cse_labbook.aiida import data, graph

if typing.TYPE_CHECKING:
    import aiida.manage
    from aiida.manage.configuration import config as aiida_cfg


@pytest.fixture(scope="session", autouse=True)
def project(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[hpclb.project.Project]:
    """Create a temporary project to test on the local F7T cluster."""
    tmp_path = tmp_path_factory.mktemp("graphtest")
    # tmp_path = pathlib.Path("/Users/ricoh/Code/graphtest")
    runner = CliRunner()
    print(
        runner.invoke(
            hpclb.cli.app, ["init", "--name", "graphtest", str(tmp_path)]
        ).output
    )
    result = hpclb.project.Project(tmp_path)
    print(result.verdi(["profile", "show", "presto"]).stdout.decode("utf-8"))
    print(
        result.verdi(
            [
                "profile",
                "configure-rabbitmq",
                "--force",
                "presto",
                "--broker-host",
                "aiida-rabbit.orb.local",
            ],
        ).stdout.decode("utf-8")
    )
    print(result.verdi(["profile", "show", "presto"]).stdout.decode("utf-8"))
    print(runner.invoke(hpclb.cli.app, ["add-site", "f7ttest", str(tmp_path)]).output)
    print(runner.invoke(hpclb.cli.app, ["auth-site", "f7ttest", str(tmp_path)]).output)
    print(
        result.verdi(
            [
                "code",
                "create",
                "core.code.installed",
                "-n",
                "--label",
                "test-dummy",
                "--default-calc-job-plugin",
                "hpclb.generic",
                "--computer",
                "f7ttest",
                "--no-with-mpi",
                "--filepath-executable",
                "/home/fireuser/dummy.sh",
            ],
            encoding="utf-8",
        ).stdout
    )
    print(result.verdi(["daemon", "start", "4"], encoding="utf-8").stdout)
    yield result
    print(result.verdi(["daemon", "stop"], encoding="utf-8").stdout)


@pytest.fixture
def minimum_graph() -> data.Graph:
    """Set up the smallest possible graph worth testing."""
    step1 = data.Job(
        workdir=data.TargetDir(
            name="root",
            upload=[
                data.UploadFile(
                    source=pathlib.Path(__file__).parent.parent
                    / "data"
                    / "dummy_input.txt",
                    input_label="input_txt",
                    tgt_name="input.txt",
                )
            ],
        )
    )
    step2 = data.Job(
        workdir=data.TargetDir(
            name="root",
            from_future=[
                data.FuturePath(
                    src_relpath=pathlib.Path("out.txt"),
                    input_label="dep_0",
                    tgt_name="input.txt",
                )
            ],
        )
    )
    return data.Graph(nodes=[step1, step2], edges=[(0, 1)])


@pytest.fixture(scope="session", autouse=True)
def aiida_config(
    aiida_config_factory: Callable[..., typing.ContextManager],
    project: hpclb.project.Project,
) -> Iterator[aiida_cfg.Config]:
    """Load the config from test-hpclb for the session."""
    with aiida_config_factory(project.path) as config:
        yield config


@pytest.fixture(scope="session", autouse=True)
def aiida_profile() -> Iterator[aiida.manage.Profile]:
    """Load the profile from test-hpclb for the session."""
    curent_profile = aiida.get_profile()
    yield aiida.load_profile("presto", allow_switch=True)
    if curent_profile:
        aiida.load_profile(curent_profile.name, allow_switch=True)


@pytest.mark.cluster
def test_min_graph(
    minimum_graph: data.Graph,
) -> None:
    """
    Test the smallest possible graph.

    A --> B

    Where:
    - A, B are jobs that read an input file (`input.txt`), wait some time,
        then write a modified version of the input file to `out.txt`
    - B depends on A, linking A's `out.txt` to it's own workdir as `input.txt`
    """
    builder: Any = graph.GraphWorkchain.get_builder()
    builder.graph = aiida.orm.JsonableData(minimum_graph)
    builder.node.n0__code = "test-dummy"
    builder.node.n1__code = "test-dummy"
    result = aiida.engine.submit(builder, wait=True)
    start_a, start_b = result.called
    calcjob_a, _, future_gen_a = start_a.called
    calcjob_b, _, future_gen_b = start_b.called
    assert start_a.ctime < start_b.ctime  # A launched before B
    assert calcjob_a.mtime < calcjob_b.mtime  # A done submitting before B
    assert calcjob_a.mtime < calcjob_b.mtime  # A completed before B
    assert calcjob_b.ctime < calcjob_a.mtime  # B started submitting before A completed
    assert future_gen_a.mtime < calcjob_a.mtime  # A future returned before A completed
    assert future_gen_b.mtime < calcjob_b.mtime  # B future returned before B completed
