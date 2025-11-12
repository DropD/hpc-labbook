"""Common fixtures."""

from __future__ import annotations

import pathlib
import typing
from typing import Callable

import aiida
import aiida.orm
import pytest

import hpclb

pytest_plugins = ["aiida.tools.pytest_fixtures"]


def pytest_collection_modifyitems(
    session: pytest.Session,  # noqa: ARG001  # signature determined by pytest
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Skip tests that require the f7t cluster container fleet by default."""
    if not config.getoption("-m"):
        skip_me = pytest.mark.skip(reason="use `-m cluster` to run this test")
        for item in items:
            if "cluster" in item.keywords:
                item.add_marker(skip_me)


@pytest.fixture
def example_code(
    aiida_computer_local: typing.Callable[[], aiida.orm.Computer],
    aiida_code_installed: typing.Callable[..., aiida.orm.InstalledCode],
) -> aiida.orm.InstalledCode:
    """Create an mock code."""
    code = aiida_code_installed(
        default_calc_job_plugin="hpclb.generic", computer=aiida_computer_local()
    )
    code.store()
    return code


@pytest.fixture
def example_targetdir(tmp_path: pathlib.Path) -> aiida.orm.JsonableData:
    """Construct an example targetdir data node (wrapped in JsonableData)."""
    staging = tmp_path
    config_file = staging / "foo.config"
    config_file.write_text("a = 1\nb = 2\n")
    return aiida.orm.JsonableData(
        hpclb.aiida.data.TargetDir(
            name="root",
            subdirs=[
                hpclb.aiida.data.TargetDir(
                    name="config",
                    upload=[
                        hpclb.aiida.data.UploadFile(
                            source=config_file,
                            input_label="config_file",
                            tgt_name="input.config",
                        )
                    ],
                ),
                hpclb.aiida.data.TargetDir(name="out-files"),
            ],
            remote=[
                hpclb.aiida.data.RemotePath(
                    src_path=pathlib.Path("/some/absolute/path/somefile.xml"),
                    tgt_name="data.xml",
                    copy=True,
                ),
                hpclb.aiida.data.RemotePath(
                    src_path=pathlib.Path("/some/path/to/dir"),
                    tgt_name="datadir",
                    copy=False,
                ),
            ],
        )
    )


@pytest.fixture
def cluster(aiida_computer: Callable[..., aiida.orm.Computer]) -> aiida.orm.Computer:
    """Set up the f7t test cluster computer."""
    comp = aiida_computer(
        label="testcluster",
        hostname="localhost",
        transport_type="firecrest",
        scheduler_type="firecrest",
        # Yes, the following are hardcoded credentials. No, they are not dangerous.
        # They are for the test cluster containers, not for a real system.
        configuration_kwargs={
            "url": "http://localhost:8000",
            "token_uri": "http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token",
            "client_id": "firecrest-test-client",
            "client_secret": "wZVHVIEd9dkJDh9hMKc6DTvkqXxnDttk",
            "compute_resource": "cluster-slurm-api",
            "billing_account": "myproject",
            "temp_directory": "/home/fireuser/f7temp",
            "max_io_allowed": 8,
            "checksum_check": False,
        },
    )
    dummy = pathlib.Path(__file__).parent / "data" / "dummy.sh"
    transport = comp.get_transport()
    transport.put(localpath=dummy, remotepath=pathlib.Path("/home/fireuser/dummy.sh"))
    transport.chmod(path=pathlib.Path("/home/fireuser/dummy.sh"), mode=755)
    return comp


@pytest.fixture
def cluster_dummy(
    cluster: aiida.orm.Computer,
    aiida_code_installed: Callable[..., aiida.orm.InstalledCode],
) -> aiida.orm.InstalledCode:
    """Set up the dummy code on the test cluster."""
    return aiida_code_installed(
        label="dummy",
        default_calc_job_plugin="hpclb.generic",
        computer=cluster,
        filepath_executable="/home/fireuser/dummy.sh",
        with_mpi=True,
    )
