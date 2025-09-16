"""Common fixtures."""

from __future__ import annotations

import pathlib
import typing

import aiida
import aiida.orm
import pytest

import cse_labbook as hplb

pytest_plugins = ["aiida.tools.pytest_fixtures"]


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
        hplb.aiida.data.TargetDir(
            name="root",
            subdirs=[
                hplb.aiida.data.TargetDir(
                    name="config",
                    upload=[
                        hplb.aiida.data.UploadFile(
                            source=config_file,
                            input_label="config_file",
                            tgt_name="input.config",
                        )
                    ],
                ),
                hplb.aiida.data.TargetDir(name="out-files"),
            ],
            remote=[
                hplb.aiida.data.RemotePath(
                    src_path=pathlib.Path("/some/absolute/path/somefile.xml"),
                    tgt_name="data.xml",
                    copy=True,
                ),
                hplb.aiida.data.RemotePath(
                    src_path=pathlib.Path("/some/path/to/dir"),
                    tgt_name="datadir",
                    copy=False,
                ),
            ],
        )
    )
