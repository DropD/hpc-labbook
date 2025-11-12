"""Test the generic calcjob without running it."""

from __future__ import annotations

import pathlib
import typing

import aiida
import aiida.common.folders
import aiida.orm

import hpclb


def test_example_generic_presubmit(
    example_code: aiida.orm.Code,
    example_targetdir: aiida.orm.JsonableData,
    tmp_path: pathlib.Path,
) -> None:
    """
    Test presubmitting a properly set up GenericCalculation.

    When presubmitting a GenericCalculation, the staging dir should mirror the input
    TargetDir hierarchy. So should the copy and symlink lists.
    """
    staging = tmp_path
    builder: typing.Any = hpclb.aiida.calcjob.GenericCalculation.get_builder()
    builder.code = example_code
    builder.workdir = example_targetdir
    config_file = aiida.orm.SinglefileData(
        builder.workdir.obj.subdirs[0].upload[0].source
    )
    builder.uploaded.config_file = config_file
    builder.metadata.options.resources = {
        "num_machines": 1,
        "num_mpiprocs_per_machine": 1,
        "num_cores_per_mpiproc": 1,
    }
    calc = hpclb.aiida.calcjob.GenericCalculation(dict(builder))
    sandbox_folder = aiida.common.folders.SandboxFolder(staging.absolute())
    sandbox = pathlib.Path(sandbox_folder.abspath)
    calcinfo = calc.presubmit(sandbox_folder)

    assert sandbox.is_dir()
    assert (sandbox / "config").is_dir()
    assert (sandbox / "out-files").is_dir()

    assert example_code.computer

    assert calcinfo.local_copy_list == [
        (config_file.uuid, "foo.config", "config/input.config")
    ]
    assert calcinfo.remote_copy_list == [
        (example_code.computer.uuid, "/some/absolute/path/somefile.xml", "data.xml")
    ]
    assert calcinfo.remote_symlink_list == [
        (example_code.computer.uuid, "/some/path/to/dir", "datadir")
    ]
