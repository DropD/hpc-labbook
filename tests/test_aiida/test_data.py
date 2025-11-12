"""Test the hpclb aiida data structures."""

import pathlib
import typing

import aiida.common.folders
import aiida.orm

import hpclb


def test_create_triplets(
    example_code: aiida.orm.InstalledCode, example_targetdir: aiida.orm.JsonableData
) -> None:
    """Test upload / copy / link triplet creation from a TargetDir."""
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
    upload, copy, link = hpclb.aiida.data.create_triplets(
        typing.cast(hpclb.aiida.data.TargetDir, example_targetdir.obj), calc
    )

    assert upload == [
        hpclb.aiida.data.UploadTriplet(
            uuid=config_file.uuid, src_name="foo.config", tgt_path="config/input.config"
        )
    ]
    assert copy == [
        hpclb.aiida.data.RemoteTriplet(
            uuid=example_code.computer.uuid,
            src_path="/some/absolute/path/somefile.xml",
            tgt_path="data.xml",
        )
    ]
    assert link == [
        hpclb.aiida.data.RemoteTriplet(
            uuid=example_code.computer.uuid,
            src_path="/some/path/to/dir",
            tgt_path="datadir",
        )
    ]


def test_create_dirs(
    example_targetdir: aiida.orm.JsonableData, tmp_path: pathlib.Path
) -> None:
    """Make sure the correct directory structure is created."""
    folder = aiida.common.folders.Folder(abspath=tmp_path.absolute())
    hpclb.aiida.data.create_dirs(
        typing.cast(hpclb.aiida.data.TargetDir, example_targetdir.obj), folder
    )

    assert (tmp_path / "config").is_dir()
    assert (tmp_path / "out-files").is_dir()
