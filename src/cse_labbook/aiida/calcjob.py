"""Generic CalcJob for use with the hpc lab book."""

from __future__ import annotations

import typing

from aiida import engine, orm
from aiida.common import datastructures
from aiida.parsers import parser

from cse_labbook.aiida import data

if typing.TYPE_CHECKING:
    from aiida.common import folders
    from aiida.engine.processes.calcjobs import calcjob


class GenericCalculation(engine.CalcJob):
    """Generic CalcJob for the hpc lab book."""

    @classmethod
    def define(cls, spec: calcjob.CalcJobProcessSpec) -> None:  # type: ignore[override] # forced by aiida-core
        """Define the input and output ports as well as some defaults."""
        super().define(spec)
        spec.input("workdir", valid_type=orm.JsonableData)
        spec.input_namespace("uploaded", dynamic=True, valid_type=orm.SinglefileData)
        options = spec.inputs["metadata"]["options"]  # type: ignore[index] # guaranteed correct by aiida-core
        options["parser_name"].default = "hpclb.generic"  # type: ignore[index] # guaranteed correct by aiida-core

    def prepare_for_submission(self, folder: folders.Folder) -> datastructures.CalcInfo:
        """Set up the template for the work dir on the compute resource."""
        codeinfo = datastructures.CodeInfo()
        codeinfo.code_uuid = self.inputs.code.uuid
        calcinfo = datastructures.CalcInfo()
        calcinfo.codes_info = [codeinfo]

        data.create_dirs(target_dir=self.inputs.workdir.obj, folder=folder)
        upload, copy, link = data.create_triplets(
            target_dir=self.inputs.workdir.obj, calcjob=self
        )

        calcinfo.local_copy_list = [(i.uuid, i.src_name, i.tgt_path) for i in upload]
        calcinfo.remote_copy_list = [(i.uuid, i.src_path, i.tgt_path) for i in copy]
        calcinfo.remote_symlink_list = [(i.uuid, i.src_path, i.tgt_path) for i in link]

        return calcinfo


class GenericParser(parser.Parser):
    """Parser for generic hpclb calculations."""

    def parse(self, **kwargs: typing.Any) -> engine.ExitCode:  # noqa: ARG002,ANN401  # kwargs must be there for superclass compatibility
        """Parse a retrieved calculation."""
        return engine.ExitCode(0)
