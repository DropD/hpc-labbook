"""Generic CalcJob for use with the hpc lab book."""

from __future__ import annotations

import pathlib
import typing

from aiida import engine, orm
from aiida.common import datastructures
from aiida.parsers import parser
from typing_extensions import Self

from hpclb.aiida import data

if typing.TYPE_CHECKING:
    from aiida.common import folders
    from aiida.engine.processes.calcjobs import calcjob


__all__ = ["GenericCalculation", "GenericParser"]


class GenericCalculation(engine.CalcJob):
    """Generic CalcJob for the hpc lab book."""

    @classmethod
    def define(cls: type[Self], spec: calcjob.CalcJobProcessSpec) -> None:  # type: ignore[override] # forced by aiida-core
        """Define the input and output ports as well as some defaults."""
        super().define(spec)
        spec.input("workdir", valid_type=orm.JsonableData)
        spec.input("cmdline_params", valid_type=orm.List, required=False)
        spec.input_namespace("uploaded", dynamic=True, valid_type=orm.SinglefileData)
        spec.input_namespace("futures", dynamic=True, valid_type=orm.JsonableData)
        spec.input_namespace(
            "download_required", dynamic=True, valid_type=orm.Str, required=False
        )
        spec.input_namespace(
            "download_optional", dynamic=True, valid_type=orm.Str, required=False
        )
        spec.output_namespace(
            "missing", dynamic=True, valid_type=orm.List, required=False
        )
        options = spec.inputs["metadata"]["options"]  # type: ignore[index] # guaranteed correct by aiida-core
        options["parser_name"].default = "hpclb.generic"  # type: ignore[index] # guaranteed correct by aiida-core
        options["resources"].default = {  # type: ignore[index] # guaranteed correct by aiida-core
            "num_machines": 1,
            "num_mpiprocs_per_machine": 1,
        }
        spec.exit_code(404, "MISSING OUTPUT FILE", "required output file not found")

    def prepare_for_submission(
        self: Self, folder: folders.Folder
    ) -> datastructures.CalcInfo:
        """Set up the template for the work dir on the compute resource."""
        codeinfo = datastructures.CodeInfo()
        codeinfo.code_uuid = self.inputs.code.uuid
        if "cmdline_params" in self.inputs:
            codeinfo.cmdline_params = self.inputs.cmdline_params.value
        calcinfo = datastructures.CalcInfo()
        calcinfo.codes_info = [codeinfo]

        data.create_dirs(target_dir=self.inputs.workdir.obj, folder=folder)
        upload, copy, link = data.create_triplets(
            target_dir=self.inputs.workdir.obj, calcjob=self
        )

        calcinfo.local_copy_list = [(i.uuid, i.src_name, i.tgt_path) for i in upload]
        calcinfo.remote_copy_list = [(i.uuid, i.src_path, i.tgt_path) for i in copy]
        calcinfo.remote_symlink_list = [(i.uuid, i.src_path, i.tgt_path) for i in link]
        calcinfo.retrieve_list = list(
            self.inputs.get("download_required", {}).values()
        ) + list(self.inputs.get("download_optional", {}).values())

        return calcinfo


class GenericParser(parser.Parser):
    """Parser for generic hpclb calculations."""

    def is_file_retrieved(self: Self, relpath: pathlib.Path) -> bool:
        """Check for a file at 'relpath' in the retrieved repository folder."""
        try:
            self.retrieved.get_object(relpath)
        except FileNotFoundError:
            return False
        return True

    def parse(self: Self, **kwargs: typing.Any) -> engine.ExitCode:  # noqa: ARG002,ANN401  # kwargs must be there for superclass compatibility
        """Parse a retrieved calculation."""
        strict_check = {
            "stdout": self.node.get_option("scheduler_stdout"),
            "stderr": self.node.get_option("scheduler_stderr"),
        } | getattr(self.node.inputs, "download_required", {})
        missing_strict = []
        for key, pathstr in strict_check.items():
            path_in_repo = pathlib.Path(pathlib.Path(pathstr).name)
            if not self.is_file_retrieved(path_in_repo):
                self.logger.error(f"missing retrieved file: {path_in_repo}")
                missing_strict.append(key)
        self.out("missing.download_required", orm.List(missing_strict))
        if missing_strict:
            return self.node.exit_codes.MISSING_OUTPUT_FILE

        return engine.ExitCode(0)
