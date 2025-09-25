"""Generic Workchain that allows dependents to be queued before it is finished."""

from __future__ import annotations

import pathlib
import time
import typing
from typing import Any

from aiida import engine, orm
from aiida.engine import processes
from aiida.engine.processes import process
from aiida.engine.processes.workchains import workchain
from typing_extensions import Self

from cse_labbook.aiida import calcjob, data


@engine.calcfunction
def create_future(uuid: orm.Str, jobid: orm.Str, workdir: orm.Str) -> orm.JsonableData:
    """Create a future object in a provenance graph friendly way."""
    return orm.JsonableData(
        data.Future(
            uuid=uuid.value, jobid=jobid.value, workdir=pathlib.Path(workdir.value)
        )
    )


class AsyncWorkchain(workchain.WorkChain):
    """Submit a CalcJob asynchronously and return a future object early."""

    @classmethod
    def define(cls: type[Self], spec: processes.ProcessSpec) -> None:  # type: ignore[override] # disagreement between aiida-core and plumpy
        """Declare inputs, outputs and outline of the workflow."""
        super().define(spec)
        spec = typing.cast(workchain.WorkChainSpec, spec)
        spec.expose_inputs(calcjob.GenericCalculation, namespace="calc")
        spec.output("future", valid_type=orm.JsonableData)
        spec.outline(
            cls.start,  # type: ignore[arg-type] # aiida-core typing predates Self type
            engine.while_(cls.submitting)(cls.wait),  # type: ignore[arg-type] # aiida-core typing predates Self type
            cls.emit_future,  # type: ignore[arg-type] # aiida-core typing predates Self type
        )
        spec.exit_code(400, "CALC_FAILED_OR_KILLED", "the calculation didn't make it.")
        spec.exit_code(
            401,
            "CALC_FAILED_TO_SUBMIT",
            "the calculation didn't get submitted in the allotted time.",
        )

    def start(self: Self) -> None:
        """Start up and submit the underlying CalcJob."""
        self.ctx.wait_time = 5  # s
        self.ctx.timeout = 300  # s
        self.ctx.waited = 0  # s
        print(str(self.exposed_inputs(calcjob.GenericCalculation, namespace="calc")))
        builder: Any = calcjob.GenericCalculation.get_builder()
        builder.metadata = self.inputs.calc.metadata
        if "futures" in self.inputs.calc:
            builder.metadata.options.prepend_text = "\n".join(
                data.iter_future_links(
                    self.inputs.calc.workdir.obj, self.inputs.calc.futures
                )
            )
        self.ctx.submitted = self.submit(
            builder,  # type: ignore[arg-type] # aiida-core typing is incorrect
            **self.exposed_inputs(calcjob.GenericCalculation, namespace="calc"),
        ).uuid

    def submitting(self: Self) -> bool | process.ExitCode:
        """Check for the underlying CalcJob to be submitted (queued) successfully."""
        submitted = orm.load_node(uuid=self.ctx.submitted)
        if submitted.process_state in [
            process.ProcessState.EXCEPTED,
            process.ProcessState.KILLED,
        ]:
            return self.exit_codes.CALC_FAILED_OR_KILLED
        self.report("checking if submitted")
        self.report("- job id: %s", submitted.get_job_id())
        self.report("- workdir: %s", submitted.get_remote_workdir())
        return submitted.get_job_id() is None or submitted.get_remote_workdir() is None

    def wait(self: Self) -> None | process.ExitCode:
        """
        Wait some time before checking whether the CalcJob is submitted.

        Time out after a while.
        """
        if self.ctx.waited >= self.ctx.timeout:
            return self.exit_codes.CALC_FAILED_TO_SUBMIT
        time.sleep(self.ctx.wait_time)
        self.ctx.waited += self.ctx.wait_time
        return None

    def emit_future(self: Self) -> None:
        """Return a provenance graph level future for the underlying async CalcJob."""
        submitted = orm.load_node(uuid=self.ctx.submitted)
        self.out(
            "future",
            create_future(
                uuid=self.ctx.submitted,
                jobid=submitted.get_job_id(),
                workdir=submitted.get_remote_workdir(),
            ),
        )
