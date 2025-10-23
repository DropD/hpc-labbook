"""Generic Workchain that allows dependents to be queued before it is finished."""

from __future__ import annotations

import asyncio
import pathlib
import typing
from typing import Any

import aiida_pythonjob  # type: ignore[import-untyped]
from aiida import engine, orm
from aiida.engine import processes
from aiida.engine.processes import process
from aiida.engine.processes.workchains import workchain
from typing_extensions import Self

from cse_labbook.aiida import calcjob, data


class NotSubmittedError(Exception):
    """Error for when a calcjob fails to submit asynchronously."""

    default_msg: typing.ClassVar[str] = "Process not submitted and / or excepted."

    def __str__(self: Self) -> str:
        """Construct the error message."""
        return self.default_msg


class SubmittingTimedOutError(NotSubmittedError):
    """Waiting for job being submitted timed out."""

    default_msg = "Submitting timed out"
    time_s: int | None

    def __init__(
        self: Self, message: str | None = None, *, time_s: int | None = None
    ) -> None:
        """Initialize with how long it took to time out."""
        super().__init__(self, message)
        self.message = message
        self.time_s = time_s

    def __str__(self: Self) -> str:
        """Construct the error message."""
        match (self.time_s, self.message):
            case (int(_), None):
                return f"{self.default_msg} in {self.time_s} s"
            case (int(_), str(msg)) if msg != "":
                return msg.format(time_s=self.time_s)
            case (None, str(msg)) if msg != "":
                return msg
            case _:
                return super().__str__()


class KilledBeforeSubmittedError(NotSubmittedError):
    """Job was killed while waiting for submission."""

    default_msg = "Process got killed while waiting to be submitted."


@engine.calcfunction
def create_future(uuid: orm.Str, jobid: orm.Str, workdir: orm.Str) -> orm.JsonableData:
    """Create a future object in a provenance graph friendly way."""
    return orm.JsonableData(
        data.Future(
            uuid=uuid.value, jobid=jobid.value, workdir=pathlib.Path(workdir.value)
        )
    )


@aiida_pythonjob.pyfunction()
async def wait_for_submitted(uuid: str, poll_interval: int, timeout: int) -> None:
    """Check for the underlying CalcJob to be submitted (queued) successfully."""
    submitted = orm.load_node(uuid=uuid)
    time_waited = 0
    while submitted.get_job_id() is None or submitted.get_remote_workdir() is None:
        if time_waited >= timeout:
            msg = (
                f"Timed out in {{time_s}}, with process_state {submitted.process_state}"
            )
            raise SubmittingTimedOutError(
                message=msg,
                time_s=time_waited,
            )
        await asyncio.sleep(poll_interval)
        time_waited += poll_interval
        print("checking if submitted")
        print("- job id: {}".format(submitted.get_job_id()))
        print("- workdir: {}".format(submitted.get_remote_workdir()))
        match submitted.process_state:
            case process.ProcessState.EXCEPTED:
                print(submitted.exception)
                raise NotSubmittedError
            case process.ProcessState.KILLED:
                raise KilledBeforeSubmittedError
            case _:
                pass
        submitted = orm.load_node(uuid=uuid)


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
        self.ctx.wait_time = 1  # s
        self.ctx.timeout = 30  # s
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
        self.ctx.wait_for_submit = self.to_context(
            monitor=self.submit(
                aiida_pythonjob.PyFunction,
                **aiida_pythonjob.prepare_pyfunction_inputs(
                    wait_for_submitted,
                    function_inputs={
                        "uuid": self.ctx.submitted,
                        "poll_interval": self.ctx.wait_time,
                        "timeout": self.ctx.timeout,
                    },
                ),
            )
        )

    def emit_future(self: Self) -> None:
        """Return a provenance graph level future for the underlying async CalcJob."""
        if not self.ctx.monitor.is_finished_ok:
            self.report(str(self.ctx.monitor.process_state))
            self.report(str(self.ctx.monitor.exit_code))
            self.report(self.ctx.monitor.exit_message)
            self.report(str(self.ctx.monitor.exception))
            raise NotSubmittedError
        submitted = orm.load_node(uuid=self.ctx.submitted)
        self.out(
            "future",
            create_future(
                uuid=self.ctx.submitted,
                jobid=submitted.get_job_id(),
                workdir=submitted.get_remote_workdir(),
            ),
        )
