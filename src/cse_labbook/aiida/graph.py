"""A WorkChain that can advance-submit SLURM jobs according to a dependency graph."""

from __future__ import annotations

import typing
from typing import Any

import networkx as nx
from aiida import engine, orm
from aiida.engine import processes
from aiida.engine.processes import exit_code
from aiida.engine.processes.workchains import workchain
from typing_extensions import Self

from cse_labbook.aiida import data, future


class GraphWorkchain(workchain.WorkChain):
    """Run a static graph of interdependent, potentially long-running jobs."""

    @classmethod
    def define(cls: type[Self], spec: processes.ProcessSpec) -> None:  # type: ignore[override] # disagreement between aiida-core and plumpy
        """Define the inputs, outputs and structure."""
        super().define(spec)
        spec = typing.cast(workchain.WorkChainSpec, spec)
        spec.input("graph", valid_type=orm.JsonableData)
        spec.input_namespace("node", dynamic=True)
        spec.outline(
            cls.start,  # type: ignore[arg-type] # aiida-core typing predates Self type
            engine.while_(cls.not_reached_end)(  # type: ignore[arg-type] # aiida-core typing predates Self type
                cls.submit_front  # type: ignore[arg-type] # aiida-core typing predates Self type
            ),
            cls.finalize,  # type: ignore[arg-type] # aiida-core typing predates Self type
        )
        spec.exit_code(500, "NOT_A_DAG", "graph parameter is not a DAG")
        spec.exit_code(501, "JOB_FAILED", "one of the graph jobs failed")

    def format_report(self, msg: str, *args: str) -> tuple[str, ...]:
        """Format reports with current iteration (generation)."""
        return ("[%s]: " + msg, self.ctx.iteration, *args)

    def start(self) -> None | exit_code.ExitCode:
        """Set up and plan execution order."""
        self.report("starting graph execution")
        graph = self.inputs.graph.obj
        dag = nx.DiGraph(graph.edges)
        if not nx.is_directed_acyclic_graph(dag):
            return self.exit_codes.NOT_A_DAG
        self.ctx.iteration = 0
        self.ctx.front = list(nx.topological_generations(dag))
        self.report(*self.format_report("submission plan: %s", str(self.ctx.front)))
        return None

    def not_reached_end(self) -> bool:
        """
        Decide whether more jobs can be submitted.

        Also, ensure the next wave of submissions has all their dependencies at least
        queued.
        """
        result = self.ctx.iteration < len(self.ctx.front)
        if not result:
            self.report(*self.format_report("waiting for all outstanding calculations"))
            self.ctx.all = []
            for node_async in self.ctx.node_async.values():
                self.to_context(
                    all=engine.append_(
                        typing.cast(
                            orm.ProcessNode,
                            orm.load_node(
                                typing.cast(
                                    orm.ProcessNode, node_async.outputs.future.obj.uuid
                                )
                            ),
                        )
                    )
                )
        return result

    def submit_front(self) -> None | exit_code.ExitCode:
        """Submit the next generation of jobs."""
        graph = self.inputs.graph.obj
        dag = nx.DiGraph(graph.edges)
        self.report(
            *self.format_report(
                "submitting %s", str(self.ctx.front[self.ctx.iteration])
            )
        )
        for node_idx in self.ctx.front[self.ctx.iteration]:
            dependencies = {
                d: self.ctx.node_async[str(d)] for d in dag.predecessors(node_idx)
            }
            for dependency in dependencies.values():
                if not dependency.is_finished_ok:
                    return self.exit_codes.JOB_FAILED
            node = graph.nodes[node_idx]
            builder: Any = future.AsyncWorkchain.get_builder()
            builder.calc.code = orm.load_code(self.inputs.node[f"n{node_idx}__code"])
            builder.calc.workdir = orm.JsonableData(node.workdir)
            builder.calc.uploaded = data.build_uploads(node.workdir)
            if dependencies:
                builder.calc.futures = {
                    f"dep_{i}": d.outputs.future for i, d in dependencies.items()
                }
            builder.calc.metadata.options.withmpi = True
            builder.calc.metadata.options.resources = {
                "num_machines": 1,
                "num_mpiprocs_per_machine": 1,
            }
            builder.calc.metadata.options.max_memory_kb = 5000
            if dependencies:
                dep_string = ":".join(
                    d.outputs.future.obj.jobid for d in dependencies.values()
                )
                builder.calc.metadata.options.custom_scheduler_commands = (
                    f"#SBATCH -d afterok:{dep_string}"
                )

            self.to_context(**{f"node_async.{node_idx}": self.submit(builder)})  # type: ignore[arg-type] # aiida_core typing is wrong
        self.ctx.iteration += 1
        return None

    def finalize(self: Self) -> None:
        """Wait for all calculations to finish and wrap up."""
        self.report("All done.")
