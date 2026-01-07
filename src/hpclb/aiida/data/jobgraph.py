"""Data types to describe workflows as job graphs."""

import dataclasses

from hpclb.aiida.data import jobspec, jsonable


@dataclasses.dataclass
class Graph(jsonable.JsonableMixin):
    """Job dependency graph."""

    nodes: list[jobspec.Generic]
    edges: list[tuple[int, int]]
