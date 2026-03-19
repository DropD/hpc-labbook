"""Display a set of processes."""

from __future__ import annotations

import dataclasses
import enum
import typing

import pendulum
import rich.text
from aiida.orm import ProcessNode
from aiida.schedulers import JobState
from plumpy import ProcessState
from textual import widgets
from typing_extensions import Self


def state_to_symbol(
    proc_state: ProcessState | None, sched_state: JobState | None
) -> str:
    """Represent an AiiDA process state in reader friendly unicode."""
    match (proc_state, sched_state):
        case (ProcessState.CREATED, _):
            return "🏗"
        case (ProcessState.EXCEPTED, _):
            return "💥"
        case (ProcessState.RUNNING, _) | (ProcessState.WAITING, JobState.RUNNING):
            return "🚀"
        case (ProcessState.WAITING, _):
            return "⏳"
        case (ProcessState.KILLED, _):
            return "🪓"
        case (ProcessState.FINISHED, _):
            return "✅"
        case _:
            return "?"


def bool_to_symbol(value: bool | None) -> str:
    """Represent an optional boolean in reader friendly unicode."""
    match value:
        case True:
            return "✅"
        case False:
            return "❌"
        case _:
            return "?"


def get_usable_symbol(node: ProcessNode) -> str:
    """Get symbol to represent the 'usable' extra."""
    return bool_to_symbol(node.base.extras.get("usable", None))


class ProcessSorting(enum.Enum):
    """The supported sortings for process tables."""

    PK = enum.auto()
    LABEL = enum.auto()
    TYPE = enum.auto()
    USABILITY = enum.auto()


class SortingDirection(enum.Enum):
    """Sorting direction (ascending or descending)."""

    ASC = enum.auto()
    DESC = enum.auto()


def process_by_pk_key(pk: int, /) -> int:
    """Turn pk column values into a sorting key."""
    return -pk


def process_by_label_key(columns: tuple[str, int]) -> tuple[str, int]:
    """Turn label, pk column values into a sorting key."""
    label, pk = columns
    return label, -pk


def process_by_usability_key(columns: tuple[str, int]) -> tuple[int, int]:
    """Turn usable, pk column values into a sorting key."""
    usable, pk = columns
    usability_level = {"✅": 0, "❌": 1, "?": 2}
    return usability_level[usable], -pk


def process_by_type_key(columns: tuple[str, int]) -> tuple[str, int]:
    """Turn type, pk column values into a sorting key."""
    type, pk = columns
    return type if type else "Ω", -pk


@dataclasses.dataclass
class ProcessTableSorter:
    """Sort a process table, remembering which sorting was used last."""

    table: ProcessTable
    sorting: ProcessSorting = ProcessSorting.PK
    direction: SortingDirection = SortingDirection.ASC

    key: typing.ClassVar[
        dict[ProcessSorting, typing.Callable[[typing.Any], typing.Any]]
    ] = {
        ProcessSorting.PK: process_by_pk_key,
        ProcessSorting.LABEL: process_by_label_key,
        ProcessSorting.TYPE: process_by_type_key,
        ProcessSorting.USABILITY: process_by_usability_key,
    }

    columns: typing.ClassVar = {
        ProcessSorting.PK: ("pk",),
        ProcessSorting.LABEL: ("label", "pk"),
        ProcessSorting.TYPE: ("type", "pk"),
        ProcessSorting.USABILITY: ("usable", "pk"),
    }

    def sort(
        self: Self,
        sorting: ProcessSorting | None = None,
        direction: SortingDirection | None = None,
    ) -> None:
        """Sort the table in the specified way."""
        if sorting:
            self.sorting = sorting
        if direction:
            self.direction = direction
        self.table.sort(
            *self.columns[self.sorting],
            key=self.key[self.sorting],
            reverse=self.direction is SortingDirection.DESC,
        )

    def toggle_sort(self: Self, sorting: ProcessSorting | None = None) -> None:
        """Sort and toggle ascending / descending if the sorting stays the same."""
        if not sorting or sorting is self.sorting:
            match self.direction:
                case SortingDirection.ASC:
                    self.direction = SortingDirection.DESC
                case SortingDirection.DESC:
                    self.direction = SortingDirection.ASC
        if sorting:
            self.sorting = sorting
        self.sort()


class ProcessTable(widgets.DataTable):
    """A Table for displaying AiiDA Processes."""

    class RowType(typing.NamedTuple):
        """A table row."""

        pk: int
        usable: str
        ok: str
        ptype: str
        pclass: str
        type: str
        created: str
        modified: str
        label: str
        description: rich.text.Text
        uuid: str

        @classmethod
        def from_node(cls, node: ProcessNode) -> Self:
            """Construct a row from a process node."""
            return cls(
                pk=node.pk or -1,
                usable=get_usable_symbol(node),
                ok=state_to_symbol(node.process_state, node.get_scheduler_state()),
                ptype=node.node_type.rsplit(".", 2)[-2],
                pclass=node.process_label or "",
                type=node.base.extras.get("type", ""),
                created=pendulum.instance(node.ctime).format("YYYY-MM-DD HH:mm:ss")
                or "NA",
                modified=pendulum.instance(node.mtime).format("YYYY-MM-DD HH:mm:ss")
                or "NA",
                label=node.label,
                description=rich.text.Text(
                    node.description, justify="left", overflow="fold"
                ),
                uuid=node.uuid,
            )

        @classmethod
        def columns(cls) -> typing.Iterator[tuple[str, str]]:
            """Iterate over label, key pairs for columns."""
            for field in cls._fields:
                yield (field, field)

    sorter: ProcessTableSorter

    def __init__(self: Self) -> None:
        """Construct a ProcessTable."""
        self.sorter = ProcessTableSorter(self)
        super().__init__(cursor_type="row", zebra_stripes=True, fixed_columns=1)
        self.add_columns(*self.RowType.columns())

    def populate(self: Self, processes: typing.Iterable[ProcessNode]) -> None:
        """Populate the table from process nodes."""
        self.add_rows([self.RowType.from_node(p) for p in processes])
