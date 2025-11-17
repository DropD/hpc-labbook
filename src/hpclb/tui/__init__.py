"""Process browser for the HPC Labbook."""

from __future__ import annotations

import pprint
import textwrap
import typing

import aiida
import pendulum
import rich.markdown
import rich.text
import textual.app
from aiida import orm
from textual import containers, widgets
from typing_extensions import Self

__all__ = ["ProcessBrowser"]


def get_usable_symbol(node: orm.ProcessNode) -> str:
    """Get symbol to represent the 'usable' extra."""
    match node.base.extras.get("usable", None):
        case True:
            return "✅"
        case False:
            return "❌"
        case _:
            return "?"


class ProcessBrowser(textual.app.App):
    """A textual app for browsing AiiDA processes."""

    BINDINGS: typing.ClassVar = [
        ("d", "toggle_dark", "Toggle dark mode"),
        ("p", "sort_by_pk", "Sort by PK"),
        ("l", "sort_by_label", "Sort by label"),
        ("u", "sort_by_usable", "Sort by usability"),
        ("t", "sort_by_type", "Sort by type"),
        ("q", "quit", "Quit"),
    ]

    CSS: typing.ClassVar = """
    DataTable {
        width: 1fr;
    }
    MarkdownViewer {
        width: 1fr;
    }
    """

    def compose(self: Self) -> textual.app.ComposeResult:
        """Create the app's child widgets."""
        yield widgets.Header()
        with containers.Horizontal():
            yield widgets.DataTable(fixed_columns=1)
            yield widgets.MarkdownViewer()
        yield widgets.Footer()

    def action_toggle_dark(self: Self) -> None:
        """Toggle dark theme."""
        self.theme = (
            "textual-dark" if self.theme == "textual-light" else "textual-light"
        )

    def on_mount(self: Self) -> None:
        """Populate the data table."""
        aiida.load_profile()
        table = self.query_one(widgets.DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_columns(
            ("PK", "pk"),
            ("created", "ctime"),
            ("modified", "mtime"),
            ("Label", "label"),
            ("Description", "desc"),
            ("UUID", "uuid"),
            ("type", "type"),
            ("usable", "usable"),
        )
        table.add_rows(
            [
                (
                    i.pk,
                    pendulum.instance(i.ctime).format("YYYY-MM-DD HH:mm:ss"),
                    pendulum.instance(i.mtime).format("YYYY-MM-DD HH:mm:ss"),
                    i.label,
                    rich.text.Text(i.description, justify="left", overflow="fold"),
                    i.uuid,
                    i.base.extras.get("type", ""),
                    get_usable_symbol(i),
                )
                for i in self.all_processes()
            ]
        )
        table.sort("pk", reverse=True)

    def on_data_table_row_selected(
        self: Self, message: widgets.DataTable.RowHighlighted
    ) -> None:
        """Show details of the selected process in a markdown viewer."""
        # TODO(ricoh): issue #894bfe90c3a7bf184ded782ffac9cf440be9a4b3
        # replace with custom widget, generating markdown is tedious:
        row = message.data_table.get_row(message.row_key)
        node = orm.load_node(pk=row[0])
        detail = self.query_one(widgets.MarkdownViewer)
        inputs = []
        for key in node.inputs._get_keys():
            value = ""
            match data := node.inputs[key]:
                case orm.JsonableData():
                    try:
                        value = pprint.pformat(data.obj)
                    except ImportError:
                        value = pprint.pformat(data)
                case _:
                    value = pprint.pformat(data)
            inputs.append(f"__{key}__:" + "\n" + value)
        detail.show_table_of_contents = False
        detail.document.update(
            textwrap.dedent(
                f"""
            ## Info

            __PK__: {node.pk}
            __label__: {node.pk}
            __created__: {node.ctime}
            __last modified__: {node.mtime}
            __description__: {node.description}

            ## Options

            {pprint.pformat(node.get_options())}

            ## Inputs

            {"\n\n".join(inputs)}
            """
            )
        )

    def all_processes(self: Self) -> list[orm.ProcessNode]:
        """
        Return all process nodes.

        Used to populate the initial list
        """
        qb = orm.QueryBuilder()
        qb.append(orm.ProcessNode)
        return [j for i in qb.all() for j in i]

    def action_sort_by_pk(self: Self) -> None:
        """Sort the table by PK."""
        table = self.query_one(widgets.DataTable)
        table.sort("pk", reverse=True)

    def action_sort_by_label(self: Self) -> None:
        """Sort the table by label."""
        table = self.query_one(widgets.DataTable)
        table.sort("label", "pk", key=lambda d: (d[0], -d[1]))

    def action_sort_by_usable(self: Self) -> None:
        """Sort the table by 'usable' extra."""
        table = self.query_one(widgets.DataTable)
        usability_level = {"✅": 0, "❌": 1, "?": 2}
        table.sort("usable", "pk", key=lambda d: (usability_level[d[0]], -d[1]))

    def action_sort_by_type(self: Self) -> None:
        """Sort the table by 'type' extra."""
        table = self.query_one(widgets.DataTable)
        table.sort("type", "pk", key=lambda d: (d[0] if d[0] else "Ω", -d[1]))
