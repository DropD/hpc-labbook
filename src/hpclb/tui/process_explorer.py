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
from aiida.cmdline.utils.common import get_calcjob_report, get_workchain_report
from textual import containers, widgets
from textual.widgets import tree as widgets_tree
from typing_extensions import Self

__all__ = ["ProcessBrowser"]


def bool_to_symbol(value: bool | None) -> str:
    match value:
        case True:
            return "✅"
        case False:
            return "❌"
        case _:
            return "?"


def get_usable_symbol(node: orm.ProcessNode) -> str:
    """Get symbol to represent the 'usable' extra."""
    return bool_to_symbol(node.base.extras.get("usable", None))


class ProcessDetail(widgets.Static):
    """A Detail view for a process node."""

    def __init__(self: Self, node: orm.ProcessNode | None = None) -> None:
        super().__init__()
        if node:
            self.update(node)

    def compose(self: Self) -> textual.app.ComposeResult:
        """Compose the info items."""
        with widgets.TabbedContent(initial="general-pane"):
            with (
                widgets.TabPane("General & Inputs", id="general-pane"),
                containers.VerticalScroll(),
            ):
                yield widgets.Markdown(id="info")
                yield widgets.Markdown(id="options")
                yield widgets.Markdown(id="inputs")
                yield widgets.Tree("Subtasks", id="subtasks")
            with (
                widgets.TabPane("Stdout", id="stdout-pane"),
                containers.VerticalScroll(),
            ):
                yield widgets.Markdown(id="stdout")
            with (
                widgets.TabPane("Stderr", id="stderr-pane"),
                containers.VerticalScroll(),
            ):
                yield widgets.Markdown(id="stderr")
            with (
                widgets.TabPane("Reports", id="reports-pane"),
                containers.VerticalScroll(),
            ):
                yield widgets.Markdown(id="reports")

    def update(self: Self, node: orm.ProcessNode) -> None:  # type: ignore[override]
        self.update_info(node)
        self.update_options(node)
        self.update_inputs(node)
        self.update_stdout(node)
        self.update_stderr(node)
        self.update_reports(node)
        self.update_subtasks(node)
        super().update()

    def update_info(self: Self, node: orm.ProcessNode) -> None:
        info = self.query_one("#info", expect_type=widgets.Markdown)
        workdir = "NA"
        if get_workdir := getattr(node, "get_remote_workdir", None):
            workdir = get_workdir()
        info.update(
            textwrap.dedent(
                f"""
            ## Info

            __PK__
            : {node.pk}

            __state__
            : {node.process_state}

            __class__
            : {node.process_label}

            __entry point__
            : {node.process_type}

            __label__
            : {node.pk}

            __created__
            : {node.ctime}

            __last modified__
            : {node.mtime}

            __description__
            : {node.description}

            __workdir__
            : {workdir}
            """
            )
        )

    def update_options(self: Self, node: orm.ProcessNode) -> None:
        if not hasattr(node, "get_options"):
            return
        options = self.query_one("#options", expect_type=widgets.Markdown)
        options_section = ["## Options"]
        for key, value in node.get_options().items():
            if key == "prepend_text":
                options_section.append(f"__{key}__\n: \n```bash\n{value}\n```\n")
            elif key == "environment_variables":
                options_section.append(f"__{key}__\n: \n")
                options_section.extend([f"- {k}: {v}" for k, v in value.items()])
                options_section.append("")
            else:
                options_section.append(f"__{key}__\n: {value}\n")
        options.update("\n".join(options_section))

    def update_inputs(self: Self, node: orm.ProcessNode) -> None:
        inputs = self.query_one("#inputs", expect_type=widgets.Markdown)
        inputs_section = ["## Inputs"]
        for key in node.inputs._get_keys():
            value = node.inputs[key]
            inputs_section.append(f"### {key}")
            match value:
                case orm.JsonableData():
                    try:
                        inputs_section.append("```python")
                        inputs_section.append(pprint.pformat(value.obj))
                        inputs_section.append("```")
                    except ImportError:
                        inputs_section.append("```")
                        inputs_section.append(
                            pprint.pformat(value.backend_entity.attributes)
                        )
                        inputs_section.append("```")
                case orm.List():
                    inputs_section.extend([f"- {i}" for i in value.value])
                case orm.InstalledCode():
                    inputs_section.append(f"__label__\n: {value.label}\n")
                    inputs_section.append(f"__computer__\n: {value.computer}\n")
                    inputs_section.append(f"__path__\n: {value.filepath_executable}\n")
                case _:
                    inputs_section.append(pprint.pformat(value))
        inputs.update("\n".join(inputs_section))

    def update_stdout(self: Self, node: orm.ProcessNode) -> None:
        stdout_name = "_scheduler-stdout.txt"
        retrieved = node.outputs.retrieved if "retrieved" in node.outputs else None
        stderr = self.query_one("#stdout", expect_type=widgets.Markdown)
        if retrieved and stdout_name in retrieved.list_object_names():
            stderr.update(f"```\n{retrieved.get_object_content(stdout_name)}\n```")
        else:
            stderr.update("Not found")

    def update_stderr(self: Self, node: orm.ProcessNode) -> None:
        stderr_name = "_scheduler-stderr.txt"
        retrieved = node.outputs.retrieved if "retrieved" in node.outputs else None
        stderr = self.query_one("#stderr", expect_type=widgets.Markdown)
        if retrieved and stderr_name in retrieved.list_object_names():
            stderr.update(f"```\n{retrieved.get_object_content(stderr_name)}\n```")
        else:
            stderr.update("Not found")

    def update_reports(self: Self, node: orm.ProcessNode) -> None:
        reports: widgets.Markdown = self.query_one(
            "#reports", expect_type=widgets.Markdown
        )
        if node.node_type.rsplit(".", 2)[-2] == "WorkChainNode":
            reports.update(
                textwrap.dedent(
                    f"""
                ```
                {get_workchain_report(typing.cast(orm.WorkChainNode, node), "INFO")}
                ```
                """
                )
            )
        elif node.node_type.rsplit(".", 2)[-2] == "CalcJobNode":
            reports.update(f"```\n{get_calcjob_report(node)}\n```")
        else:
            reports.update("NA")

    def update_subtasks(self: Self, node: orm.ProcessNode) -> None:
        subtasks: widgets.Tree[str] = self.query_one(
            "#subtasks", expect_type=widgets.Tree
        )
        subtasks.root.remove_children()

        def add_nodes(
            some_node: orm.ProcessNode, tree_node: widgets_tree.TreeNode
        ) -> None:
            for called in some_node.called:
                if called.called:
                    new_node = tree_node.add(str(called))
                    add_nodes(called, new_node)
                else:
                    tree_node.add_leaf(str(called))

        add_nodes(node, subtasks.root)
        subtasks.root.expand()


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
    ProcessDetail {
        width: 1fr;
    }
    """

    def compose(self: Self) -> textual.app.ComposeResult:
        """Create the app's child widgets."""
        yield widgets.Header()
        with containers.Horizontal():
            yield widgets.DataTable(fixed_columns=1)
            yield ProcessDetail()
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
            ("usable", "usable"),
            ("ok", "ok"),
            ("ptype", "ptype"),
            ("pclass", "pclass"),
            ("type", "type"),
            ("created", "ctime"),
            ("modified", "mtime"),
            ("Label", "label"),
            ("Description", "desc"),
            ("UUID", "uuid"),
        )
        table.add_rows(
            [
                (
                    i.pk,
                    get_usable_symbol(i),
                    bool_to_symbol(i.is_finished_ok),
                    i.node_type.rsplit(".", 2)[-2],
                    i.process_label,
                    i.base.extras.get("type", ""),
                    pendulum.instance(i.ctime).format("YYYY-MM-DD HH:mm:ss"),
                    pendulum.instance(i.mtime).format("YYYY-MM-DD HH:mm:ss"),
                    i.label,
                    rich.text.Text(i.description, justify="left", overflow="fold"),
                    i.uuid,
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
        node: orm.ProcessNode = typing.cast(orm.ProcessNode, orm.load_node(pk=row[0]))
        detail: ProcessDetail = self.query_one(ProcessDetail)
        detail.update(node)

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
