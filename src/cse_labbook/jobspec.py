"""Data structures describing hpclb jobs."""

from __future__ import annotations

import dataclasses
import pathlib

from cse_labbook.aiida import data


@dataclasses.dataclass
class Uenv:
    """An ALPS user environment, image must have been pulled previously."""

    name: str
    view: str = ""


@dataclasses.dataclass
class Generic:
    """Represent Generic CalcJob."""

    code: str
    workdir: data.TargetDir
    label: str
    description: str
    queue: str
    envvars: dict[str, str] = dataclasses.field(default_factory=dict)
    extras: dict[str, str] = dataclasses.field(default_factory=dict)
    setup_script: list[str] = dataclasses.field(default_factory=list)
    cleanup_script: list[str] = dataclasses.field(default_factory=list)
    resources: dict[str, int] = dataclasses.field(default_factory=dict)
    uploads: dict[str, pathlib.Path] = dataclasses.field(default_factory=dict)
    args: list[str] = dataclasses.field(default_factory=list)
    withmpi: bool = True
    computer: str | None = None
    uenv: Uenv | None = None
