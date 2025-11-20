"""Test jobspec rehydration, grouping and annotating."""

from __future__ import annotations

import typing

import pytest

from hpclb import jobspec
from hpclb.aiida import data

if typing.TYPE_CHECKING:
    from aiida.orm import Code, InstalledCode


def test_minimal_spec(aiida_code_installed: InstalledCode) -> None:
    """Make sure no fields are required that shouldn't be."""
    spec = jobspec.Generic(
        code=aiida_code_installed.label,
        workdir=data.TargetDir("root"),
        label="testjob",
        description="Minimal GenericJobCalc spec",
        queue="default",
    )
    assert spec.load_code() == aiida_code_installed
    assert spec.load_computer() == aiida_code_installed.computer
    _ = spec.to_builder()  # make sure no errors thrown


def test_missing_comp(aiida_code: typing.Callable[..., Code]) -> None:
    """Make sure that missing computer raises error."""
    code = aiida_code("core.code.portable")
    spec = jobspec.Generic(
        code=code.label,
        workdir=data.TargetDir("root"),
        label="testjob",
        description="Minimal GenericJobCalc spec",
        queue="default",
    )
    with pytest.raises(jobspec.ComputerNotFoundError):
        _ = spec.load_computer()
