"""Test jobspec rehydration, grouping and annotating."""

from __future__ import annotations

import pathlib
import typing
from typing import Callable

import pytest

from hpclb.aiida import data
from hpclb.aiida.data import jobspec

if typing.TYPE_CHECKING:
    from aiida.orm import Code, InstalledCode


def test_minimal_spec(aiida_code_installed: Callable[..., InstalledCode]) -> None:
    """Make sure no fields are required that shouldn't be."""
    spec = jobspec.Generic(
        code=(code := aiida_code_installed()).label,
        workdir=data.TargetDir("root"),
        label="testjob",
        description="Minimal GenericJobCalc spec",
        queue="default",
    )
    assert spec.load_code() == code
    assert spec.load_computer() == code.computer
    _ = spec.to_builder()  # make sure no errors thrown


def test_missing_comp(aiida_code: Callable[..., Code]) -> None:
    """Make sure that missing computer raises error."""
    code = aiida_code(
        "core.code.portable",
        filepath_executable="dummy.sh",
        filepath_files=pathlib.Path(__file__).parent / "data",
    )
    spec = jobspec.Generic(
        code=code.label,
        workdir=data.TargetDir("root"),
        label="testjob",
        description="Minimal GenericJobCalc spec",
        queue="default",
    )
    with pytest.raises(jobspec.ComputerNotFoundError):
        _ = spec.load_computer()
