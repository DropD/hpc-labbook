"""Test the graph workchain."""

from __future__ import annotations

import aiida
import aiida.orm
import pytest


@pytest.mark.cluster
def test_min_graph(
    cluster: aiida.orm.Computer, cluster_dummy: aiida.orm.InstalledCode
) -> None:
    """Test the smallest possible graph."""
    assert cluster.label == "testcluster"
    assert cluster_dummy.label == "dummy"
