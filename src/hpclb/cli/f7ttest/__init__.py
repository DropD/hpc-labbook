"""Site specific cli commands for the Firecrestv2 test cluster container fleet."""

from __future__ import annotations

from hpclb.cli.f7ttest.add_f7ttest_cmd import add_f7ttest
from hpclb.cli.f7ttest.auth_f7ttest_cmd import auth_f7ttest

__all__ = ["add_f7ttest", "auth_f7ttest"]
