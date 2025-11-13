"""CSCS specific cli commands."""

from __future__ import annotations

from hpclb.cli.cscs.add_cscs_cmd import add_cscs
from hpclb.cli.cscs.auth_cscs_cmd import auth_cscs

__all__ = ["add_cscs", "auth_cscs"]

SITE_DIR_NAME = "cscs"
