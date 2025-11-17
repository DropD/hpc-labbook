"""
The hpc-labbook CLI.

Commands:
- init: create a new project
- add-site: add infrastructure for a compute site (only CSCS included)
- auth-site: authenticate to a compute site's resources
"""

from __future__ import annotations

from hpclb.cli import cscs, f7ttest, init_cmd, jobs_cmd, run_generic_cmd
from hpclb.cli.app import app

__all__ = ["app", "cscs", "f7ttest", "init_cmd", "jobs_cmd", "run_generic_cmd"]
