"""
Utilities to work with user data stored outside projects.

Either because the data is too sensitive for being under version control,
or because it is about configuration of hpclb overall.
"""

from __future__ import annotations

import pathlib

import platformdirs

USER_DATA_DIR = pathlib.Path(platformdirs.user_data_dir("hpclb", "ricoh"))


def get_user_data_dir() -> pathlib.Path:
    """Retrieve data dir, ensuring it exists."""
    if not USER_DATA_DIR.exists():
        USER_DATA_DIR.mkdir(parents=True)
    return USER_DATA_DIR
