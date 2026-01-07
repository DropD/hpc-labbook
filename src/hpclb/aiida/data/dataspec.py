"""Allow users to specify arbitrary AiiDA data structures in yaml / json."""

from __future__ import annotations

import dataclasses

from hpclb.aiida.data import jsonable


@dataclasses.dataclass
class DataSpec(jsonable.JsonableMixin):
    """Represent any AiiDA data type."""

    entry_point: str
    constructor: str
    args: list[str | int | float | dict[str, str | int | float]]
    kwargs: dict[str, str | int | float | dict[str, str | int | float]]
