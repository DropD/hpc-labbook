"""JSON serializing / deserializing mixin for data structures."""

from __future__ import annotations

from cattrs.preconf.json import make_converter
from typing_extensions import Self

CONVERTER = make_converter()


class JsonableMixin:
    """
    Defines API required by 'aiida.orm.JsonableData'.

    Can be used to augment dataclasses or 'attrs' classes.
    """

    def as_dict(self: Self) -> dict[str, str]:
        """Convert to jsonable dictionary."""
        return CONVERTER.unstructure(self)

    @classmethod
    def from_dict(cls: type[Self], data: dict[str, str]) -> Self:
        """Reconstruct from dictionary."""
        return CONVERTER.structure(data, cls)
