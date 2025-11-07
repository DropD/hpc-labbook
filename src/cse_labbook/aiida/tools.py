from __future__ import annotations

import dataclasses


@dataclasses.dataclass
class Downloads:
    """
    Downloads accessor for a GenericCalculation's resulting CalcJobNode.

    Example:
        inputs = ...
        mycalcjobnode = engine.run(GenericCalculation(**inputs))
        downloads = Downloads(mycalcjobnode)

        downloads.get("stdout", "")  # returns the content of the job scheduler stdout or empty string

    """
