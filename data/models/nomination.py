from dataclasses import dataclass, replace
from datetime import datetime
from copy import deepcopy


@dataclass(frozen=True)
class Nomination:
    nominee: str
    date: datetime
    voted_by: str
    reason: str

    def copy(self, **changes) -> 'Nomination':
        return replace(deepcopy(self), **changes)

