from dataclasses import dataclass, replace
from copy import deepcopy
from datetime import datetime

@dataclass(frozen=True)
class PingPongMatch:
    date: datetime
    winner: str
    loser: str

    def copy(self, **changes) -> 'PingPongMatch':
        return replace(deepcopy(self), **changes)