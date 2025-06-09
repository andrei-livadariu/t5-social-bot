from dataclasses import dataclass, replace
from copy import deepcopy
from typing import Optional


@dataclass(frozen=True)
class PingPongStanding:
    player_name: str
    rating: float
    wins: int = 0
    losses: int = 0
    rank: int = 1
    telegram_username: Optional[str] = None

    def __eq__(self, other):
        return self.player_name == other.player_name

    def __hash__(self):
        return hash(self.player_name)

    def copy(self, **changes) -> 'PingPongStanding':
        return replace(deepcopy(self), **changes)