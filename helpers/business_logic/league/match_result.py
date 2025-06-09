from dataclasses import dataclass

from data.models.user import User

from helpers.business_logic.league.delta import Delta

@dataclass(frozen=True)
class MatchResult:
    player: User
    rating: Delta[float]
    rank: Delta[int]
    streak: Delta[int]
