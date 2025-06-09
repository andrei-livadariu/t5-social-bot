import logging

import pytz
from datetime import datetime

from readerwriterlock.rwlock import RWLockWrite

from data.models.user import User
from data.models.ping_pong_standing import PingPongStanding
from data.models.ping_pong_match import PingPongMatch
from data.repositories.ping_pong_match import PingPongMatchRepository
from data.repositories.ping_pong_standing import PingPongStandingRepository
from helpers.business_logic.league.delta import Delta

from helpers.business_logic.league.elo import Elo
from helpers.business_logic.league.match_result import MatchResult

logger = logging.getLogger(__name__)


class PingPongCalculator:
    def __init__(self, standings: PingPongStandingRepository, matches: PingPongMatchRepository, timezone: pytz.timezone = None):
        self._standings = standings
        self._matches = matches
        self._elo = Elo(20)
        self._timezone = timezone

        self._lock = RWLockWrite()

    def get_standing(self, player: User) -> PingPongStanding:
        return self._standings.get_standing(player.full_name) or self._init_standing(player)

    def get_streak(self, player: User) -> int:
        return self._matches.get_streak(player.full_name)

    def game_over(self, winner: User, loser: User) -> tuple[MatchResult, MatchResult]:
        # Get the existing data
        old_winner_standing = self.get_standing(winner)
        old_loser_standing = self.get_standing(loser)

        old_winner_streak = self.get_streak(winner)
        old_loser_streak = self.get_streak(loser)

        # Use the ELO algorithm to calculate the new ratings
        (new_winner_rating, new_loser_rating) = self._elo.game_over(old_winner_standing.rating, old_loser_standing.rating)
        # Update the ratings and the win/loss count - this is a "partial" result because the rank might also need updating
        partial_winner_standing = old_winner_standing.copy(rating=new_winner_rating, wins=old_winner_standing.wins + 1)
        partial_loser_standing = old_loser_standing.copy(rating=new_loser_rating, losses=old_loser_standing.losses + 1)

        # Recalculate the ranks, which can affect more players than just the two that played
        standing_updates = self._calculate_rank_changes(partial_winner_standing, partial_loser_standing)

        # Save the changes to storage
        self._standings.save_all(standing_updates)
        self._matches.insert(PingPongMatch(
            date=datetime.now(self._timezone),
            winner=winner.full_name,
            loser=loser.full_name,
        ))

        # The objects coming from standing_updates have the correct rank as well
        new_winner_standing = self._find_in_list(standing_updates, partial_winner_standing)
        new_loser_standing = self._find_in_list(standing_updates, partial_loser_standing)

        return (
            MatchResult(
                player=winner,
                rating=Delta(before=old_winner_standing.rating, after=new_winner_standing.rating),
                rank=Delta(before=old_winner_standing.rank, after=new_winner_standing.rank),
                streak=Delta(before=old_winner_streak, after=self._advance_streak(old_winner_streak, 1))
            ),
            MatchResult(
                player=loser,
                rating=Delta(before=old_loser_standing.rating, after=new_loser_standing.rating),
                rank=Delta(before=old_loser_standing.rank, after=new_loser_standing.rank),
                streak=Delta(before=old_loser_streak, after=self._advance_streak(old_loser_streak, -1))
            ),
        )

    def _init_standing(self, player: User) -> PingPongStanding:
        return PingPongStanding(
            player_name=player.full_name,
            telegram_username=player.telegram_username,
            rating=self._elo.base_rating,
        )

    def _advance_streak(self, streak: int, change: int) -> int:
        # If the streak and the change have the same sign, continue the streak
        if streak * change > 0:
            return streak + change

        # Otherwise reset the streak
        return change

    def _calculate_rank_changes(self, winner_standing: PingPongStanding, loser_standing: PingPongStanding) -> list[PingPongStanding]:
        # Get the standings for all the players
        all_standings = self._standings.get_all_standings()

        # The list contains old data for the winner/loser. So we need to splice the updated data into the list.
        self._splice_into_list(all_standings, winner_standing)
        self._splice_into_list(all_standings, loser_standing)

        # The rank changes might affect more than just the winner/loser. So we keep a list of all the models that change.
        standing_updates = []
        # Sort by rating
        all_standings.sort(key=lambda s: s.rating, reverse=True)
        # Go through all the players and check if their saved rank is different from their calculated rank
        for i, standing in enumerate(all_standings):
            correct_rank = i + 1
            if standing.rank != correct_rank:
                standing_updates.append(standing.copy(rank=correct_rank))

        # Even if the winner/loser have not changed rank, they still need to be added to the update list because they changed their rating
        if winner_standing not in standing_updates:
            standing_updates.append(winner_standing)
        if loser_standing not in standing_updates:
            standing_updates.append(loser_standing)

        return standing_updates

    def _splice_into_list(self, standing_list: list[PingPongStanding], standing: PingPongStanding) -> None:
        try:
            i = standing_list.index(standing)
            standing_list[i] = standing
        except ValueError:
            standing_list.append(standing)

    def _find_in_list(self, standing_list: list[PingPongStanding], standing: PingPongStanding) -> PingPongStanding:
        try:
            i = standing_list.index(standing)
            return standing_list[i]
        except ValueError:
            return standing

