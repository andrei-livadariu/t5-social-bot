from typing import Optional, TYPE_CHECKING

from data.models.ping_pong_match import PingPongMatch
from data.repositories.ping_pong_match import PingPongMatchRepository
from integrations.google.sheets.contracts.tables.col_row_table import ColRowTable
from integrations.google.sheets.contracts.tables.insertable_table import InsertableTable
from integrations.google.sheets.indexes.sorted_bucket_index import SortedBucketIndex

if TYPE_CHECKING:
    from integrations.google.sheets.contracts.database import Database


class PingPongMatchesTable(
    ColRowTable[PingPongMatch],
    InsertableTable[PingPongMatch, dict[str, str]],
    PingPongMatchRepository,
):
    _append_to_top = True

    def __init__(self, database: 'Database', sheet_name: str):
        super().__init__(database, sheet_name)

        self._by_player_name = SortedBucketIndex[PingPongMatch, str](
            keys=[
                lambda model: model.winner,
                lambda model: model.loser,
            ],
            sorter=lambda model: model.date,
            shared_lock=self._lock
        )

    def get_streak(self, player_name: str) -> int:
        last_result = None
        streak = 0

        for match in self.get_matches(player_name)[::-1]: # reversed
            result = 1 if match.winner == player_name else -1
            if last_result and result != last_result:
                return streak

            streak += result
            last_result = result

        return streak

    def get_matches(self, player_name: str) -> list[PingPongMatch]:
        return self._by_player_name.get(player_name) or []

    def _serialize(self, model: PingPongMatch) -> dict[str, str]:
        return {
            'date': self._database.to_datetime_string(model.date),
            'winner': model.winner,
            'loser': model.loser,
        }

    def _deserialize(self, row: dict[str, str]) -> Optional[PingPongMatch]:
        played_date = self._database.from_datetime_string(row.get('date', '').strip())
        if not played_date:
            return None

        winner = row.get('winner', '').strip()
        if not winner:
            return None

        loser = row.get('loser', '').strip()
        if not loser:
            return None

        return PingPongMatch(
            date=played_date,
            winner=winner,
            loser=loser
        )
