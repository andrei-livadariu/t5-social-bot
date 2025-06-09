from typing import Optional, TYPE_CHECKING

from data.models.ping_pong_standing import PingPongStanding
from data.repositories.ping_pong_standing import PingPongStandingRepository
from integrations.google.sheets.contracts.tables.col_row_table import ColRowTable
from integrations.google.sheets.contracts.tables.insertable_table import InsertableTable
from integrations.google.sheets.contracts.tables.updatable_table import UpdatableTable
from integrations.google.sheets.indexes.unique_index import UniqueIndex

if TYPE_CHECKING:
    from integrations.google.sheets.contracts.database import Database


class PingPongStandingsTable(
    ColRowTable[PingPongStanding],
    InsertableTable[PingPongStanding, dict[str, str]],
    UpdatableTable[PingPongStanding, str, dict[str, str]],
    PingPongStandingRepository,
):
    def __init__(self, database: 'Database', sheet_name: str):
        super().__init__(database, sheet_name)

        self._by_player_name = UniqueIndex[PingPongStanding, str](lambda model: model.player_name, self._lock)

    def _get_key_name(self) -> str:
        return 'player_name'

    def _get_key(self, model: PingPongStanding) -> str:
        return model.player_name

    def _get_key_index(self) -> UniqueIndex:
        return self._by_player_name

    def get_all_standings(self) -> list[PingPongStanding]:
        return list(self._by_player_name.raw().values())

    def get_standing(self, player_name: str) -> PingPongStanding:
        return self._by_player_name.get(player_name)

    def save(self, model: PingPongStanding) -> None:
        self.save_all([model])

    def save_all(self, models: list[PingPongStanding]) -> None:
        to_insert = []
        to_update = []

        existing_models = self._by_player_name.get_all([entry.player_name for entry in models])
        existing_names = {entry.player_name for entry in existing_models}

        for entry in models:
            if entry.player_name in existing_names:
                to_update.append(entry)
            else:
                to_insert.append(entry)

        self.update_all(to_update)
        self.insert_all(to_insert)
        self._sort()

    def _sort(self) -> None:
        with self._get_lock().gen_wlock():
            spreadsheet = self._database.load()
            worksheet = self._load_worksheet(spreadsheet)
            worksheet.sort((1, 'asc'))

    def _serialize(self, model: PingPongStanding) -> dict[str, str]:
        return {
            'player_name': model.player_name,
            'rating': model.rating,
            'wins': model.wins,
            'losses': model.losses,
            'rank': model.rank,
            'telegram_username': model.telegram_username,
        }

    def _deserialize(self, row: dict[str, str]) -> Optional[PingPongStanding]:
        player_name = row.get('player_name', '').strip()
        if not player_name:
            return None

        rating = PingPongStandingsTable._parse_float(row.get('rating', ''))
        if not rating:
            return None

        return PingPongStanding(
            player_name=player_name,
            rating=rating,
            wins=PingPongStandingsTable._parse_int(row.get('wins', '')) or 0,
            losses=PingPongStandingsTable._parse_int(row.get('losses', '')) or 0,
            rank=PingPongStandingsTable._parse_int(row.get('rank', '')) or 1,
            telegram_username=row.get('telegram_username', '').strip() or None,
        )

    @staticmethod
    def _parse_float(float_string: str) -> Optional[float]:
        try:
            return float(float_string.strip())
        except ValueError:
            return None

    @staticmethod
    def _parse_int(int_string: str) -> Optional[int]:
        try:
            return int(int_string.strip())
        except ValueError:
            return None
