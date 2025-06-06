from datetime import datetime
from typing import Optional, TYPE_CHECKING

from data.models.raffle_entry import RaffleEntry
from data.models.user import User
from data.repositories.raffle import RaffleRepository
from integrations.google.sheets.contracts.tables.col_row_table import ColRowTable
from integrations.google.sheets.contracts.tables.insertable_table import InsertableTable
from integrations.google.sheets.indexes.bucket_index import BucketIndex

if TYPE_CHECKING:
    from integrations.google.sheets.contracts.database import Database

class RaffleEntriesTable(
    ColRowTable[RaffleEntry],
    InsertableTable[RaffleEntry, dict[str, str]],
    RaffleRepository,
):
    def __init__(self, database: 'Database', sheet_name: str):
        super().__init__(database, sheet_name)

        self._by_full_name: BucketIndex[RaffleEntry, str] = BucketIndex(lambda entry: entry.full_name, self._lock)

    def get_by_user(self, user: User) -> set[RaffleEntry]:
        return self._by_full_name.get(user.full_name) or set()

    def list_by_user(self) -> dict[str, set[RaffleEntry]]:
        return self._by_full_name.raw()

    def create(self, user: User) -> RaffleEntry:
        entry = RaffleEntry(
            full_name=user.full_name,
            created_at=datetime.now(tz=self._database.timezone),
        )
        self.insert(entry)
        return entry

    def _serialize(self, model: RaffleEntry) -> dict[str, str]:
        return {
            'full_name': model.full_name,
            'date': self._database.to_datetime_string(model.created_at),
        }

    def _deserialize(self, row: dict[str, str]) -> Optional[RaffleEntry]:
        full_name = row.get('full_name', '').strip()
        if not full_name:
            return None

        return RaffleEntry(
            full_name=full_name,
            created_at=self._database.from_datetime_string(row.get('date', '')),
        )
