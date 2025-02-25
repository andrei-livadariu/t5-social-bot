from typing import Optional

from data.models.raffle_entry import RaffleEntry
from integrations.google.sheets.contracts.tables.col_row_table import ColRowTable
from integrations.google.sheets.contracts.tables.insertable_table import InsertableTable


class RaffleEntriesTable(
    ColRowTable[RaffleEntry],
    InsertableTable[RaffleEntry, dict[str, str]],
):
    def _serialize(self, model: RaffleEntry) -> dict[str, str]:
        return {
            'champion_name': model.full_name,
            'date': self._database.to_datetime_string(model.created_at),
            'country': model.country,
        }

    def _deserialize(self, row: dict[str, str]) -> Optional[RaffleEntry]:
        full_name = row.get('champion_name', '').strip()
        if not full_name:
            return None

        country = row.get('country', '').strip()
        if not country:
            return None

        return RaffleEntry(
            full_name=full_name,
            created_at=self._database.from_datetime_string(row.get('date', '')),
            country=country,
        )
