from typing import Optional, TYPE_CHECKING
from datetime import datetime

from data.models.nomination import Nomination
from data.models.user import User
from data.repositories.nomination import NominationRepository
from integrations.google.sheets.contracts.tables.col_row_table import ColRowTable
from integrations.google.sheets.contracts.tables.insertable_table import InsertableTable
from integrations.google.sheets.indexes.sorted_bucket_index import SortedBucketIndex

if TYPE_CHECKING:
    from integrations.google.sheets.contracts.database import Database


class NominationsTable(
    ColRowTable[Nomination],
    InsertableTable[Nomination, dict[str, str]],
    NominationRepository,
):
    _append_to_top = True

    def __init__(self, database: 'Database', sheet_name: str):
        super().__init__(database, sheet_name)

        self._by_voter = SortedBucketIndex[Nomination, str](
            keys=lambda nomination: nomination.voted_by,
            sorter=lambda nomination: nomination.date,
            shared_lock=self._lock,
        )

    def has_voted(self, user: User, since: datetime) -> bool:
        nominations = self._by_voter.get(user.full_name)
        if not nominations:
            return False

        return nominations[0].date >= since

    def _serialize(self, model: Nomination) -> dict[str, str]:
        return {
            'nominee': model.nominee,
            'date': self._database.to_datetime_string(model.date),
            'voted_by': model.voted_by,
            'reason': model.reason,
        }

    def _deserialize(self, row: dict[str, str]) -> Optional[Nomination]:
        nominee = row.get('nominee', '').strip()
        if not nominee:
            return None

        date_object = self._database.from_datetime_string(row.get('date', ''))
        if not date_object:
            return None

        voted_by = row.get('voted_by', '')
        if not voted_by:
            return None

        return Nomination(
            nominee=nominee,
            date=date_object,
            voted_by=voted_by,
            reason=row.get('reason', '')
        )
