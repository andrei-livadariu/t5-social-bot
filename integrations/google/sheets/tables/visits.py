import gspread

from datetime import datetime, date
from typing import Optional, TYPE_CHECKING

from data.models.user import User
from data.models.visits_entry import VisitsEntry
from data.repositories.visit import VisitRepository
from integrations.google.sheets.contracts.tables.col_row_table import ColRowTable
from integrations.google.sheets.contracts.tables.insertable_table import InsertableTable
from integrations.google.sheets.contracts.tables.updatable_table import UpdatableTable
from integrations.google.sheets.indexes.unique_index import UniqueIndex

if TYPE_CHECKING:
    from integrations.google.sheets.contracts.database import Database


class VisitsTable(
    ColRowTable[VisitsEntry],
    InsertableTable[VisitsEntry, dict[str, str]],
    UpdatableTable[VisitsEntry, str, dict[str, str]],
    VisitRepository
):
    def __init__(self, database: 'Database', sheet_name: str):
        super().__init__(database, sheet_name)

        self._by_full_name = UniqueIndex[VisitsEntry, str](lambda entry: entry.full_name, self._lock)

    def get_by_user(self, user: User) -> VisitsEntry:
        return self._by_full_name.get(user.full_name) or VisitsEntry(full_name=user.full_name)

    def save(self, entry: VisitsEntry) -> None:
        self.save_all([entry])

    def save_all(self, entries: list[VisitsEntry]) -> None:
        to_insert = []
        to_update = []

        existing_entries = self._by_full_name.get_all([entry.full_name for entry in entries])
        existing_names = {entry.full_name for entry in existing_entries}

        for entry in entries:
            if entry.full_name in existing_names:
                to_update.append(entry)
            else:
                to_insert.append(entry)

        self.update_all(to_update)
        self.insert_all(to_insert)

    def _get_key_name(self) -> str:
        return 'full_name'

    def _get_key(self, model: VisitsEntry) -> str:
        return model.full_name

    def _get_key_index(self) -> UniqueIndex:
        return self._by_full_name

    def _serialize(self, model: VisitsEntry) -> dict[str, str]:
        visits_by_month = {month.strftime('%b_%Y').lower(): str(visits) for month, visits in model.visits_by_month.items()}

        return {
            'full_name': model.full_name,
            'last_visit': self._database.to_datetime_string(model.last_visit),
            **visits_by_month,
        }

    def _deserialize(self, row: dict[str, str]) -> Optional[VisitsEntry]:
        # The full name is the primary key so it's required
        full_name = row.get('full_name', '').strip()
        if not full_name:
            return None

        parsed_visits = [(VisitsTable._parse_month(month), VisitsTable._parse_int(visits)) for month, visits in row.items()]
        visits_by_month = {month: visits for month, visits in parsed_visits if month and visits}

        return VisitsEntry(
            full_name=full_name,
            last_visit=self._database.from_datetime_string(row.get('last_visit', '')),
            visits_by_month=visits_by_month
        )

    @staticmethod
    def _parse_month(month_string: str) -> Optional[date]:
        try:
            return datetime.strptime(month_string, '%b_%Y').date()
        except ValueError:
            return None

    @staticmethod
    def _parse_int(int_string: str) -> Optional[int]:
        try:
            return int(int_string.strip())
        except ValueError:
            return None

    def _resolve_columns(self, worksheet: gspread.Worksheet, header: list[str], changed_rows: list[dict[str,str]]) -> dict[str, int]:
        columns = super()._resolve_columns(worksheet, header, changed_rows)

        # Check if we need to add any new months as columns in the table
        months_indexes = VisitsTable._get_months_indexes(columns)
        touched_months = VisitsTable._get_touched_months(changed_rows)
        new_months = touched_months - {month for month, index in months_indexes}

        if new_months:
            changed_indexes = VisitsTable._insert_month_columns(worksheet, new_months, months_indexes)
            for month, index in changed_indexes:
                columns[ColRowTable._header_to_key(month.strftime('%b %Y'))] = index

        return columns

    @staticmethod
    def _get_months_indexes(columns: dict[str, int]) -> list[(date, int)]:
        raw_indexes = [[VisitsTable._parse_month(column), index] for column, index in columns.items()]
        return [(month, index) for month, index in raw_indexes if month]

    @staticmethod
    def _get_touched_months(rows: list[dict[str,str]]) -> set[date]:
        # Get all the columns in all the changed rows
        touched_columns = set()
        for row in rows:
            touched_columns |= row.keys()

        raw_months = [VisitsTable._parse_month(column) for column in touched_columns]
        return {month for month in raw_months if month}

    @staticmethod
    def _insert_month_columns(worksheet: gspread.Worksheet, months: set[date], indexes: list[(date, int)]) -> list[(date, int)]:
        # This method inserts any missing months
        # Example:
        # months: apr mar jan
        # indexes: feb 3 dec 4 nov 5
        # result step by step:
        # indexes: apr 3 feb 4 dec 5 nov 6
        # indexes: apr 3 mar 4 feb 5 dec 6 nov 7
        # indexes: apr 3 mar 4 feb 5 jan 6 dec 7 nov 8

        # Make a copy, as we will be modifying this list
        indexes = indexes.copy()

        # For optimization purposes, the position can be carried between loops
        position = 0

        for month in sorted(months, reverse=True):
            # Skip existing months, stop when we find a month that's older than the current month
            while position < len(indexes) and month < indexes[position][0]:
                position += 1

            if position < len(indexes):
                index = indexes[position][1] # Get the index of the month we reached
            elif len(indexes) > 0:
                index = indexes[-1][1] + 1  # Go just after the index of the last column
            else:
                index = 2 # min index is 2 - this is a failsafe in case there are no months in the index (only if someone drops everything from the table)

            # Add the column to the sheet
            worksheet.insert_cols(
                values=[[month.strftime('%b %Y')]],
                col=index + 1, # one-based
                inherit_from_before=(position >= len(indexes)) # inherit before if we're at the end, otherwise after
            )
            # Add the column to the indexes as well
            indexes.insert(position, (month, index))

            # Advance the position to skip the column we just added
            position += 1

            # Advance the indexes for any columns left in the indexes array
            for i in range(position, len(indexes)):
                indexes[i] = (indexes[i][0], indexes[i][1] + 1)

        return indexes


