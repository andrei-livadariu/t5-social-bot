import logging
import re
from typing import Optional
from reactivex import Observable, operators as op
from reactivex.subject import BehaviorSubject

import gspread
from gspread.utils import Dimension

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from integrations.google.sheet_database import GoogleSheetDatabase

logger = logging.getLogger(__name__)


class GoogleSheetDatabaseTable:
    _dimension = Dimension.rows

    def __init__(self, database: 'GoogleSheetDatabase', sheet_name: str, key_name: Optional[str] = None):
        self._database = database
        self._sheet_name = sheet_name
        self._key_name = key_name

        self._data = self._attach()

    @property
    def data(self) -> Observable:
        return self._data

    def insert(self, data: dict[str,str]) -> None:
        try:
            # Load the data from Google
            spreadsheet = self._database.load()
            worksheet = self._load_worksheet(spreadsheet)
            raw = self._load_values(worksheet)

            header = raw[0]

            # Map the header keys to their column numbers - instead of A, B, C we use 0, 1, 2
            columns = {GoogleSheetDatabaseTable._header_to_key(h): i for i, h in enumerate(header)}

            # Map the data entries to their column numbers
            updates_by_column = {columns[k]: v for k, v in data.items() if k in columns}

            # We add 1 to the row to account for the headers
            GoogleSheetDatabaseTable._insert_row(worksheet, updates_by_column)
        except Exception as e:
            logger.exception(e)

    def update(self, data: dict[str, dict[str,str]]) -> None:
        if not data:
            return

        try:
            if not self._key_name:
                raise ValueError('Attempting to update a sheet without a primary key')

            # Load the data from Google
            spreadsheet = self._database.load()
            worksheet = self._load_worksheet(spreadsheet)
            raw = self._load_values(worksheet)

            header = raw[0]
            rows = raw[1:]

            # Map the header keys to their column numbers - instead of A, B, C we use 0, 1, 2
            columns = {GoogleSheetDatabaseTable._header_to_key(h): i for i, h in enumerate(header)}

            # Index the rows by the value in the key column
            key_column = columns[self._key_name]
            row_number_by_key = {row[key_column]: i for i, row in enumerate(rows)}

            for key, update in data.items():
                row_number = row_number_by_key.get(key, None)
                if row_number is None:
                    continue

                # Map the updates to their column numbers
                updates_by_column = {columns[k]: v for k, v in update.items() if k in columns}
                # We add 1 to the row to account for the headers
                GoogleSheetDatabaseTable._update_row(worksheet, row_number + 1, updates_by_column)
        except Exception as e:
            logger.exception(e)

    @staticmethod
    def _insert_row(worksheet: gspread.Worksheet, data_by_column: dict[int, str]) -> None:
        worksheet.append_row(list(dict(sorted(data_by_column.items())).values()))

    @staticmethod
    def _update_row(worksheet: gspread.Worksheet, row_number: int, updates_by_column: dict[int, str]) -> None:
        for k, v in updates_by_column.items():
            worksheet.update_cell(row_number + 1, k + 1, v)  # Coordinates start at 1

    def _attach(self) -> Observable:
        cached_data = BehaviorSubject([])  # Start with an empty array until we get some data

        self._database.spreadsheet.pipe(  # Start with the spreadsheet
            op.map(self._load_worksheet),  # Load the sheet
            op.map(self._load_values),  # Load the actual data
            op.distinct_until_changed(),  # Only propagate when the sheet data changes, because it rarely changes
            op.map(self._parse),  # Parse the data
        ).subscribe(
            on_next=cached_data.on_next,   # Propagate the parsed data to the cache
            on_error=logger.exception,  # Log errors
        )

        return cached_data

    def _load_worksheet(self, spreadsheet: gspread.Spreadsheet) -> gspread.Worksheet:
        logger.debug(f"Loading worksheet {self._sheet_name}")
        return spreadsheet.worksheet(self._sheet_name)

    def _load_values(self, worksheet: gspread.Worksheet) -> list[list]:
        logger.debug(f"Loading worksheet values")
        return worksheet.get_values(major_dimension=self._dimension)

    def _parse(self, raw: list[list]) -> list[dict]:
        if len(raw) < 2:
            raise ValueError("The sheet does not contain the necessary data")

        header = raw[0]
        rows = raw[1:]

        keys = [GoogleSheetDatabaseTable._header_to_key(h) for h in header]
        return [dict(zip(keys, row)) for row in rows]

    @staticmethod
    def _header_to_key(text: str) -> str:
        text = re.sub(r"\([^)]*\)", '', text)  # Remove anything in parentheses
        text = re.sub(r"\s+", ' ', text)  # Squash multiple whitespaces together
        text = text.strip()  # Remove leading / trailing whitespace
        text = text.lower()  # Everything should be lowercase
        text = re.sub(r"[^a-z0-9_]", '_', text)  # Remove any characters except the ones used for variables

        return text
