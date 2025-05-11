import logging
import re
import gspread
from abc import abstractmethod
from typing import TypeVar, Optional

from gspread.utils import ValueInputOption

from integrations.google.sheets.contracts.tables.readable_table import ReadableTable

logger = logging.getLogger(__name__)

T = TypeVar("T")

# This is a standard table format, with the top row being the header and the following rows being the data
# After extending this class you will have to implement the deserialization method
# Also, this class implements some of the methods necessary for Insertable and Updatable
class ColRowTable(ReadableTable[T]):
    _append_to_top = False

    def _parse(self, raw: list[list[str]]) -> list[T]:
        if not raw:
            raise ValueError("The sheet does not contain the necessary data")

        header = raw[0]
        rows = raw[1:]

        keys = [ColRowTable._header_to_key(h) for h in header]
        serialized = [dict(zip(keys, row)) for row in rows]
        deserialized = [self._deserialize(row) for row in serialized]
        return [model for model in deserialized if model]

    def _insert_rows(self, rows: list[dict[str,str]]) -> None:
        if not rows:
            return

        try:
            # Load the data from Google
            spreadsheet = self._database.load()
            worksheet = self._load_worksheet(spreadsheet)
            raw = self._load_values(worksheet)

            header = raw[0]
            columns = self._resolve_columns(worksheet, header, rows)

            to_append = []

            for row in rows:
                # Map the row values to their column numbers
                values_by_column = {columns[k]: v for k, v in row.items() if k in columns}

                # Flatten the values, filling any gaps with blank strings
                flat_values = ColRowTable._fill_gaps(values_by_column)

                to_append.append(flat_values)

            if to_append:
                if self._append_to_top:
                    to_append.reverse()
                    worksheet.insert_rows(to_append, row=2, value_input_option=ValueInputOption.user_entered)
                else:
                    worksheet.append_rows(to_append, value_input_option=ValueInputOption.user_entered)
        except Exception as e:
            logger.exception(e)

    @staticmethod
    def _fill_gaps(data_by_column: dict[int, str], filler: str = '') -> list[str]:
        sorted_data = dict(sorted(data_by_column.items()))

        result = []
        last_index = -1
        for key, value in sorted_data.items():
            delta = key - last_index - 1
            if delta > 0:
                for i in range(delta):
                    result.append(filler)
            result.append(value)
            last_index = key

        return result

    def _update_rows(self, rows: dict[str, dict[str,str]], key_name: str) -> None:
        if not rows:
            return

        try:
            # Load the data from Google
            spreadsheet = self._database.load()
            worksheet = self._load_worksheet(spreadsheet)
            raw = self._load_values(worksheet)

            header = raw[0]
            existing_rows = raw[1:]
            columns = self._resolve_columns(worksheet, header, list(rows.values()))

            if key_name not in columns:
                raise ValueError('Attempting to update a sheet using an invalid key')

            # Index the rows by the value in the key column
            key_column = columns[key_name]
            existing_row_number_by_key = {row[key_column]: i for i, row in enumerate(existing_rows)}

            to_update = []
            for key, row in rows.items():
                row_number = existing_row_number_by_key.get(key, None)
                if row_number is None:
                    continue

                # Map the row values to their column numbers
                values_by_column = {columns[k]: v for k, v in row.items() if k in columns}

                for k, v in values_by_column.items():
                    to_update.append(gspread.Cell(row_number + 1 + 1, k + 1, v)) # Coordinates start at 1 and we also have 1 row for the headers

            if to_update:
                worksheet.update_cells(to_update, value_input_option=ValueInputOption.user_entered)
        except Exception as e:
            logger.exception(e)

    def _resolve_columns(self, worksheet: gspread.Worksheet, header: list[str], changed_rows: list[dict[str,str]]) -> dict[str, int]:
        # Map the header keys to their column numbers - instead of A, B, C we use 0, 1, 2
        return {ColRowTable._header_to_key(h): i for i, h in enumerate(header)}

    @staticmethod
    def _header_to_key(text: str) -> str:
        text = re.sub(r"\([^)]*\)", '', text)  # Remove anything in parentheses
        text = re.sub(r"\s+", ' ', text)  # Squash multiple whitespaces together
        text = text.strip()  # Remove leading / trailing whitespace
        text = text.lower()  # Everything should be lowercase
        text = re.sub(r"[^a-z0-9_]", '_', text)  # Remove any characters except the ones used for variables

        return text

    @abstractmethod
    def _deserialize(self, row: dict[str, str]) -> Optional[T]:
        pass
