import logging
from abc import ABC, abstractmethod
from typing import TypeVar, Generic
from reactivex import operators as op

import gspread
from gspread.utils import Dimension

from typing import TYPE_CHECKING

from readerwriterlock.rwlock import RWLockWrite

from integrations.google.sheets.contracts.index import Index

if TYPE_CHECKING:
    from integrations.google.sheets.contracts.database import Database

logger = logging.getLogger(__name__)

T = TypeVar("T")

# This should be the base class for Google Sheets tables
# The parse method must be implemented and provide a way to transform the raw table data into a list of models
class ReadableTable(ABC, Generic[T]):
    _dimension = Dimension.rows

    def __init__(self, database: 'Database', sheet_name: str):
        self._database = database
        self._sheet_name = sheet_name

        self._lock = RWLockWrite()

        self._database.spreadsheet.pipe(  # Start with the spreadsheet
            op.map(self._load_worksheet),  # Load the sheet
            op.map(self._load_values),  # Load the actual data
            op.distinct_until_changed(),  # Only propagate when the sheet data changes, because it rarely changes
            op.map(self._parse),  # Parse the data
        ).subscribe(
            on_next=self._reset_indexes,  # Propagate the parsed data to the indexes
            on_error=logger.exception,  # Log errors
        )

    def _load_worksheet(self, spreadsheet: gspread.Spreadsheet) -> gspread.Worksheet:
        logger.info(f"Loading worksheet {self._sheet_name}")
        return spreadsheet.worksheet(self._sheet_name)

    def _load_values(self, worksheet: gspread.Worksheet) -> list[list]:
        logger.debug(f"Loading worksheet values")
        return worksheet.get_values(major_dimension=self._dimension)

    def _get_lock(self) -> RWLockWrite:
        return self._lock

    def _get_indexes(self) -> dict[str, Index]:
        return {key: value for key, value in self.__dict__.items() if isinstance(value, Index)}

    def _reset_indexes(self, models: list[T]) -> None:
        with self._lock.gen_wlock():
            for index in self._get_indexes().values():
                index.reset(models)

    @abstractmethod
    def _parse(self, raw: list[list[str]]) -> list[T]:
        pass
