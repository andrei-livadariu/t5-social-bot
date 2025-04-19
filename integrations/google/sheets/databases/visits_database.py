import pytz

from integrations.google.api import GoogleApi
from integrations.google.sheets.contracts.database import Database
from integrations.google.sheets.tables.visits import VisitsTable


class VisitsDatabase(Database):
    def __init__(self, api: GoogleApi, spreadsheet_key: str, timezone: pytz.timezone):
        super().__init__(api, spreadsheet_key, timezone)

        self._visits = VisitsTable(self, 'VISITS')

        self.refresh()

    @property
    def visits(self) -> VisitsTable:
        return self._visits
