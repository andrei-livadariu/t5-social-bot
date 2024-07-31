import logging
from reactivex import Observable
from reactivex.subject import Subject

import gspread

from integrations.google.api import GoogleApi
from integrations.google.sheet_database_table import GoogleSheetDatabaseTable
from integrations.google.sheet_database_tasks_table import GoogleSheetDatabaseTasksTable

logger = logging.getLogger(__name__)


class GoogleSheetDatabase:
    def __init__(self, spreadsheet_key: str, api_credentials: str = None, api: GoogleApi = None):
        self.api = api if api else GoogleApi(api_credentials)
        self.spreadsheet_key = spreadsheet_key

        self._spreadsheet = Subject()
        self._users = GoogleSheetDatabaseTable(self, 'Community', 'full_name')
        self._events = GoogleSheetDatabaseTable(self, 'Events')
        self._raffle = GoogleSheetDatabaseTable(self, 'Raffle')
        self._tasks = GoogleSheetDatabaseTasksTable(self, 'Team Checklist')

        self.refresh()

    @property
    def spreadsheet(self) -> Observable:
        return self._spreadsheet

    @property
    def events(self) -> GoogleSheetDatabaseTable:
        return self._events

    @property
    def users(self) -> GoogleSheetDatabaseTable:
        return self._users

    @property
    def tasks(self) -> GoogleSheetDatabaseTasksTable:
        return self._tasks

    @property
    def raffle(self) -> GoogleSheetDatabaseTable:
        return self._raffle

    def refresh(self) -> None:
        logger.info('Refreshing Google Sheets data')
        try:
            self._spreadsheet.on_next(self.load())
        except Exception as e:
            self._spreadsheet.on_error(e)

    async def refresh_job(self, context) -> None:
        self.refresh()

    def load(self) -> gspread.Spreadsheet:
        logger.info(f"Loading spreadsheet {self.spreadsheet_key}")
        return self.api.get_spreadsheet(self.spreadsheet_key)
