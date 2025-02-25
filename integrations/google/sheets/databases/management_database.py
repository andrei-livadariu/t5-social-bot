import pytz

from integrations.google.api import GoogleApi
from integrations.google.sheets.contracts.database import Database
from integrations.google.sheets.tables.tasks import TasksTable


class ManagementDatabase(Database):
    def __init__(self, api: GoogleApi, spreadsheet_key: str, timezone: pytz.timezone):
        super().__init__(api, spreadsheet_key, timezone)

        self._tasks = TasksTable(self, 'Shift Checklist')

        self.refresh()

    @property
    def tasks(self) -> TasksTable:
        return self._tasks
