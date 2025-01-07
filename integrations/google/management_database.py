from integrations.google.api import GoogleApi
from integrations.google.sheet_database import GoogleSheetDatabase
from integrations.google.sheet_database_tasks_table import GoogleSheetDatabaseTasksTable


class ManagementDatabase(GoogleSheetDatabase):
    def __init__(self, spreadsheet_key: str, api_credentials: str = None, api: GoogleApi = None):
        super().__init__(spreadsheet_key, api_credentials, api)

        self._tasks = GoogleSheetDatabaseTasksTable(self, 'Shift Checklist')

        self.refresh()

    @property
    def tasks(self) -> GoogleSheetDatabaseTasksTable:
        return self._tasks
