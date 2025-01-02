from integrations.google.api import GoogleApi
from integrations.google.sheet_database import GoogleSheetDatabase
from integrations.google.sheet_database_events_table import GoogleSheetDatabaseEventsTable
from integrations.google.sheet_database_table import GoogleSheetDatabaseTable


class CommunityDatabase(GoogleSheetDatabase):
    def __init__(self, spreadsheet_key: str, api_credentials: str = None, api: GoogleApi = None):
        super().__init__(spreadsheet_key, api_credentials, api)

        self._users = GoogleSheetDatabaseTable(self, 'MEMBERS', 'full_name')
        self._events = GoogleSheetDatabaseEventsTable(self, 'EVENTS')
        self._raffle = GoogleSheetDatabaseTable(self, 'Raffle')

        self.refresh()

    @property
    def events(self) -> GoogleSheetDatabaseEventsTable:
        return self._events

    @property
    def users(self) -> GoogleSheetDatabaseTable:
        return self._users

    @property
    def raffle(self) -> GoogleSheetDatabaseTable:
        return self._raffle
