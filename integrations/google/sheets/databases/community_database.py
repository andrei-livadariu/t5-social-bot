import pytz

from integrations.google.api import GoogleApi
from integrations.google.sheets.contracts.database import Database
from integrations.google.sheets.tables.events import EventsTable
from integrations.google.sheets.tables.users import UsersTable


class CommunityDatabase(Database):
    def __init__(self, api: GoogleApi, spreadsheet_key: str, timezone: pytz.timezone):
        super().__init__(api, spreadsheet_key, timezone)

        self._users = UsersTable(self, 'MEMBERS')
        self._events = EventsTable(self, 'EVENTS')

        self.refresh()

    @property
    def users(self) -> UsersTable:
        return self._users

    @property
    def events(self) -> EventsTable:
        return self._events
