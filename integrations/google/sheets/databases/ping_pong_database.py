import pytz

from integrations.google.api import GoogleApi
from integrations.google.sheets.contracts.database import Database
from integrations.google.sheets.tables.ping_pong_matches import PingPongMatchesTable
from integrations.google.sheets.tables.ping_pong_standings import PingPongStandingsTable


class PingPongDatabase(Database):
    def __init__(self, api: GoogleApi, spreadsheet_key: str, timezone: pytz.timezone):
        super().__init__(api, spreadsheet_key, timezone)

        self._standings = PingPongStandingsTable(self, 'Ladder')
        self._matches = PingPongMatchesTable(self, 'Matches')

        self.refresh()

    @property
    def standings(self) -> PingPongStandingsTable:
        return self._standings

    @property
    def matches(self) -> PingPongMatchesTable:
        return self._matches
