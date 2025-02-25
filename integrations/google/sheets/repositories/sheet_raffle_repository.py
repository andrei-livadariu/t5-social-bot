import pytz
import random

from itertools import groupby
from datetime import datetime

from readerwriterlock import rwlock

from data.repositories.raffle import RaffleRepository
from data.models.user import User
from data.models.raffle_entry import RaffleEntry

from integrations.google.sheets.tables.raffle_entries import RaffleEntriesTable

countries = [
    'Albania',
    'Austria',
    'Belgium',
    'Croatia',
    'Czech Republic',
    'Denmark',
    'England',
    'France',
    'Georgia',
    'Germany',
    'Hungary',
    'Italy',
    'Netherlands',
    'Poland',
    'Portugal',
    'Romania',
    'Scotland',
    'Serbia',
    'Slovakia',
    'Slovenia',
    'Spain',
    'Switzerland',
    'Turkey',
    'Ukraine',
]


class SheetRaffleRepository(RaffleRepository):
    def __init__(self, table: RaffleEntriesTable, timezone: pytz.timezone = None):
        self.timezone = timezone

        self.entries: list[RaffleEntry] = []
        self.entries_by_full_name: dict[str, list[RaffleEntry]] = {}

        # The repository data can be read and refreshed from different threads,
        # so any data operation needs to be protected
        self.lock = rwlock.RWLockWrite()

        self._table = table
        self._table.data.subscribe(self._load)

    def get_by_user(self, user: User) -> list[RaffleEntry]:
        with self.lock.gen_rlock():
            return self.entries_by_full_name.get(user.full_name, [])

    def list_by_user(self) -> dict[str, list[RaffleEntry]]:
        with self.lock.gen_rlock():
            return self.entries_by_full_name.copy()

    def create(self, user: User) -> RaffleEntry:
        with self.lock.gen_wlock():
            entry = RaffleEntry(
                full_name=user.full_name,
                created_at=datetime.now(tz=self.timezone),
                country=random.choice(countries)
            )

            self.entries.append(entry)
            if entry.full_name in self.entries_by_full_name:
                self.entries_by_full_name[entry.full_name].append(entry)
            else:
                self.entries_by_full_name[entry.full_name] = [entry]

            self._table.insert(entry)

            return entry

    def _load(self, entries: list[RaffleEntry]) -> None:
        with self.lock.gen_wlock():
            self.entries = entries

            sorted_entries = sorted(self.entries, key=lambda entry: entry.full_name)
            self.entries_by_full_name = {key: list(group) for key, group in groupby(sorted_entries, key=lambda entry: entry.full_name)}
