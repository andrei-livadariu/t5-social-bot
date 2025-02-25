from datetime import date, datetime
from typing import Union
from itertools import groupby

from readerwriterlock import rwlock

from data.repositories.event import EventRepository
from data.models.event import Event

from integrations.google.sheets.tables.events import EventsTable


class SheetEventRepository(EventRepository):
    def __init__(self, table: EventsTable):
        # The repository data can be read and refreshed from different threads,
        # so any data operation needs to be protected
        self.lock = rwlock.RWLockWrite()

        self.events: list[Event] = []
        self.events_by_date: dict[date, list[Event]] = {}

        table.data.subscribe(self._load)

    def get_all_events(self) -> list[Event]:
        with self.lock.gen_rlock():
            return self.events

    def get_events_on(self, on_date: Union[date, datetime]) -> list[Event]:
        real_date = on_date if type(on_date) is date else on_date.date()
        with self.lock.gen_rlock():
            return self.events_by_date.get(real_date, [])

    def _load(self, events: list[Event]) -> None:
        with self.lock.gen_wlock():
            self.events = events

            self.events.sort(key=lambda event: event.start_date)
            grouped_events = groupby(self.events, key=lambda event: event.start_date.date())
            self.events_by_date = {key: list(items) for key, items in grouped_events}
