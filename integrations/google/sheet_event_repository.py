import pytz
import re

from datetime import date, datetime, time
from typing import Union, Optional, Tuple
from itertools import groupby

from readerwriterlock import rwlock

from data.models.event_location import EventLocation
from data.repositories.event import EventRepository
from data.models.event import Event

from integrations.google.handle import Handle
from integrations.google.sheet_database_table import GoogleSheetDatabaseTable

EventHandle = Handle[Event]


class GoogleSheetEventRepository(EventRepository):
    def __init__(self, table: GoogleSheetDatabaseTable, timezone: pytz.timezone = None):
        self.timezone = timezone

        # The repository data can be read and refreshed from different threads,
        # so any data operation needs to be protected
        self.lock = rwlock.RWLockWrite()

        self.events: list[EventHandle] = []
        self.events_by_date: dict[date, list[EventHandle]] = {}

        table.data.subscribe(self._load)

    def get_all_events(self) -> list[Event]:
        with self.lock.gen_rlock():
            return EventHandle.unwrap_list(self.events)

    def get_events_on(self, on_date: Union[date, datetime]) -> list[Event]:
        real_date = on_date if type(on_date) is date else on_date.date()
        with self.lock.gen_rlock():
            return EventHandle.unwrap_list(self.events_by_date.get(real_date, []))

    def _load(self, raw_data: list) -> None:
        with self.lock.gen_wlock():
            raw_events = [self._from_row(row) for row in raw_data]
            self.events = [EventHandle(event) for event in raw_events if event]

            self.events.sort(key=lambda handle: handle.inner.start_date)
            grouped_events = groupby(self.events, key=lambda handle: handle.inner.start_date.date())
            self.events_by_date = {key: list(items) for key, items in grouped_events}

    def _from_row(self, row: dict[str, str]) -> Optional[Event]:
        # The event name and start date are required
        name = row.get('name', '').strip()
        if not name:
            return None

        location = GoogleSheetEventRepository._parse_event_location(row.get('location', ''))
        if not location:
            return None

        start_date = GoogleSheetEventRepository._parse_event_date(row.get('date', '').strip())
        if not start_date:
            return None

        (name, start_time) = GoogleSheetEventRepository._parse_event_time(name)
        if not name:
            return None

        if not start_time:
            start_time = location.default_start_time

        start_datetime = self.timezone.localize(datetime.combine(start_date, start_time))

        return Event(
            name=name,
            location=location,
            start_date=start_datetime,
            end_date=start_datetime + location.default_duration,
            host=row.get('host', '').strip(),
        )

    @staticmethod
    def _parse_event_location(event_location_string: str) -> Optional[EventLocation]:
        try:
            return EventLocation(event_location_string.strip().lower())
        except ValueError:
            return None

    @staticmethod
    def _parse_event_date(date_string: str) -> Optional[datetime]:
        try:
            return datetime.strptime(date_string, '%Y-%m-%d')
        except ValueError:
            return None

    @staticmethod
    def _parse_event_time(text: str) -> (str, Optional[time]):
        return (
            GoogleSheetEventRepository._try_parse_time(text, "([0-9]+:[0-9]+ *(?:am|pm)) *-? *", '%I:%M%p')
            or GoogleSheetEventRepository._try_parse_time(text, "([0-9]+ *(?:am|pm)) *-? *", '%I%p')
            or GoogleSheetEventRepository._try_parse_time(text, "([0-9]+:[0-9]+) *-? *", '%H:%M')
            or (text, None)
        )

    @staticmethod
    def _try_parse_time(text: str, pattern: str, time_format: str) -> Optional[Tuple[str, time]]:
        regex = re.compile(pattern, re.IGNORECASE)
        match = regex.search(text)
        if not match:
            return None

        time_string = match.group(1).replace(' ', '').lower()
        return regex.sub('', text), datetime.strptime(time_string, time_format).time()
