import re
from datetime import datetime, date, time
from itertools import zip_longest
from typing import Optional, Tuple, TYPE_CHECKING

from data.models.event import Event
from data.models.event_location import EventLocation
from data.repositories.event import EventRepository
from integrations.google.sheets.contracts.tables.readable_table import ReadableTable
from integrations.google.sheets.indexes.sorted_bucket_index import SortedBucketIndex

if TYPE_CHECKING:
    from integrations.google.sheets.contracts.database import Database


class EventsTable(
    ReadableTable[Event],
    EventRepository,
):
    def __init__(self, database: 'Database', sheet_name: str):
        super().__init__(database, sheet_name)

        self._by_date = SortedBucketIndex[Event, date](
            keys=lambda event: event.start_date.date(),
            sorter=lambda event: event.start_date,
            shared_lock=self._lock,
        )

    def get_events_on(self, on_date: date|datetime) -> list[Event]:
        real_date = on_date.date() if isinstance(on_date, datetime) else on_date
        return self._by_date.get(real_date)

    def _parse(self, raw: list[list[str]]) -> list[Event]:
        if len(raw) < 2:
            raise ValueError("The sheet does not contain the necessary data")

        keys = ['weekday', 'date', 'outside_name', 'outside_host', 'inside_name', 'inside_host', 'daytime_name', 'daytime_host']
        rows = raw[2:]

        merged_events = [dict(zip(keys, row)) for row in rows]
        split_events = [EventsTable._split_event_row(event_row) for event_row in merged_events]

        flat = [row for chunk in split_events for row in chunk]

        deserialized = [self._deserialize(row) for row in flat]
        return [model for model in deserialized if model]

    def _deserialize(self, row: dict[str, str]) -> Optional[Event]:
        # The event name and start date are required
        name = row.get('name', '').strip()
        if not name:
            return None

        location = EventsTable._parse_event_location(row.get('location', ''))
        if not location:
            return None

        start_date = EventsTable._parse_event_date(row.get('date', '').strip())
        if not start_date:
            return None

        (name, start_time) = EventsTable._parse_event_time(name)
        if not name:
            return None

        if not start_time:
            start_time = location.default_start_time

        start_datetime = self._database.timezone.localize(datetime.combine(start_date, start_time))

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
    def _split_event_row(event_row: dict[str, str]) -> list[dict[str, str]]:
        events = []

        for location in EventLocation:
            names = EventsTable._parse_multiline_string(event_row.get(f"{location.value}_name", ''))
            if not names:
                continue

            hosts = EventsTable._parse_multiline_string(event_row.get(f"{location.value}_host", ''))

            for name, host in zip_longest(names, hosts, fillvalue=''):
                events.append({
                    'weekday': event_row.get('weekday'),
                    'date': event_row.get('date'),
                    'location': location.value,
                    'name': name,
                    'host': host,
                })

        return events

    @staticmethod
    def _parse_multiline_string(text: str) -> list[str]:
        lines = text.split("\n")
        lines = [line.strip() for line in lines]
        lines = [line for line in lines if line]
        return lines

    @staticmethod
    def _parse_event_date(date_string: str) -> Optional[datetime]:
        try:
            return datetime.strptime(date_string, '%Y-%m-%d')
        except ValueError:
            return None

    @staticmethod
    def _parse_event_time(text: str) -> (str, Optional[time]):
        return (
            EventsTable._try_parse_time(text, r"@? *([0-9]+:[0-9]+ *(?:am|pm)) *-? *", '%I:%M%p')
            or EventsTable._try_parse_time(text, r"@? *([0-9]+\.[0-9]+ *(?:am|pm)) *-? *", '%I.%M%p')
            or EventsTable._try_parse_time(text, r"@? *([0-9]+ *(?:am|pm)) *-? *", '%I%p')
            or EventsTable._try_parse_time(text, r"@? *([0-9]+:[0-9]+) *-? *", '%H:%M')
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
