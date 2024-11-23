from itertools import zip_longest

from data.models.event_location import EventLocation
from integrations.google.sheet_database_table import GoogleSheetDatabaseTable


class GoogleSheetDatabaseEventsTable(GoogleSheetDatabaseTable):
    def _parse(self, raw: list[list]) -> list[dict]:
        if len(raw) < 2:
            raise ValueError("The sheet does not contain the necessary data")

        keys = ['weekday', 'date', 'outside_name', 'outside_host', 'inside_name', 'inside_host', 'daytime_name', 'daytime_host']
        rows = raw[2:]

        merged_events = [dict(zip(keys, row)) for row in rows]
        split_events = [GoogleSheetDatabaseEventsTable._split_event_row(event_row) for event_row in merged_events]

        flat = [row for chunk in split_events for row in chunk]
        return flat

    @staticmethod
    def _split_event_row(event_row: dict[str, str]) -> list[dict[str, str]]:
        events = []

        for location in EventLocation:
            names = GoogleSheetDatabaseEventsTable._parse_multiline_string(event_row.get(f"{location.value}_name", ''))
            if not names:
                continue

            hosts = GoogleSheetDatabaseEventsTable._parse_multiline_string(event_row.get(f"{location.value}_host", ''))

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
