from dataclasses import dataclass, replace, field
from datetime import datetime, timedelta
from copy import deepcopy

from data.models.event_location import EventLocation


@dataclass(frozen=True)
class Event:
    name: str
    location: EventLocation
    start_date: datetime
    end_date: datetime = field(default=None)
    host: str = ''

    def __post_init__(self):
        if not self.end_date:
            # Workaround to initialize a field in a frozen class
            super().__setattr__('end_date', self.start_date + timedelta(hours=1))

    def copy(self, **changes) -> 'Event':
        return replace(deepcopy(self), **changes)