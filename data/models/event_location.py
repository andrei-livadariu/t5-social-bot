from datetime import time, timedelta
from enum import Enum, unique


@unique
class EventLocation(Enum):
    OUTSIDE = 'outside'
    INSIDE = 'inside'
    DAYTIME = 'daytime'

    @property
    def is_main(self) -> bool:
        return self in {EventLocation.OUTSIDE, EventLocation.INSIDE}

    @property
    def default_start_time(self) -> time:
        return time(19, 0, 0, 0) if self.is_main else time(15, 0, 0, 0)

    @property
    def default_duration(self) -> timedelta:
        return timedelta(hours=3) if self.is_main else timedelta(hours=1)