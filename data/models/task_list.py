from dataclasses import dataclass, replace, field
from copy import deepcopy
from datetime import datetime, time, timedelta

from data.models.task import Task


@dataclass(frozen=True)
class TaskList:
    weekday: int
    group: str
    tasks: list[Task] = field(default_factory=list)

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def copy(self, **changes) -> 'TaskList':
        return replace(deepcopy(self), **changes)

    @property
    def id(self) -> str:
        return f"{self.weekday}.{self.group}"

    @property
    def start_time(self) -> time:
        return self.tasks[0].time

    def next_run(self, now: datetime) -> datetime:
        days_ahead = self.weekday - now.weekday()
        if days_ahead < 0:
            days_ahead += 7
        elif days_ahead == 0 and now.time() >= self.start_time:
            days_ahead += 7

        next_weekday = now.date() + timedelta(days=days_ahead)
        return datetime.combine(next_weekday, self.start_time, now.tzinfo)
