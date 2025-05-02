from dataclasses import dataclass, replace
from copy import deepcopy
from datetime import time


@dataclass(frozen=True)
class Task:
    time: time
    name: str
    is_done: bool = False

    def __eq__(self, other):
        return self.time == other.time and self.name == other.name

    def __hash__(self):
        return hash(f"{self.time.strftime('%H:%M')}.{self.name}")

    def copy(self, **changes) -> 'Task':
        return replace(deepcopy(self), **changes)

