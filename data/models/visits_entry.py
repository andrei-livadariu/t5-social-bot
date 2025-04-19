from dataclasses import dataclass, replace, field
from copy import deepcopy
from datetime import datetime, date
from typing import Optional


@dataclass(frozen=True)
class VisitsEntry:
    full_name: str
    last_visit: Optional[datetime] = None
    visits_by_month: dict[date, int] = field(default_factory=dict)

    def add_visits(self, visits_by_month: dict[date, int] = None) -> 'VisitsEntry':
        combined_visits = {month: visits_by_month.get(month, 0) + self.visits_by_month.get(month, 0) for month in set(visits_by_month) | set(self.visits_by_month)}

        return self.copy(
            visits_by_month=combined_visits
        )

    def copy(self, **changes) -> 'VisitsEntry':
        return replace(deepcopy(self), **changes)