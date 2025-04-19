import logging
from datetime import datetime, date, timedelta
from itertools import groupby
from typing import Optional

from data.models.user import User
from data.models.visits_entry import VisitsEntry
from data.repositories.visit import VisitRepository

from modules.base_module import BaseModule
from helpers.points import Points

logger = logging.getLogger(__name__)

Checkpoints = dict[int, Points]
ReachedCheckpoints = dict[date, Checkpoints]
RawVisit = tuple[User, datetime]


class VisitCalculator(BaseModule):
    def __init__(self, checkpoints: Checkpoints, visits: VisitRepository):
        # Make sure the checkpoints are sorted
        self._checkpoints = dict(sorted(checkpoints.items()))
        self._visits = visits

    def get_last_visit(self, user: User) -> Optional[datetime]:
        entry = self._visits.get_by_user(user)
        return entry.last_visit

    def get_visits_in_month(self, user: User, month: datetime) -> int:
        entry = self._visits.get_by_user(user)
        return entry.visits_by_month.get(VisitCalculator.month(month), 0)

    def get_visitors_in_month(self, month: datetime) -> list[tuple[str, int]]:
        return self._visits.get_visitors_in_month(VisitCalculator.month(month))

    def get_next_checkpoint(self, visits: int) -> Optional[tuple[int, Points]]:
        for checkpoint, points in self._checkpoints.items():
            if checkpoint > visits:
                return checkpoint, points
        return None

    def add_visits(self, raw_visits: list[RawVisit]) -> dict[User, ReachedCheckpoints]:
        if not raw_visits:
            return {}

        # Group the visits by User
        sorted_visits = sorted(raw_visits, key=lambda visit: visit[0].full_name)
        grouped_visits = groupby(sorted_visits, key=lambda visit: visit[0])
        user_visits = {user: [visit[1] for visit in visits] for user, visits in grouped_visits}

        # Add the visits to each user
        raw_updates = [(user, *self._add_user_visits(user, visits)) for user, visits in user_visits.items()]

        # Save the entries to the repository
        self._visits.save_all([entry for user, entry, checkpoints in raw_updates])

        # Return the checkpoints, but not the entries
        return {user: checkpoints for user, entry, checkpoints in raw_updates if checkpoints}

    def _add_user_visits(self, user: User, raw_visits: list[datetime]) -> tuple[VisitsEntry, ReachedCheckpoints]:
        entry = self._visits.get_by_user(user)
        clean_visits = VisitCalculator._clean_visits(raw_visits, entry.last_visit)
        if not clean_visits:
            return entry, {}

        # Grouping the visits by month helps us deal with various edge cases
        # 1. We have crossed from one month into another and we need to start the count again from 0 (most common)
        # 2. The visits span more than 1 month (extremely rare edge case)
        #    E.g. This is possible if we last checked on November 30th at 12:00 and it's now December 1st at 12:00
        #    Some visits could have come in during this 24 hour interval and they would be in different months
        # 3. The bot has been shut down for a while and we are dealing with an incoming flux of historical data
        #    spanning many months, and we need to grant partial points for one month and full points for the rest
        visits_by_month = VisitCalculator._count_visits_by_month(clean_visits)
        updated_entry = entry.add_visits(visits_by_month).copy(last_visit=clean_visits[-1])

        month_checkpoints = {}
        for month in visits_by_month.keys():
            checkpoints = self._reach_checkpoints(
                entry.visits_by_month.get(month, 0),
                updated_entry.visits_by_month.get(month, 0)
            )
            if checkpoints:
                month_checkpoints[month] = checkpoints

        return updated_entry, month_checkpoints

    @staticmethod
    def _clean_visits(visits: list[datetime], last_visit: Optional[datetime]) -> list[datetime]:
        # Clean up the list of raw visits by:
        # - Removing old visits
        # - Sorting them by date
        # - Removing duplicate visits

        # Keep only visits that are newer than the user's last visit
        new_visits = [visit for visit in visits if visit > last_visit] if last_visit else visits
        if not new_visits:
            return []

        new_visits = sorted(new_visits)

        # Keep only visits that happen more than 8 hours after each other
        distance = timedelta(hours=8)

        distinct_visits = []
        next_allowed_visit = last_visit + distance if last_visit else new_visits[0]
        for visit in new_visits:
            if visit >= next_allowed_visit:
                distinct_visits.append(visit)
                next_allowed_visit = visit + distance

        return distinct_visits

    @staticmethod
    def _count_visits_by_month(visits: list[datetime]) -> dict[date, int]:
        # Group by month and count the visits
        visits_grouped_by_month = groupby(sorted(visits), key=lambda visit: VisitCalculator.month(visit))
        return {month: len(list(visits)) for month, visits in visits_grouped_by_month}

    def _reach_checkpoints(self, start: int, end: int) -> Checkpoints:
        return {visits: points for visits, points in self._checkpoints.items() if start < visits <= end}

    @staticmethod
    def month(value: datetime) -> date:
        return value.date().replace(day=1)
