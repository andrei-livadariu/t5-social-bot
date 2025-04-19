from abc import ABC, abstractmethod

from datetime import date

from data.models.user import User
from data.models.visits_entry import VisitsEntry


class VisitRepository(ABC):
    @abstractmethod
    def get_by_user(self, user: User) -> VisitsEntry:
        pass

    @abstractmethod
    def get_visitors_in_month(self, month: date) -> list[tuple[str, int]]:
        pass

    @abstractmethod
    def save(self, entry: VisitsEntry) -> None:
        pass

    @abstractmethod
    def save_all(self, entries: list[VisitsEntry]) -> None:
        pass