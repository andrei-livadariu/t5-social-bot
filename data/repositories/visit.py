from abc import ABC, abstractmethod

from data.models.user import User
from data.models.visits_entry import VisitsEntry


class VisitRepository(ABC):
    @abstractmethod
    def get_by_user(self, user: User) -> VisitsEntry:
        pass

    @abstractmethod
    def save(self, entry: VisitsEntry) -> None:
        pass

    @abstractmethod
    def save_all(self, entries: list[VisitsEntry]) -> None:
        pass