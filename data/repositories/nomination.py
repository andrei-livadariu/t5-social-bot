from abc import ABC, abstractmethod
from datetime import datetime

from data.models.nomination import Nomination
from data.models.user import User


class NominationRepository(ABC):
    @abstractmethod
    def insert(self, model: Nomination) -> None:
        pass

    @abstractmethod
    def insert_all(self, models: list[Nomination]) -> None:
        pass

    @abstractmethod
    def has_voted(self, user: User, since: datetime) -> bool:
        pass
