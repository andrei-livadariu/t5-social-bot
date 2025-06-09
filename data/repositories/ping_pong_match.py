from abc import ABC, abstractmethod

from data.models.ping_pong_match import PingPongMatch


class PingPongMatchRepository(ABC):
    @abstractmethod
    def insert(self, model: PingPongMatch) -> None:
        pass

    @abstractmethod
    def insert_all(self, models: list[PingPongMatch]) -> None:
        pass

    @abstractmethod
    def get_streak(self, player_name: str) -> int:
        pass

    @abstractmethod
    def get_matches(self, player_name: str) -> list[PingPongMatch]:
        pass
