from abc import ABC, abstractmethod

from data.models.ping_pong_standing import PingPongStanding


class PingPongStandingRepository(ABC):
    @abstractmethod
    def save(self, model: PingPongStanding) -> None:
        pass

    @abstractmethod
    def save_all(self, models: list[PingPongStanding]) -> None:
        pass

    @abstractmethod
    def get_all_standings(self) -> list[PingPongStanding]:
        pass

    @abstractmethod
    def get_standing(self, player_name: str) -> PingPongStanding:
        pass
