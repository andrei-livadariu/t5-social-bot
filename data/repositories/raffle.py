from data.models.user import User
from data.models.raffle_entry import RaffleEntry


class RaffleRepository:
    def get_by_user(self, user: User) -> set[RaffleEntry]:
        pass

    def list_by_user(self) -> dict[str, set[RaffleEntry]]:
        pass

    def create(self, user: User) -> RaffleEntry:
        pass
