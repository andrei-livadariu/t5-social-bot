from datetime import datetime

from data.models.user import User
from data.models.raffle_entry import RaffleEntry
from data.repositories.raffle import RaffleRepository
from data.repositories.user import UserRepository

from helpers.business_logic.points import Points

from integrations.loyverse.api import LoyverseApi


class Raffle:
    def __init__(self, loy: LoyverseApi, users: UserRepository, entries: RaffleRepository, title: str, description: str, end_date: datetime, ticket_price: Points, max_tickets: int):
        self._loy = loy
        self._users = users
        self._entries = entries

        self.title = title
        self.description = description
        self.end_date = end_date
        self.ticket_price = ticket_price
        self.max_tickets = max_tickets

    @property
    def is_active(self) -> bool:
        return datetime.now(self.end_date.tzinfo) < self.end_date

    @property
    def is_free_entry(self) -> bool:
        return self.ticket_price.is_zero

    def buy_ticket(self, user: User) -> None:
        self._loy.remove_points(user, self.ticket_price)
        self._entries.create(user)

    def get_all_entries(self) -> dict[User, set[RaffleEntry]]:
        matched_entries = [(self._users.get_by_full_name(full_name), entries) for full_name, entries in self._entries.list_by_user().items()]
        return {user: entries for user, entries in matched_entries if user}

    def get_entries(self, user: User) -> set[RaffleEntry]:
        return self._entries.get_by_user(user)

    def has_entries(self, user: User) -> bool:
        return len(self.get_entries(user)) > 0

    def can_enter(self, user: User) -> bool:
        return self.max_tickets == 0 or len(self.get_entries(user)) < self.max_tickets
