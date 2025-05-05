from data.models.user import User
from helpers.username_filter import UsernameFilter


class AccessChecker:
    def __init__(self, masters: set, point_masters: set):
        self._masters = masters
        self._point_masters = point_masters

        self.filter_master = UsernameFilter(self._masters)

    def is_master(self, username: str) -> bool:
        return username in self._masters

    def can_donate_for_free(self, user: User) -> bool:
        return user.telegram_username in self._point_masters
