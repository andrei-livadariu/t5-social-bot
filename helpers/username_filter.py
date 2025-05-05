from typing import Optional

from telegram import Update
from telegram.ext.filters import UpdateFilter

# This class implements a telegram bot filter which ensures the message sender is in a set of specific usernames
class UsernameFilter(UpdateFilter):
    __slots__ = '_usernames'

    def __init__(self, usernames: set[str], name: Optional[str] = 'helpers.UsernameFilter', data_filter: bool = False):
        self._usernames = usernames
        super().__init__(name=name, data_filter=data_filter)

    def filter(self, update: Update) -> bool:
        return update.effective_user.username in self._usernames