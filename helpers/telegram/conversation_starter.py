from typing import Callable

from ratelimit import limits, sleep_and_retry
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import Forbidden

from data.models.user import User
from data.repositories.user import UserRepository

# This class initiates conversations with users, taking into account their messaging preferences
class ConversationStarter:
    def __init__(self, bot: Bot, users: UserRepository):
        self._bot = bot
        self._users = users

    async def send(self, recipients: list[User], message: str | Callable[[User], str|dict], on_fail: Callable[[User], None]|None = None) -> None:
        message_callable = message if isinstance(message, Callable) else lambda u: message

        blocked_users = set()
        for user in recipients:
            if user.can_contact:
                processed_message = message_callable(user)
                processed_message = {'text': processed_message} if isinstance(processed_message, str) else processed_message

                successful = await self._send_user_message(user.telegram_id, processed_message)
                if not successful:
                    blocked_users.add(user)
            else:
                successful = False

            if not successful and on_fail:
                on_fail(user)

        if blocked_users:
            self._users.save_all([user.copy(telegram_blocked=True) for user in blocked_users])

    @sleep_and_retry
    @limits(calls=25, period=1)
    async def _send_user_message(self, telegram_id: int, processed_message: dict) -> bool:
        try:
            await self._bot.send_message(telegram_id, parse_mode=ParseMode.HTML, **processed_message)
            return True
        except Forbidden:
            return False
