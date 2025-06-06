import logging
import pytz
from typing import Optional, Callable
from datetime import datetime, date, time, timedelta

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, filters

from data.models.user import User
from data.repositories.user import UserRepository
from helpers.telegram.conversation_starter import ConversationStarter
from helpers.telegram.points_claim import PointsClaim

from modules.base_module import BaseModule
from helpers.business_logic.access_checker import AccessChecker
from helpers.business_logic.points import Points
from helpers.telegram.chat_target import ChatTarget

from messages import birthday_congratulations

from integrations.loyverse.api import LoyverseApi

logger = logging.getLogger(__name__)

BIRTHDAY_MESSAGE: Callable[[User, Points], str] = lambda user, points: f"""La Mulți Ani {user.first_name} 🎉

{birthday_congratulations.random}

Enjoy {points} Loyalty Points from T5 🎁
"""

class BirthdayModule(BaseModule):
    def __init__(self, loy: LoyverseApi, ac: AccessChecker, users: UserRepository, admin_chats: set[ChatTarget] = None, points_to_award: Points = Points(5), timezone: Optional[pytz.timezone] = None):
        self.loy: LoyverseApi = loy
        self.ac: AccessChecker = ac
        self.users: UserRepository = users
        self.admin_chats: set[ChatTarget] = (admin_chats or set()).copy()
        self.points_to_award: Points = points_to_award
        self.timezone: Optional[pytz.timezone] = timezone

    def install(self, application: Application) -> None:
        application.add_handler(CommandHandler('force_announce_birthdays', self._force_announce_birthdays, filters.ChatType.PRIVATE & self.ac.filter_master))

        daily_time = time(0, 0, 0, 0, self.timezone)
        application.job_queue.run_daily(self._process_birthdays, daily_time)
        application.job_queue.run_daily(self._announce_advance_birthdays, daily_time, days=(0,))

        logger.info("Birthday module installed")

    async def _force_announce_birthdays(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._process_birthdays(context)

    async def _process_birthdays(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        current_date = datetime.now(self.timezone)
        logger.info(f"Processing birthdays for {current_date}.")

        users = self.users.get_by_birthday(current_date)
        if not users:
            logger.info("No users have birthdays today")
            return

        logger.info(f"The following users have birthdays today: {users}")

        convo = ConversationStarter(context.bot, self.users)
        await convo.send(
            recipients=list(users),
            message=lambda user: {
                'text': BIRTHDAY_MESSAGE(user, self.points_to_award),
                'reply_markup': PointsClaim(self.points_to_award).keyboard(),
            },
            # Award the points directly if we can't send the message for the user to confirm
            on_fail=lambda user: self.loy.add_points(user, self.points_to_award),
        )

    async def _announce_advance_birthdays(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.admin_chats:
            return

        today = datetime.now(self.timezone).date()
        days_to_end_of_week = 6 - today.weekday()
        next_monday = today + timedelta(days=days_to_end_of_week + 1)
        monday_two_weeks = next_monday + timedelta(days=7)

        message_parts = []
        this_week = self._get_birthdays_in_range(today, range(1, days_to_end_of_week + 1))
        if this_week:
            message_parts.append(BirthdayModule._format_birthday_list("This week", this_week))

        next_week = self._get_birthdays_in_range(next_monday, range(7))
        if next_week:
            message_parts.append(BirthdayModule._format_birthday_list("Next week", next_week))

        two_weeks = self._get_birthdays_in_range(monday_two_weeks, range(7))
        if two_weeks:
            message_parts.append(BirthdayModule._format_birthday_list("In two weeks", two_weeks))

        if message_parts:
            announcement = "\n\n".join(["<b>Upcoming birthdays:</b>"] + message_parts)
        else:
            announcement = "Unlikely as it is, there are no upcoming birthdays in the next couple of weeks."

        for target in self.admin_chats:
            await context.bot.send_message(target.chat_id, announcement, parse_mode=ParseMode.HTML, message_thread_id=target.thread_id)

    def _get_birthdays_in_range(self, start: date, span: range) -> dict[date, set[User]]:
        birthdays = {day: self.users.get_by_birthday(day) for day in (start + timedelta(days=n) for n in span)}
        return {day: users for day, users in birthdays.items() if users}

    @staticmethod
    def _format_birthday_list(heading: str, birthdays: dict[date, set[User]]) -> str:
        message_parts = [f"<b>{heading}:</b>"]
        for day, users in birthdays.items():
            users_text = BirthdayModule._enumerate([user.friendly_name for user in users])
            message_parts.append(f"{day.strftime('%A, %d %B')} - {users_text}")

        return "\n".join(message_parts)

    @staticmethod
    def _enumerate(lst: list[str]) -> str:
        return (', '.join(lst[:-1]) + ' and ' + lst[-1]) if len(lst) > 1 else lst[0]
