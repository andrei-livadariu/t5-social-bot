import logging
from math import floor

import pytz
from datetime import datetime, timedelta, date, time

from telegram import Update, InlineKeyboardButton
from telegram.constants import ChatType, ParseMode
from telegram.ext import Application, ContextTypes, CommandHandler, CallbackQueryHandler

from data.models.user import User
from data.repositories.user import UserRepository
from helpers.telegram.chat_target import ChatTarget
from helpers.telegram.conversation_starter import ConversationStarter
from helpers.telegram.points_claim import PointsClaim

from modules.base_module import BaseModule
from helpers.business_logic.points import Points
from helpers.business_logic.visit_calculator import VisitCalculator, ReachedCheckpoints
from helpers.telegram.exceptions import UserFriendlyError, MissingUsernameError, UserNotFoundError

from integrations.loyverse.api import LoyverseApi

from messages import visits_checkpoints

logger = logging.getLogger(__name__)


class VisitsModule(BaseModule):
    def __init__(self, loy: LoyverseApi, users: UserRepository, vc: VisitCalculator, timezone: pytz.timezone = None, admin_chats: set[ChatTarget] = None):
        self.loy = loy
        self.users = users
        self.timezone = timezone
        self.vc = vc
        self._admin_chats = admin_chats

        # We start checking for visits from the first day of the current month
        self.last_check = datetime.now(self.timezone).replace(day=1, hour=0, minute=0, second=0)

    def install(self, application: Application) -> None:
        application.add_handlers([
            CommandHandler("visits", self._status),
            CallbackQueryHandler(self._status, pattern="^visits/status"),
        ])

        application.job_queue.run_once(callback=self._update_visits, when=0)
        application.job_queue.run_repeating(callback=self._update_visits, interval=60 * 5)

        monthly_time = time(0, 5, 0, 0, self.timezone) # Midnight plus a few minutes to leave room for other messages
        application.job_queue.run_monthly(self._monthly_announcements, when=monthly_time, day=1)

        logger.info(f"Visits module installed")

    def get_menu_buttons(self) -> list[list[InlineKeyboardButton]]:
        return [
            [InlineKeyboardButton('Check Your Visits', callback_data='visits/status')],
        ]

    async def _status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            user = self._validate_user(update)

            right_now = datetime.now(self.timezone)
            last_visit = self.vc.get_last_visit(user)
            visits_this_month = self.vc.get_visits_in_month(user, right_now)

            reply_parts = []
            if visits_this_month:
                reply_parts.append(f"You visited T5 {visits_this_month} times this month!")
            else:
                reply_parts.append(f"I haven't seen you at T5 at all this month! Or maybe you're there right now for the first time?")

            if last_visit:
                if last_visit < right_now - timedelta(days=365):
                    date_format = '%d %B %Y'
                elif last_visit < right_now - timedelta(days=7):
                    date_format = '%d %B'
                else:
                    date_format = '%A, %d %B'
                reply_parts.append(f"The last time I saw you there was on {last_visit.strftime(date_format)}.")

            if VisitsModule._can_earn_points(user):
                next_checkpoint = self.vc.get_next_checkpoint(visits_this_month)
                if next_checkpoint:
                    visits_until_checkpoint = next_checkpoint[0] - visits_this_month
                    more = 'more ' if visits_this_month else ''
                    reply_parts.append(f"If you visit {visits_until_checkpoint} {more}times, you will be rewarded with {next_checkpoint[1]} points!")

            reply_parts.append(f"Please remember to pay your tab at the bar so I can tell you've been around.")

            reply = "\n\n".join(reply_parts)
        except UserFriendlyError as e:
            reply = str(e)
        except Exception as e:
            logger.exception(e)
            reply = f"BeeDeeBeeBoop 🤖 Error : {e}"

        if update.effective_chat.type != ChatType.PRIVATE:
            reply += "\n\n" + 'You can also <a href="https://t.me/T5socialBot?start=help">talk to me directly</a> to check your visits!'

        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(reply, disable_web_page_preview=True)
        else:
            await update.message.reply_html(reply, disable_web_page_preview=True)

    async def _monthly_announcements(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._admin_chats:
            return

        first_day_of_this_month = datetime.now(self.timezone).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day_of_previous_month = first_day_of_this_month - timedelta(days=1)
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

        await self._announce_top_visits(first_day_of_previous_month, context)
        await self._announce_top_spenders(first_day_of_previous_month, first_day_of_this_month, context)

    async def _announce_top_visits(self, month: datetime, context: ContextTypes.DEFAULT_TYPE) -> None:
        raw_visitors = self.vc.get_visitors_in_month(month)
        linked_visitors = [(self.users.get_by_full_name(full_name), visits) for full_name, visits in raw_visitors]
        visitors = [(user, visits) for user, visits in linked_visitors if user]

        if visitors:
            sorted_visitors = sorted(visitors, key=lambda tup: tup[1], reverse=True)
            top_10 = sorted_visitors[:10]
            lines = [f"{i + 1}. {user.full_name} - {visits} visits" for i, (user, visits) in enumerate(top_10)]

            announcement = "\n\n".join([
                f"<b>Here are the top 10 visitors in the month of {month.strftime('%B')}:</b>",
                "\n".join(lines),
            ])
        else:
            announcement = "Unlikely as it is, no community members visited this month."

        for target in self._admin_chats:
            await context.bot.send_message(target.chat_id, announcement, parse_mode=ParseMode.HTML, message_thread_id=target.thread_id)

    async def _announce_top_spenders(self, start: datetime, end: datetime, context: ContextTypes.DEFAULT_TYPE) -> None:
        spending = self.loy.load_spending(start, end)
        filtered_spending = {user: spending for user, spending in spending.items() if not user.role.is_staff}

        if filtered_spending:
            sorted_spending = sorted(filtered_spending.items(), key=lambda entry: entry[1], reverse=True)
            top_10 = sorted_spending[:10]
            lines = [f"{i + 1}. {user.full_name} - {floor(amount)} lei" for i, (user, amount) in enumerate(top_10)]

            announcement = "\n\n".join([
                f"<b>Here are the top 10 spenders in the month of {start.strftime('%B')}:</b>",
                "\n".join(lines),
            ])
        else:
            announcement = "Unlikely as it is, no community members spent any money this month."

        for target in self._admin_chats:
            await context.bot.send_message(target.chat_id, announcement, parse_mode=ParseMode.HTML, message_thread_id=target.thread_id)

    async def _update_visits(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        # This function may take several seconds to run, so it's important that we sample the time at the start
        right_now = datetime.now(self.timezone)

        # Load fresh visits that came in since the last time we checked
        visits = self.loy.load_visits(self.last_check)

        # Add the visits to the users
        updates = self.vc.add_visits(visits)

        # Send messages to users about the points they received
        await self._send_messages(updates, right_now, context)

        # Remember when we last retrieved new information
        self.last_check = right_now

    async def _send_messages(self, updates: dict[User, ReachedCheckpoints], right_now: datetime, context: ContextTypes.DEFAULT_TYPE):
        updates_with_points = {user: points for user, points in updates.items() if points and VisitsModule._can_earn_points(user)}

        prepared_announcements: dict[date, dict[User, tuple[Points, str]]] = {}

        for user, month_checkpoints in updates_with_points.items():
            for month, checkpoints in month_checkpoints.items():
                total_points = sum(checkpoints.values(), start=Points(0))
                a_total_of = 'a total of ' if len(checkpoints) > 1 else ''
                print(f"{user.full_name} receives {a_total_of}{total_points} point{total_points.plural} for visits in {month.strftime('%B')}")

                max_checkpoint = max(checkpoints.keys())
                messages = visits_checkpoints.get(max_checkpoint, [])
                message = (messages.random + "\n\n") if messages else ''
                month_text = 'this month' if month.month == right_now.month else f"in {month.strftime('%B')}"
                announcement = f"{message}Because you visited us on {max_checkpoint} occasions {month_text}, we want to thank you for your persistence with {a_total_of}{total_points} point{total_points.plural}!"

                if month not in prepared_announcements:
                    prepared_announcements[month] = {}
                prepared_announcements[month][user] = (total_points, announcement)


        convo = ConversationStarter(context.bot, self.users)
        for month, announcements in prepared_announcements.items():
            await convo.send(
                recipients=list(announcements.keys()),
                message=lambda user: {
                    'text': announcements[user][1],
                    'reply_markup': PointsClaim(announcements[user][0]).keyboard(),
                },
                # Award the points directly if we can't send the message for the user to confirm
                on_fail=lambda user: self.loy.add_points(user, announcements[user][0]),
            )

    def _validate_user(self, update: Update) -> User:
        sender_name = update.effective_user.username
        if not sender_name:
            raise MissingUsernameError()

        sender = self.users.get_by_telegram_name(sender_name)
        if not sender:
            raise UserNotFoundError()

        return sender

    @staticmethod
    def _can_earn_points(user: User) -> bool:
        return not user.role.is_staff
