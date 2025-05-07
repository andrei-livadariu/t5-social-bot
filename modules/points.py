import pytz
import logging
from datetime import datetime, time, timedelta

from telegram import Update, InlineKeyboardButton
from telegram.constants import ChatType
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from data.models.user import User
from data.repositories.user import UserRepository
from data.repositories.visit import VisitRepository

from modules.base_module import BaseModule
from helpers.telegram.exceptions import UserFriendlyError
from helpers.business_logic.points import Points

from messages import points_balance_sarcasm

from integrations.loyverse.api import LoyverseApi

logger = logging.getLogger(__name__)


class PointsModule(BaseModule):
    def __init__(self, loy: LoyverseApi, users: UserRepository, visits: VisitRepository, timezone: pytz.timezone = None):
        self._loy = loy
        self._users = users
        self._visits = visits
        self._timezone = timezone

    def install(self, application: Application) -> None:
        application.add_handlers([
            CommandHandler("balance", self._balance),
            CallbackQueryHandler(self._balance, pattern="^points/balance$"),
        ])
        logger.info("Points module installed")

        application.job_queue.run_daily(self._send_reminders, time(12, 0, 0, 0, self._timezone), days=(1,))

    def get_menu_buttons(self) -> list[list[InlineKeyboardButton]]:
        return [
            [InlineKeyboardButton('Check Your Points', callback_data='points/balance')],
        ]

    async def _balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            user = self._validate_user(update)
            balance = self._loy.get_balance(user).to_integral()
            sarc = points_balance_sarcasm.random

            if update.effective_chat.type == ChatType.PRIVATE:
                reply = f"{sarc}\n\nYou have {balance} T5 Loyalty Point{balance.plural}!"
            else:
                reply = (
                    f"{sarc} {user.main_alias or user.first_name}, you have {balance} T5 Loyalty Point{balance.plural}!\n\n" +
                    'You can also <a href="https://t.me/T5socialBot?start=help">talk to me directly</a> to check your points!'
                )
        except UserFriendlyError as e:
            reply = str(e)
        except Exception as e:
            logger.exception(e)
            reply = f"BeeDeeBeeBoop 🤖 Error : {e}"

        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(reply, disable_web_page_preview=True)
        else:
            await update.message.reply_html(reply, disable_web_page_preview=True)

    async def _send_reminders(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        # Every 2 weeks = only even week numbers; this is not perfect but it works
        if datetime.now(self._timezone).isocalendar().week % 2 != 0:
            return

        for user, points in self._loy.get_all_points():
            if self._should_send_reminder(user, points):
                balance = points.to_integral()
                sarc = points_balance_sarcasm.random
                message = f"{sarc}\n\nYou have {balance} T5 Loyalty Point{balance.plural}!\n\nRemember to spend your points at the bar when you visit! Not sure how? Ask our staff for guidance!"
                await context.bot.send_message(user.telegram_id, message)

    def _should_send_reminder(self, user: User, points: Points) -> bool:
        return user.telegram_id and (not user.role.is_staff) and points >= Points(15) and self._visits.has_visited_since(user, timedelta(days=60))

    def _validate_user(self, update: Update) -> User:
        sender_name = update.effective_user.username
        if not sender_name:
            raise UserFriendlyError("I don't really know who you are - to donate or receive points you first need to create a username in Telegram.")

        sender = self._users.get_by_telegram_name(sender_name)
        if not sender:
            raise UserFriendlyError("Sorry, but this feature is for Community Champions only.")

        return sender
