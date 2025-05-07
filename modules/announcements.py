import logging
import pytz
from datetime import time, timedelta

from ratelimit import limits, sleep_and_retry

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import Application, ContextTypes, CommandHandler, filters, CallbackQueryHandler

from data.models.user import User
from data.repositories.user import UserRepository
from data.repositories.visit import VisitRepository

from helpers.business_logic.access_checker import AccessChecker
from helpers.telegram.chat_target import ChatTarget
from helpers.telegram.exceptions import CommandSyntaxError, UserFriendlyError

from modules.base_module import BaseModule

logger = logging.getLogger(__name__)

ANNOUNCE_HELP_TEXT = "To use this command you need to write it like this:\n/announce (message)\nFor example:\n/announce Good news everyone!"


class AnnouncementsModule(BaseModule):
    def __init__(self, ac: AccessChecker, users: UserRepository, visits: VisitRepository, team_schedule_chats: set[ChatTarget], timezone: pytz.timezone = None):
        self._ac: AccessChecker = ac
        self._users = users
        self._visits = visits
        self._team_schedule_chats: set[ChatTarget] = team_schedule_chats.copy()
        self._timezone = timezone
        self._announcements_cache: dict[str, str] = {}

    def install(self, application: Application) -> None:
        application.add_handlers([
            CommandHandler('announce', self._announce, filters.ChatType.PRIVATE & self._ac.filter_master),
            CallbackQueryHandler(self._confirm, pattern="^announce/confirm/"),
            CallbackQueryHandler(self._cancel, pattern="^announce/cancel/"),
        ])
        application.job_queue.run_daily(self._send_schedule_announcement, time(9, 0, 0, 0, self._timezone), days=(3,))

        logger.info("Announcements module installed")

    async def _announce(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            text_to_strip = '/announce '

            raw_message = update.message.text_html_urled
            if not raw_message.startswith(text_to_strip):
                raise CommandSyntaxError()

            parsed_message = raw_message[len(text_to_strip):].lstrip()
            if not parsed_message:
                raise CommandSyntaxError()

            message_hash = str(hash(parsed_message))
            self._announcements_cache[message_hash] = parsed_message

            await update.message.reply_html(parsed_message, do_quote=False, reply_markup=self._confirm_keyboard(message_hash))
        except CommandSyntaxError:
            await update.message.reply_text(ANNOUNCE_HELP_TEXT)
        except UserFriendlyError as e:
            await update.message.reply_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.message.reply_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    async def _confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            args = update.callback_query.data.split('/')
            if len(args) < 3:
                raise UserFriendlyError("There was an error and I could not understand your command. Please try again.")

            message_hash = args[2]
            message = self._announcements_cache.pop(message_hash, None)
            if message is None:
                raise UserFriendlyError("There was an error and I can't seem to remember what you were trying to say. These buttons expire after a while, don't cha know? Please try again.")

            users = self._get_contactable_users()

            await update.callback_query.answer()

            if not users:
                await update.callback_query.edit_message_text("Somehow, there are no eligible champions to receive this announcement. I couldn't send it to anyone, sorry!")
                return

            await update.callback_query.edit_message_text(f"Done! I'm sending your announcement to {len(users)} eligible champions right now. Hopefully it brightens up their day!")

            for user in users:
                await self._send_user_message(context, user.telegram_id, message)
        except UserFriendlyError as e:
            await update.callback_query.edit_message_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.callback_query.edit_message_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    @sleep_and_retry
    @limits(calls=25, period=1)
    async def _send_user_message(self, context: ContextTypes.DEFAULT_TYPE, telegram_id: int, message: str) -> None:
        await context.bot.send_message(telegram_id, message, parse_mode=ParseMode.HTML)

    async def _cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            args = update.callback_query.data.split('/')
            if len(args) < 3:
                raise UserFriendlyError("There was an error and I could not understand your command. Please try again.")

            message_hash = args[2]
            self._announcements_cache.pop(message_hash, None)

            await update.callback_query.answer()
            await update.callback_query.delete_message()
        except UserFriendlyError as e:
            await update.callback_query.edit_message_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.callback_query.edit_message_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    @staticmethod
    def _confirm_keyboard(message_hash: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Confirm and send", callback_data=f"announce/confirm/{message_hash}"),
                InlineKeyboardButton("Cancel", callback_data=f"announce/cancel/{message_hash}"),
            ]
        ])

    def _get_contactable_users(self) -> list[User]:
        all_users = self._users.get_all()
        return [user for user in all_users if self._can_contact_user(user)]

    def _can_contact_user(self, user: User) -> bool:
        # People with a telegram id
        # And who have visited in the last couple of months
        return user.telegram_id and self._visits.has_visited_since(user, timedelta(days=60))

    async def _send_schedule_announcement(self, context: ContextTypes.DEFAULT_TYPE):
        for target in self._team_schedule_chats:
            await context.bot.send_message(
                target.chat_id,
                'Don’t forget to send us your schedule requests and preferences before 5pm!',
                message_thread_id=target.thread_id,
            )
