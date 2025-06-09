import logging
import re
from typing import Callable

import pytz
from datetime import datetime, time, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatType
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, filters

from data.models.nomination import Nomination
from data.models.user import User
from data.repositories.nomination import NominationRepository
from data.repositories.user import UserRepository
from data.repositories.visit import VisitRepository
from helpers.telegram.conversation_starter import ConversationStarter

from modules.base_module import BaseModule
from helpers.telegram.exceptions import UserFriendlyError, CommandSyntaxError, UserNotFoundError, MissingUsernameError

logger = logging.getLogger(__name__)

NOMINATE_HELP_TEXT = """Every week that you visit T5 you can nominate one community member who stood out, went the extra mile, brought great vibes, or made the space shine! 💛

We’ll share your nomination at the end of the month, and they might even be crowned our Community Champion! 🏆

To use this command you need to write it like this:

/nominate (first name) (reason)

For example:

/nominate Moni Because she's an awesome bartender!

Since we have so many people with similar names, I will then help you choose the exact person you need.
"""

NOMINATE_ANNOUNCEMENT: Callable[[User], str] = lambda user: f"""Hey {user.first_name} – thanks for visiting T5 this week! 🙌 Don't forget to check your /balance and our upcoming /events. 

If a community member stood out this week, went the extra mile, brought great vibes, or made the space shine – you can /nominate them now! 💛 

We’ll share your nomination at the end of the month, and they might even be crowned our Community Champion! 🏆"""

class NominateModule(BaseModule):
    def __init__(self, users: UserRepository, visits: VisitRepository, nominations: NominationRepository, timezone: pytz.timezone):
        self._users = users
        self._visits = visits
        self._nominations = nominations
        self._timezone = timezone
        self._reasons_cache: dict[str, str] = {}

    def install(self, application: Application) -> None:
        application.add_handlers([
            CommandHandler("nominate", self._initiate),

            CommandHandler("start", self._help, filters.Regex('nominate')),

            CallbackQueryHandler(self._help, pattern="^nominate/help"),
            CallbackQueryHandler(self._confirm, pattern="^nominate/confirm/"),
            CallbackQueryHandler(self._cancel, pattern="^nominate/cancel"),
        ])

        application.job_queue.run_daily(self._send_reminders, time(12, 0, 0, 0, self._timezone), days=(1,))

        logger.info("Nominate module installed")

    def get_menu_buttons(self) -> list[list[InlineKeyboardButton]]:
        return [
            [InlineKeyboardButton('Nominate a community member', callback_data='nominate/help')],
        ]

    async def _help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(NOMINATE_HELP_TEXT)
        else:
            await update.message.reply_html(NOMINATE_HELP_TEXT, do_quote=False)

    async def _initiate(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            if update.effective_chat.type != ChatType.PRIVATE:
                raise UserFriendlyError('Please <a href="https://t.me/T5socialBot?start=nominate">talk to me directly</a> to nominate community members!')

            if not context.args:
                raise CommandSyntaxError()

            nominee_name = context.args[0]
            raw_message = update.message.text
            reason = re.sub(
                r"^/nominate\s+" + nominee_name + r"\s+",
                "",
                raw_message,
                count=1
            )

            if not reason or len(context.args) < 2:
                raise UserFriendlyError("I'd really like to know the reason for your nomination so I can tell this person how great they are! Please type your reason after the name.")

            voter = self._validate_voter(update)
            self._validate_time_rules(voter)

            nominees = self._validate_possible_nominees(nominee_name, voter)
            nominee = list(nominees)[0] if len(nominees) == 1 else None

            reason_hash = self._remember_reason(reason)

            if nominee:
                await update.message.reply_text(
                    f"You are about to nominate {nominee.specific_name}. Are you sure?",
                    reply_markup=NominateModule._confirm_keyboard(nominee, reason_hash)
                )
                return

            await update.message.reply_text(
                "There is more than one person who goes by that name. Please select the right one from the choices below.",
                reply_markup=NominateModule._choose_keyboard(nominees, reason_hash),
                do_quote=False
            )
        except CommandSyntaxError:
            await update.message.reply_text(NOMINATE_HELP_TEXT, do_quote=False)
        except UserFriendlyError as e:
            await update.message.reply_html(str(e), do_quote=False, disable_web_page_preview=True)
        except Exception as e:
            logger.exception(e)
            await update.message.reply_text(f"BeeDeeBeeBoop 🤖 Error : {e}", do_quote=False)

    def _remember_reason(self, reason: str) -> str:
        reason_hash = str(hash(reason))
        self._reasons_cache[reason_hash] = reason
        return reason_hash

    def _recall_reason(self, reason_hash: str) -> str | None:
        return self._reasons_cache.pop(reason_hash, None)

    async def _confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            args = update.callback_query.data.split('/')
            if len(args) < 4:
                raise UserFriendlyError("There was an error and I could not understand your command. Please try again.")

            right_now = datetime.now(self._timezone)

            voter = self._validate_voter(update)
            self._validate_time_rules(voter)

            nominee = self._validate_nominee_direct(args[2], voter)
            reason = self._recall_reason(args[3]) or ''

            await update.callback_query.answer()
            await update.callback_query.edit_message_text(f"Thank you for letting me know your nomination! Someone made your day better and now you're sending back some good vibes!")

            self._nominations.insert(Nomination(
                nominee=nominee.full_name,
                date=right_now,
                voted_by=voter.full_name,
                reason=reason,
            ))
        except UserFriendlyError as e:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    async def _cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            args = update.callback_query.data.split('/')
            if len(args) < 3:
                raise UserFriendlyError("There was an error and I could not understand your command. Please try again.")

            self._recall_reason(args[2])

            await update.callback_query.answer()
            await update.callback_query.edit_message_text("You were soooo close, but you turned away at the last moment. Don't worry - nobody else will know. It'll be our little secret.")
        except UserFriendlyError as e:
            await update.callback_query.edit_message_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.callback_query.edit_message_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    async def _send_reminders(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        convo = ConversationStarter(context.bot, self._users)

        this_monday, last_monday = self._get_mondays()
        users = [user for user in self._users.get_all() if self._visits.has_visited_since(user, last_monday)]

        await convo.send(users, NOMINATE_ANNOUNCEMENT)

    def _get_mondays(self) -> tuple[datetime, datetime]:
        today = datetime.now(self._timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        this_monday = today - timedelta(days=today.weekday())
        last_monday = this_monday - timedelta(days=7)

        return this_monday, last_monday

    def _validate_voter(self, update: Update) -> User:
        user_name = update.effective_user.username
        if not user_name:
            raise MissingUsernameError()

        user = self._users.get_by_telegram_name(user_name)
        if not user:
            raise UserNotFoundError()

        return user

    def _validate_time_rules(self, voter: User) -> None:
        this_monday, last_monday = self._get_mondays()

        if not self._visits.has_visited_since(voter, last_monday):
            raise UserFriendlyError("It seems like you haven't visited in the last week. You have to come to T5 to run into someone cool that you can nominate.")

        if self._nominations.has_voted(voter, this_monday):
            raise UserFriendlyError("It seems that you've already nominated a community member this week. I appreciate your enthusiasm, but you need to wait for next week.")

    def _validate_possible_nominees(self, query: str, voter: User) -> set[User]:
        if len(query) < 2:
            raise UserFriendlyError("Minimalism is a quality to be admired, but not when looking for people's names. Please try a longer name.")

        nominees = self._users.search(query)
        if not nominees:
            raise UserFriendlyError("I don't know this strange person that you are trying to nominate - is this one of our community members?")

        # Don't allow sending to yourself
        nominees.discard(voter)
        # If the set is empty after removing the voter, then we were trying to donate to ourselves
        if not nominees:
            raise UserFriendlyError("Nominating yourself is like high-fiving in a mirror – impressive to you, but not making the world a better place!")

        return nominees

    def _validate_nominee_direct(self, telegram_name: str, voter: User) -> User:
        nominee = self._users.get_by_telegram_name(telegram_name)
        if not nominee:
            raise UserFriendlyError("I don't know this strange person that you are trying to nominate - is this one of our community members?")

        if voter == nominee:
            raise UserFriendlyError("Nominating yourself is like high-fiving in a mirror – impressive to you, but not making the world a better place!")

        return nominee

    @staticmethod
    def _confirm_keyboard(recipient: User, reason_hash: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                NominateModule._confirm_button(recipient, reason_hash, "Yes, I'm sure"),
                NominateModule._cancel_button(reason_hash, "No, cancel"),
            ]
        ])

    @staticmethod
    def _choose_keyboard(recipients: set[User], reason_hash: str) -> InlineKeyboardMarkup:
        recipients = sorted(recipients, key=lambda u: u.aliases[0] if u.aliases else u.full_name)
        buttons = [NominateModule._confirm_button(u, reason_hash) for u in recipients]
        buttons.append(NominateModule._cancel_button(reason_hash))

        return InlineKeyboardMarkup([[b] for b in buttons])

    @staticmethod
    def _confirm_button(user: User, reason_hash: str, text: str = '') -> InlineKeyboardButton:
        return InlineKeyboardButton(
            text or user.specific_name,
            callback_data=f"nominate/confirm/{user.telegram_username}/{reason_hash}"
        )

    @staticmethod
    def _cancel_button(reason_hash: str, text: str = "Cancel") -> InlineKeyboardButton:
        return InlineKeyboardButton(text, callback_data=f"nominate/cancel/{reason_hash}")
