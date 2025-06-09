import logging

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, filters

from data.repositories.user import UserRepository
from data.models.raffle_entry import RaffleEntry
from data.models.user import User

from helpers.business_logic.raffle import Raffle
from helpers.telegram.exceptions import MissingUsernameError, UserNotFoundError, UserFriendlyError
from modules.base_module import BaseModule

from integrations.loyverse.exceptions import InsufficientFundsError

logger = logging.getLogger(__name__)

TALK_DIRECTLY = """<a href="https://t.me/T5socialBot?start=raffle">Talk to me directly</a> to participate!"""

class RaffleModule(BaseModule):
    def __init__(self, raffle: Raffle, users: UserRepository):
        self.raffle = raffle
        self.users = users

    def install(self, application: Application) -> None:
        if not self.raffle.is_active:
            return

        application.add_handlers([
            CommandHandler("start", self._help, filters.Regex('raffle')),
            CommandHandler("zaganu", self._help, filters.ChatType.PRIVATE),
            CommandHandler("zaganu", self._help_public, filters.ChatType.GROUPS | filters.ChatType.CHANNEL),

            CallbackQueryHandler(self._help, pattern="^raffle/help"),
            CallbackQueryHandler(self._buy, pattern="^raffle/buy"),
            CallbackQueryHandler(self._list_entries, pattern="^raffle/list_entries"),
        ])
        logger.info(f"Raffle module installed")

    def get_menu_buttons(self) -> list[list[InlineKeyboardButton]]:
        return [
            [InlineKeyboardButton(self.raffle.title, callback_data='raffle/help')],
        ] if self.raffle.is_active else []

    async def _help_public(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            self._validate_is_active()

            message = self.raffle.description + "\n\n" + TALK_DIRECTLY

            await update.message.reply_html(message, disable_web_page_preview=True, do_quote=False)
        except UserFriendlyError as e:
            await update.message.reply_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.message.reply_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    async def _help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            self._validate_is_active()
            user = self._validate_user(update)
            keyboard = self._menu_keyboard('raffle/help', user)

            message = self.raffle.description

            if self.raffle.has_entries(user):
                message += "\n\n<i>You are already participating!</i>"

            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(message, reply_markup=keyboard, parse_mode=ParseMode.HTML)
            else:
                await update.message.reply_html(message, reply_markup=keyboard, disable_web_page_preview=True)
        except UserFriendlyError as e:
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(str(e))
            else:
                await update.message.reply_text(str(e))
        except Exception as e:
            logger.exception(e)
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(f"BeeDeeBeeBoop 🤖 Error : {e}")
            else:
                await update.message.reply_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    async def _buy(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            self._validate_is_active()
            user = self._validate_user(update)

            self._execute_buy(user)

            message = f"Congrats {user.main_alias or user.first_name}! You just entered the {self.raffle.title}!\n\nGood luck!"

            keyboard = self._menu_keyboard('raffle/bought', user)

            await update.callback_query.answer()
            await update.callback_query.edit_message_text(message, reply_markup=keyboard)
        except UserFriendlyError as e:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    def _execute_buy(self, user: User) -> None:
        if not self.raffle.can_enter(user):
            if self.raffle.max_tickets == 1:
                raise UserFriendlyError(f"You have already entered the {self.raffle.title}. Or maybe it was your evil twin? In any case, one shot is all you get.")
            else:
                raise UserFriendlyError(f"You already have {self.raffle.max_tickets} entries so any more than this will only get you a red card!")

        try:
            self.raffle.buy_ticket(user)
        except InsufficientFundsError as error:
            raise UserFriendlyError(f"Oh no! You don't have enough points for the {self.raffle.title}. Buy some drinks from the bar or beg a friend for a donation!") from error

    async def _list_entries(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            user = self._validate_user(update)

            all_entries = self.raffle.get_all_entries()
            if all_entries:
                text = f"The following people have entered the {self.raffle.title}:\n\n"

                lines = [self.format_entries(user, entries) for user, entries in all_entries.items()]
                text += "\n".join(lines)
            else:
                text = "Nobody is playing yet! Will you be the one to break the ice?"

            keyboard = self._menu_keyboard('raffle/list_entries', user)

            await update.callback_query.answer()
            await update.callback_query.edit_message_text(text, reply_markup=keyboard)
        except UserFriendlyError as e:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    def _menu_keyboard(self, current_entry: str, user: User) -> InlineKeyboardMarkup:
        if self.raffle.can_enter(user):
            if self.raffle.has_entries(user):
                buy = InlineKeyboardButton("One more ticket, please!", callback_data="raffle/buy")
            else:
                buy = InlineKeyboardButton("I want to join!", callback_data="raffle/buy")
        else:
            buy = None

        buttons = [
            buy,
            InlineKeyboardButton("Who's playing?", callback_data="raffle/list_entries"),
            InlineKeyboardButton("How does it work?", callback_data="raffle/help"),
        ]

        buttons = [button for button in buttons if button and button.callback_data != current_entry]

        if len(buttons) == 3:
            buttons = [
                [buttons[0]],
                [buttons[1], buttons[2]]
            ]
        else:
            buttons = [buttons]

        return InlineKeyboardMarkup(buttons)

    def format_entries(self, user: User, entries: set[RaffleEntry]) -> str:
        return user.friendly_name

    def _validate_user(self, update: Update) -> User:
        sender_name = update.effective_user.username
        if not sender_name:
            raise MissingUsernameError()

        sender = self.users.get_by_telegram_name(sender_name)
        if not sender:
            raise UserNotFoundError()

        return sender

    def _validate_is_active(self) -> None:
        if not self.raffle.is_active:
            raise UserFriendlyError(f"Entries for the {self.raffle.title} are now closed!")
