import logging

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatType
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, filters

from data.models.user import User
from data.repositories.user import UserRepository

from modules.base_module import BaseModule
from helpers.business_logic.access_checker import AccessChecker
from helpers.telegram.exceptions import UserFriendlyError, CommandSyntaxError, UserNotFoundError, MissingUsernameError
from helpers.business_logic.points import Points
from helpers.telegram.chat_target import ChatTarget

from messages import donate_sarcasm

from integrations.loyverse.api import LoyverseApi
from integrations.loyverse.exceptions import InsufficientFundsError, InvalidCustomerError

logger = logging.getLogger(__name__)


class DonateModule(BaseModule):
    HELP_TEXT = "To use this command you need to write it like this:\n/donate name points\nFor example:\n/donate Moni G 5"

    def __init__(self, loy: LoyverseApi, ac: AccessChecker, users: UserRepository, announcement_chats: set[ChatTarget] = None):
        self.loy: LoyverseApi = loy
        self.ac: AccessChecker = ac
        self.users: UserRepository = users
        self.announcement_chats: set[ChatTarget] = (announcement_chats or set()).copy()

    def install(self, application: Application) -> None:
        application.add_handlers([
            CommandHandler("donate", self._initiate),

            CommandHandler("start", self._initiate, filters.Regex('donate')),

            CallbackQueryHandler(self._help, pattern="^donate/help"),
            CallbackQueryHandler(self._confirm, pattern="^donate/confirm/"),
            CallbackQueryHandler(self._cancel, pattern="^donate/cancel"),
        ])
        logger.info("Donate module installed")

    def get_menu_buttons(self) -> list[list[InlineKeyboardButton]]:
        return [
            [InlineKeyboardButton('Donate Points', callback_data='donate/help')],
        ]

    async def _help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(self.HELP_TEXT)

    async def _initiate(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            recipient_name, point_string = DonateModule._parse_args(context.args)

            points = self._validate_points(point_string)
            sender = self._validate_sender(update)
            recipients = self._validate_possible_recipients(recipient_name, sender)

            recipient = list(recipients)[0] if len(recipients) == 1 else None

            if update.message.chat.type == ChatType.PRIVATE:
                if recipient:
                    await update.message.reply_text(
                        f"You are about to donate {points} point{points.plural} to {recipient.specific_name}. Are you sure?",
                        reply_markup=DonateModule._confirm_keyboard(recipient, points)
                    )
                else:
                    await update.message.reply_text(
                        "There is more than one person who goes by that name. Please select the right one from the choices below.",
                        reply_markup=DonateModule._choose_keyboard(recipients, points)
                    )
                return

            if not recipient:
                # This passthrough is parsed by the private chat, so you can continue donating to the same user
                passthrough = f"donate_{recipient_name.replace(' ', '-')}_{points}"
                await update.message.reply_html(f"There is more than one person who goes by that name. Please <a href=\"https://t.me/T5socialBot?start={passthrough}\">contact me in private</a> so I can help you find the right one.", disable_web_page_preview=True)
                return

            self._execute_donation(sender, recipient, points)

            messages = DonateModule._make_donation_messages(sender, recipient, points)

            await update.message.reply_text(messages['announcement'], do_quote=False)
            if recipient.telegram_id:
                await context.bot.send_message(recipient.telegram_id, messages['recipient'])
        except CommandSyntaxError:
            await update.message.reply_text(self.HELP_TEXT)
        except UserFriendlyError as e:
            await update.message.reply_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.message.reply_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    @staticmethod
    def _parse_args(args: list[str]) -> tuple[str, str]:
        if not args:
            raise CommandSyntaxError()

        if len(args) == 1:
            return DonateModule._parse_single_argument_form(args)

        return DonateModule._parse_multi_argument_form(args)

    @staticmethod
    def _parse_single_argument_form(args: list[str]) -> tuple[str, str]:
        # One argument: /start donate_Moni-G_5
        # This form is only used when switching from group chat to private chat
        tokens = args[0].split('_')
        if tokens[0] != 'donate' or len(tokens) < 3:
            raise CommandSyntaxError()

        recipient_name = "_".join(tokens[1: -1]).replace('-', ' ')
        point_string = tokens[-1]

        return recipient_name, point_string

    @staticmethod
    def _parse_multi_argument_form(args: list[str]) -> tuple[str, str]:
        # 2 or more arguments: /donate Moni G 5 - this command can also have more text after the number
        # The name may have spaces in it, which the library interprets as separate arguments
        # Parsing the name stops when we come across a number and ignore any text after it
        i = 0
        while (i < len(args)) and (not args[i].isnumeric()):
            i += 1

        if i == 0:
            raise CommandSyntaxError()

        if i >= len(args):
            raise CommandSyntaxError()

        recipient_name = " ".join(args[0:i]).lstrip('@')
        point_string = args[i]

        return recipient_name, point_string

    async def _confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            args = update.callback_query.data.split('/')
            if len(args) < 4:
                raise UserFriendlyError("There was an error and I could not understand your command. Please try again.")

            points = self._validate_points(args[3])
            sender = self._validate_sender(update)
            recipient = self._validate_recipient_direct(args[2], sender)

            self._execute_donation(sender, recipient, points)

            messages = DonateModule._make_donation_messages(sender, recipient, points)

            await update.callback_query.answer()
            await update.callback_query.edit_message_text(messages['sender'])

            if recipient.telegram_id:
                await context.bot.send_message(recipient.telegram_id, messages['recipient'])
            else:
                for target in self.announcement_chats:
                    await context.bot.send_message(target.chat_id, messages['announcement'], message_thread_id=target.thread_id)
        except UserFriendlyError as e:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    async def _cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("You were soooo close, but you turned away at the last moment. Don't worry - nobody else will know. It'll be our little secret.")

    def _validate_points(self, raw_points: str) -> Points:
        points = Points(raw_points)
        if not points.is_positive:
            raise UserFriendlyError("Your sense of charity is as high as the amount of points you tried to donate - donations have to be greater than zero.")

        return points

    def _validate_sender(self, update: Update) -> User:
        sender_name = update.effective_user.username
        if not sender_name:
            raise MissingUsernameError()

        sender = self.users.get_by_telegram_name(sender_name)
        if not sender:
            raise UserNotFoundError()

        return sender

    def _validate_possible_recipients(self, query: str, sender: User) -> set[User]:
        if len(query) < 3:
            raise UserFriendlyError("Minimalism is a quality to be admired, but not when looking for people's names. Please try a longer name.")

        recipients = self.users.search(query)
        if not recipients:
            raise UserFriendlyError("I don't know this strange person that you are trying to donate to - is this one of our community members?")

        # Don't allow sending to yourself
        recipients.discard(sender)
        # If the set is empty after removing the sender, then we were trying to donate to ourselves
        if not recipients:
            raise UserFriendlyError("Donating to yourself is like high-fiving in a mirror – impressive to you, but not making the world a better place!")

        return recipients

    def _validate_recipient_direct(self, telegram_name: str, sender: User) -> User:
        recipient = self.users.get_by_telegram_name(telegram_name)
        if not recipient:
            raise UserFriendlyError("I don't know this strange person that you are trying to donate to - is this one of our community members?")

        if sender == recipient:
            raise UserFriendlyError("Donating to yourself is like high-fiving in a mirror – impressive to you, but not making the world a better place!")

        return recipient

    def _execute_donation(self, sender: User, recipient: User, points: Points) -> None:
        if not self.ac.can_donate_for_free(sender):
            try:
                self.loy.remove_points(sender, points)
            except InvalidCustomerError as error:
                raise UserFriendlyError(f"You do not have a bar tab as a community member. You should ask Rob to make one for you.") from error
            except InsufficientFundsError as error:
                raise UserFriendlyError("Your generosity is the stuff of legends, but you cannot donate more points than you have in your balance.") from error
            except Exception as error:
                raise UserFriendlyError("The donation has failed - perhaps the stars were not right? You can try again later.") from error

        try:
            self.loy.add_points(recipient, points)
        except InvalidCustomerError as error:
            raise UserFriendlyError(f"{recipient.friendly_name} does not have a bar tab as a community member. You should ask Rob to make one for them.") from error
        except Exception as error:
            raise UserFriendlyError("The donation has failed - perhaps the stars were not right? You can try again later.") from error

    @staticmethod
    def _make_donation_messages(sender: User, recipient: User, points: Points) -> dict[str, str]:
        sarc = donate_sarcasm.random

        return {
            "sender": f"{sarc} You donated {points} point{points.plural} to {recipient.friendly_name}.",
            "recipient": f"{sender.friendly_name} donated {points} point{points.plural} to you!",
            "announcement": f"{sarc} {sender.friendly_name} donated {points} point{points.plural} to {recipient.friendly_name}.",
        }

    @staticmethod
    def _confirm_keyboard(recipient: User, points: Points) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                DonateModule._confirm_button(recipient, points, "Yes, I'm sure"),
                DonateModule._cancel_button("No, cancel"),
            ]
        ])

    @staticmethod
    def _choose_keyboard(recipients: set[User], points: Points) -> InlineKeyboardMarkup:
        recipients = sorted(recipients, key=lambda u: u.aliases[0] if u.aliases else u.full_name)
        buttons = [DonateModule._confirm_button(u, points) for u in recipients]
        buttons.append(DonateModule._cancel_button())

        return InlineKeyboardMarkup([[b] for b in buttons])

    @staticmethod
    def _confirm_button(user: User, points, text: str = '') -> InlineKeyboardButton:
        return InlineKeyboardButton(
            text or user.specific_name,
            callback_data=f"donate/confirm/{user.telegram_username}/{points}"
        )

    @staticmethod
    def _cancel_button(text: str = 'Cancel') -> InlineKeyboardButton:
        return InlineKeyboardButton(text, callback_data=f"donate/cancel")
