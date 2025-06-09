import logging

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode, ChatType
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, filters

from data.models.user import User
from data.repositories.user import UserRepository
from helpers.business_logic.league.match_result import MatchResult
from helpers.telegram.chat_target import ChatTarget
from helpers.business_logic.ping_pong_calculator import PingPongCalculator

from modules.base_module import BaseModule
from helpers.telegram.exceptions import UserFriendlyError, MissingUsernameError, UserNotFoundError, CommandSyntaxError

logger = logging.getLogger(__name__)

HELP_TEXT = """🏓 T5 Ping Pong Ladder

Welcome to the T5 Ping Pong Ladder - where friendly rivalry meets fierce forehands. Open to players of all levels, the ladder is your chance to rise through the ranks, challenge new opponents, and earn bragging rights one game at a time.

To check your standing in the ladder, type:

/pingpong standing

To report a win type:

/pingpong win (name of your opponent)

e.g.

/pingpong win Andrei L"""

MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}

class PingPongModule(BaseModule):
    def __init__(self, users: UserRepository, calculator: PingPongCalculator, chats: set[ChatTarget] = None):
        self._calculator = calculator
        self._users = users
        self._chats: set[ChatTarget] = (chats or set()).copy()

    def install(self, application: Application) -> None:
        application.add_handlers([
            CommandHandler("start", self._help, filters.Regex('pingpong')),
            CommandHandler("pingpong", self._base_command),

            CallbackQueryHandler(self._help, pattern="^pingpong/help"),
            CallbackQueryHandler(self._confirm, pattern="^pingpong/confirm/"),
            CallbackQueryHandler(self._cancel, pattern="^pingpong/cancel"),
        ])
        logger.info("Ping pong module installed")

    def get_menu_buttons(self) -> list[list[InlineKeyboardButton]]:
        return []
        # Disabled until we are ready to advertise this feature
        # return [
        #     [InlineKeyboardButton('🏓 Ping Pong Ladder', callback_data='pingpong/help')],
        # ]

    async def _help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(HELP_TEXT)
        else:
            await update.message.reply_html(HELP_TEXT, do_quote=False)

    async def _base_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_chat.type != ChatType.PRIVATE:
            await update.message.reply_html('Please <a href="https://t.me/T5socialBot?start=pingpong">talk to me directly</a> to participate in the Ping Pong Ladder!', disable_web_page_preview=True)
            return

        if not context.args:
            await self._help(update, context)
            return

        match context.args[0]:
            case 'standing':
                await self._standing(update, context)
            case 'win' | 'wins' | 'won':
                await self._win(update, context)
            case 'loss' | 'lost' | 'lose':
                await self._loss(update, context)
            case _:
                await self._help(update, context)

    async def _standing(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            user = self._validate_initiator(update)

            standing = self._calculator.get_standing(user)
            if standing.wins == 0 and standing.losses == 0:
                raise UserFriendlyError("It looks like you haven't played any games yet. Go ahead, challenge someone and take the first step towards your Ping Pong destiny!")

            streak = self._calculator.get_streak(user)
            win_percentage = standing.wins / (standing.wins + standing.losses) * 100

            medal = self._rank_icon(standing.rank)

            message = f"You are number {standing.rank}{medal} in the Ping Pong Ladder."
            message += f"\n\nYour rating is {standing.rating}, with {standing.wins} wins and {standing.losses} losses ({win_percentage:.0f}% win rate)."

            if streak >= 3:
                message += f"\n\nYou are currently on a {streak}-win streak 💪 How high can you go? Keep playing and find out!"
            elif streak <= -3:
                message += f"\n\nYou are currently on a {streak}-loss streak 😭 Keep playing and you will definitely catch a break!"

            await update.message.reply_html(message, do_quote=False)
        except CommandSyntaxError:
            await update.message.reply_html(HELP_TEXT, do_quote=False)
        except UserFriendlyError as e:
            await update.message.reply_html(str(e), do_quote=False)
        except Exception as e:
            logger.exception(e)
            await update.message.reply_text(f"BeeDeeBeeBoop 🤖 Error : {e}", do_quote=False)

    async def _win(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            try:
                target_name = self._validate_target_player_name(context.args)
            except MissingUsernameError as error:
                raise UserFriendlyError('Who did you win against? Please try the command again.') from error

            initiator = self._validate_initiator(update)
            targets = self._validate_possible_targets(target_name, initiator)
            target = list(targets)[0] if len(targets) == 1 else None

            if target:
                await update.message.reply_text(
                    f"You are marking a win against {target.specific_name}. Are you sure?",
                    reply_markup=PingPongModule._confirm_keyboard(target, 'win')
                )
                return

            await update.message.reply_text(
                "There is more than one person who goes by that name. Please select the right one from the choices below.",
                reply_markup=PingPongModule._choose_keyboard(targets, 'win'),
                do_quote=False
            )
        except CommandSyntaxError:
            await update.message.reply_html(HELP_TEXT, do_quote=False)
        except UserFriendlyError as e:
            await update.message.reply_html(str(e), do_quote=False)
        except Exception as e:
            logger.exception(e)
            await update.message.reply_text(f"BeeDeeBeeBoop 🤖 Error : {e}", do_quote=False)

    async def _loss(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            try:
                target_name = self._validate_target_player_name(context.args)
            except MissingUsernameError as error:
                raise UserFriendlyError('Who did you lose against? Please try the command again.') from error

            initiator = self._validate_initiator(update)
            targets = self._validate_possible_targets(target_name, initiator)
            target = list(targets)[0] if len(targets) == 1 else None

            if target:
                await update.message.reply_text(
                    f"You are marking a loss against {target.specific_name}. Are you sure?",
                    reply_markup=PingPongModule._confirm_keyboard(target, 'loss')
                )
                return

            await update.message.reply_text(
                "There is more than one person who goes by that name. Please select the right one from the choices below.",
                reply_markup=PingPongModule._choose_keyboard(targets, 'loss'),
                do_quote=False
            )
        except CommandSyntaxError:
            await update.message.reply_html(HELP_TEXT, do_quote=False)
        except UserFriendlyError as e:
            await update.message.reply_html(str(e), do_quote=False)
        except Exception as e:
            logger.exception(e)
            await update.message.reply_text(f"BeeDeeBeeBoop 🤖 Error : {e}", do_quote=False)

    async def _confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            args = update.callback_query.data.split('/')
            if len(args) < 4:
                raise UserFriendlyError("There was an error and I could not understand your command. Please try again.")

            initiator = self._validate_initiator(update)
            target = self._validate_target_direct(args[2], initiator)

            action = args[3]
            if action == 'win':
                winner = initiator
                loser = target
            elif action == 'loss':
                winner = target
                loser = initiator
            else:
                raise UserFriendlyError("What is there to do besides winning or losing? Please try again.")

            game = self._calculator.game_over(winner, loser)
            message = self._game_recap(game)

            await update.callback_query.answer()
            await update.callback_query.edit_message_text(message)

            for target in self._chats:
                await context.bot.send_message(target.chat_id, message, parse_mode=ParseMode.HTML, message_thread_id=target.thread_id)
        except UserFriendlyError as e:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    async def _cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text("You were soooo close, but you turned away at the last moment. Don't worry - nobody else will know. It'll be our little secret.")
        except UserFriendlyError as e:
            await update.callback_query.edit_message_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.callback_query.edit_message_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    def _game_recap(self, game: tuple[MatchResult, MatchResult]) -> str:
        winner, loser = game

        winner_medal = self._rank_icon(winner.rank.after)
        loser_medal = self._rank_icon(loser.rank.after)

        message = f"{winner.player.friendly_name} wins vs {loser.player.friendly_name}."

        commentary: list[str] = []

        if winner.streak.after >= 3:
            commentary.append(f"is on a {winner.streak.after}-win streak 💪")
        elif winner.streak.before <= -3:
            commentary.append(f"broke out of their losing streak 🥲")

        if loser.streak.before >= 3:
            commentary.append(f"smashed their opponent's {loser.streak.before}-win streak ☠️")

        if winner.rank.delta <= -3:
            commentary.append(f"jumped ahead {-winner.rank.delta} ranks 🤯")

        if winner.rank.after <= 3 and winner.rank.delta < 0:
            commentary.append(f"is now number {winner.rank.after}{winner_medal} in the ladder!")
        elif winner.rank.after <= 3 and winner.rank.delta == 0:
            commentary.append(f"defended their position as number {winner.rank.after}{winner_medal} in the ladder!")

        if commentary:
            message += f"\n\n{winner.player.friendly_first_name} {self._enumerate(commentary)}"

        message += f"\n\n{winner.player.friendly_first_name} / Rank {winner.rank.after}{winner_medal}{self._arrow_icon(-winner.rank.delta)} / Rating {winner.rating.after:.0f} (+{winner.rating.delta:.0f})"
        message += f"\n{loser.player.friendly_first_name} / Rank {loser.rank.after}{loser_medal}{self._arrow_icon(-loser.rank.delta)} / Rating {loser.rating.after:.0f} ({loser.rating.delta:.0f})"

        return message

    def _rank_icon(self, rank: int) -> str:
        return MEDALS.get(rank, '')

    def _arrow_icon(self, delta: int | float) -> str:
        if delta > 0:
            return ' ⬆️'
        if delta < 0:
            return ' ⬇️'
        return ''

    def _validate_initiator(self, update: Update) -> User:
        sender_name = update.effective_user.username
        if not sender_name:
            raise MissingUsernameError()

        sender = self._users.get_by_telegram_name(sender_name)
        if not sender:
            raise UserNotFoundError()

        return sender

    def _validate_target_player_name(self, args: list[str]) -> str:
        args = args[1:]

        if not args:
            raise MissingUsernameError()

        if args[0] == 'vs' or args[0] == 'against':
            args = args[1:]

        if not args:
            raise MissingUsernameError()

        return ' '.join(args)

    def _validate_possible_targets(self, query: str, initiator: User) -> set[User]:
        if len(query) < 2:
            raise UserFriendlyError("Minimalism is a quality to be admired, but not when looking for people's names. Please try a longer name.")

        targets = self._users.search(query)
        if not targets:
            raise UserFriendlyError("I don't know this strange person that you are referring to - is this one of our community members?")

        # Don't allow targeting yourself
        targets.discard(initiator)
        # If the set is empty after removing the user, then we were trying to target ourselves
        if not targets:
            raise UserFriendlyError("Plato said that the first and greatest victory is to conquer yourself. In ping pong, however, you need to conquer <i>other people</i>. Please try somebody else.")

        return targets

    def _validate_target_direct(self, telegram_name: str, initiator: User) -> User:
        target = self._users.get_by_telegram_name(telegram_name)
        if not target:
            raise UserFriendlyError("I don't know this strange person that you are referring to - is this one of our community members?")

        if initiator == target:
            raise UserFriendlyError("Plato said that the first and greatest victory is to conquer yourself. In ping pong, however, you need to conquer <i>other people</i>. Please try somebody else.")

        return target

    @staticmethod
    def _confirm_keyboard(recipient: User, action: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                PingPongModule._confirm_button(recipient, action, "Yes, I'm sure"),
                PingPongModule._cancel_button("No, cancel"),
            ]
        ])

    @staticmethod
    def _choose_keyboard(recipients: set[User], action: str) -> InlineKeyboardMarkup:
        recipients = sorted(recipients, key=lambda u: u.aliases[0] if u.aliases else u.full_name)
        buttons = [PingPongModule._confirm_button(u, action) for u in recipients]
        buttons.append(PingPongModule._cancel_button())

        return InlineKeyboardMarkup([[b] for b in buttons])

    @staticmethod
    def _confirm_button(user: User, action: str, text: str = '') -> InlineKeyboardButton:
        return InlineKeyboardButton(
            text or user.specific_name,
            callback_data=f"pingpong/confirm/{user.telegram_username}/{action}"
        )

    @staticmethod
    def _cancel_button(text: str = "Cancel") -> InlineKeyboardButton:
        return InlineKeyboardButton(text, callback_data=f"pingpong/cancel")

    @staticmethod
    def _enumerate(lst: list[str]) -> str:
        return (', '.join(lst[:-1]) + ' and ' + lst[-1]) if len(lst) > 1 else lst[0]
