import re
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, filters

from helpers.access_checker import AccessChecker
from helpers.loyverse import LoyverseConnector
from helpers.prompt_parser import parse


prompts = parse("resources/raffle_prompts.txt")


class Raffle:
    def __init__(self, lc: LoyverseConnector, ticket_price, max_tickets: int = 3):
        self.lc = lc
        self.ticket_price = ticket_price
        self.max_tickets = max_tickets
        self.entries = {}

    def buy_ticket(self, username: str) -> int:
        # self.lc.remove_points(username, self.ticket_price)
        self.entries[username] = self.get_tickets(username) + 1
        return self.get_tickets(username)

    def get_tickets(self, username: Optional[str]) -> int:
        return self.entries.get(username, 0)

    def has_bought_tickets(self, username: Optional[str]) -> bool:
        return self.get_tickets(username) > 0

    def can_buy_tickets(self, username: Optional[str]) -> bool:
        return self.max_tickets > 0 and self.get_tickets(username) < self.max_tickets


class RaffleModule:
    def __init__(self, raffle: Raffle, ac: AccessChecker):
        self.raffle = raffle
        self.ac = ac

    def install(self, application: Application) -> None:
        application.add_handlers([
            CommandHandler("start", self.__help, filters.Regex('raffle')),
            CommandHandler("raffle", self.__help),

            CommandHandler('raffle_set_entries', self.__set_entries),
            CallbackQueryHandler(self.__button_clicked)
        ])

    async def __button_clicked(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query

        if query.data == "raffle/help":
            await self.__help(update, context)
        elif query.data == "raffle/buy":
            await self.__buy(update, context)
        elif query.data == "raffle/list_entries":
            await self.__list_entries(update, context)

    async def __help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = prompts.get('explain')
        if not update.effective_user.username:
            text += "\n\nYou need to have a Telegram username to play."

        keyboard = self.__menu_keyboard('raffle/help', update.effective_user.username)

        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(text, reply_markup=keyboard)
        else:
            await update.message.reply_text(text, reply_markup=keyboard)

    async def __buy(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user.username

        try:
            if not user:
                raise Exception('You first need to choose a username in Telegram')

            if not self.raffle.can_buy_tickets(user):
                raise Exception(f"You already have {self.raffle.max_tickets} tickets and you're not getting any more than this!")

            tickets = self.raffle.buy_ticket(user)

            text = f"Congrats @{update.effective_user.username}! You just bought a ticket for the Community Raffle!\n\nYou have a total of {self.__format_tickets(tickets)} out of a maximum of {self.__format_tickets(self.raffle.max_tickets)}.\n\nThanks for supporting and good luck!"

            keyboard = self.__menu_keyboard('raffle/bought', user)

            if update.callback_query:
                await update.callback_query.answer('You have joined the Community Raffle!')
                await update.callback_query.edit_message_text(text, reply_markup=keyboard)
            else:
                await update.message.reply_text(text, reply_markup=keyboard)
        except Exception as e:
            if update.callback_query:
                await update.callback_query.answer(str(e))
            else:
                keyboard = self.__menu_keyboard('raffle/bought', user)
                await update.message.reply_text(str(e), reply_markup=keyboard)

    async def __list_entries(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if self.raffle.entries:
            text = f"The following people are playing in this raffle:\n\n"

            for username, tickets in self.raffle.entries.items():
                text += f"@{username} - {self.__format_tickets(tickets)}\n"
        else:
            text = "Nobody is playing yet! Will you be the one to break the ice?"

        keyboard = self.__menu_keyboard('raffle/list_entries', update.effective_user.username)

        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(text, reply_markup=keyboard)
        else:
            await update.message.reply_text(text, reply_markup=keyboard)

    async def __set_entries(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.ac.is_master(update.effective_user.username):
            return

        self.raffle.entries = {}
        for username, tickets in re.findall(r'@([^ ]+) - ([0-9]+) ', update.message.text):
            self.raffle.entries[username] = int(tickets)

        await self.__list_entries(update, context)

    def __menu_keyboard(self, current_entry: str, username: Optional[str]) -> InlineKeyboardMarkup:
        if self.raffle.can_buy_tickets(username):
            if self.raffle.has_bought_tickets(username):
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

    def __format_tickets(self, count: int) -> str:
        return f'{count} ' + ('ticket' if count == 1 else 'tickets')
