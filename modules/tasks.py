import logging
import pytz
from datetime import datetime, date, time

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import Application, ContextTypes, CallbackQueryHandler

from data.repositories.task import TaskRepository

from helpers.exceptions import UserFriendlyError
from helpers.chat_target import ChatTarget

from modules.base_module import BaseModule
from data.models.task import Task

logger = logging.getLogger(__name__)


class TasksModule(BaseModule):
    def __init__(self, tasks: TaskRepository, tasks_chats: set[ChatTarget], timezone: pytz.timezone = None):
        self.tasks_chats: set[ChatTarget] = tasks_chats.copy()
        self.tasks = tasks
        self.timezone = timezone

    def install(self, application: Application) -> None:
        application.add_handlers([
            CallbackQueryHandler(self._toggle, pattern="^tasks/toggle/"),
        ])

        application.job_queue.run_daily(self._clear_day, time(8, 40, 0, 0, self.timezone))
        application.job_queue.run_daily(self._send_am_tasks, time(8, 50, 0, 0, self.timezone))
        application.job_queue.run_daily(self._send_pm_tasks, time(16, 24, 0, 0, self.timezone))

        logger.info("Tasks module installed")

    async def _clear_day(self, context: ContextTypes.DEFAULT_TYPE):
        self.tasks.clear(datetime.now(self.timezone).date())

    async def _send_am_tasks(self, context: ContextTypes.DEFAULT_TYPE):
        await self._send_tasks(context, 'am')

    async def _send_pm_tasks(self, context: ContextTypes.DEFAULT_TYPE):
        await self._send_tasks(context, 'pm')

    async def _send_tasks(self, context: ContextTypes.DEFAULT_TYPE, ampm: str) -> None:
        today = datetime.now(self.timezone).date()
        task_list = self.tasks.get_task_list(today, ampm)
        list_id = today.strftime('%Y_%m_%d').lower() + '_' + ampm

        announcement = (today.strftime('%A') + ' ' + ampm).upper()

        for target in self.tasks_chats:
            await context.bot.send_message(
                target.chat_id,
                announcement,
                message_thread_id=target.thread_id,
                reply_markup=self._tasks_keyboard(task_list, list_id),
                parse_mode=ParseMode.HTML
            )

    def _tasks_keyboard(self, tasks: list[Task], list_id: str) -> InlineKeyboardMarkup:
        buttons = [[InlineKeyboardButton(TasksModule._format_task(task), callback_data=f"tasks/toggle/{list_id}/{i}")] for i, task in enumerate(tasks)]

        return InlineKeyboardMarkup(buttons)

    async def _toggle(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            args = update.callback_query.data.split('/')
            if len(args) < 4:
                raise UserFriendlyError("There was an error and I could not understand your command. Please try again.")

            list_id = args[2]
            task_id = int(args[3])

            list_id_tokens = list_id.split('_')
            if len(list_id_tokens) < 4:
                raise UserFriendlyError("There was an error and I could not find the task you selected. Please try again.")

            list_date = date(
                year=int(list_id_tokens[0]),
                month=int(list_id_tokens[1]),
                day=int(list_id_tokens[2]),
            )
            task_list = self.tasks.get_task_list(list_date, list_id_tokens[3])
            task_list[task_id] = self.tasks.toggle(task_list[task_id])

            await update.callback_query.answer()
            await update.callback_query.edit_message_text(update.effective_message.text, reply_markup=self._tasks_keyboard(task_list, list_id), parse_mode=ParseMode.HTML)
        except UserFriendlyError as e:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(str(e))
        except Exception as e:
            logger.exception(e)
            # Don't update the message with the exception text because we lose the buttons with the tasks

    @staticmethod
    def _format_task(task: Task) -> str:
        check = '✅' if task.is_done else '⬜️'
        return f"{task.time.strftime('%H:%M')} {check} {task.name}"
