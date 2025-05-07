import logging

import pytz
import calendar
from time import strptime
from datetime import datetime, date, timedelta

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import Application, ContextTypes, CallbackQueryHandler, CommandHandler, filters, JobQueue

from data.models.task_list import TaskList
from data.models.task import Task
from data.repositories.task import TaskRepository

from helpers.telegram.exceptions import UserFriendlyError, CommandSyntaxError
from helpers.telegram.chat_target import ChatTarget

from modules.base_module import BaseModule

logger = logging.getLogger(__name__)

HEADS_UP_TIME = timedelta(minutes=15)
POST_TASKS_HELP_TEXT = "To use this command you need to write it like this:\n/post_tasks (time)\nor:\n/post_tasks (weekday) (time)\n\nFor example:\n/post_tasks am\nor:\n/post_tasks monday pm"

class TasksModule(BaseModule):
    def __init__(self, tasks: TaskRepository, tasks_chats: set[ChatTarget], timezone: pytz.timezone = None):
        self.tasks_chats: set[ChatTarget] = tasks_chats.copy()
        self.tasks = tasks
        self.timezone = timezone

    def install(self, application: Application) -> None:
        chat_ids = [target.chat_id for target in self.tasks_chats]

        application.add_handlers([
            # Allow these commands to be issued only from the task chats (ignore the topic ids for now)
            CommandHandler('post_tasks', self._post_tasks, filters.Chat(chat_ids)),
            CallbackQueryHandler(self._toggle, pattern="^tasks/toggle/"),
        ])

        right_now = datetime.now(self.timezone)
        self._schedule_next_post(application.job_queue, right_now)

        logger.info("Tasks module installed")

    async def _post_tasks(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            if len(context.args) == 1:
                weekday = datetime.now(self.timezone).weekday()
                group = context.args[0]
            elif len(context.args) == 2:
                try:
                    weekday = strptime(context.args[0], '%A').tm_wday
                except ValueError:
                    raise UserFriendlyError('I could not understand the weekday you are referring to. Please try again.')

                group = context.args[1]
            else:
                raise CommandSyntaxError()

            task_list = self.tasks.get_task_list(weekday, group)
            if not task_list:
                raise UserFriendlyError('I could not find any tasks for the time period you requested. Please try a different time period.')

            await self._send_tasks(context, task_list)
        except CommandSyntaxError:
            await update.message.reply_text(POST_TASKS_HELP_TEXT)
        except UserFriendlyError as e:
            await update.message.reply_text(str(e))
        except Exception as e:
            logger.exception(e)
            await update.message.reply_text(f"BeeDeeBeeBoop 🤖 Error : {e}")

    async def _send_scheduled_tasks(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        right_now = datetime.now(self.timezone)
        task_list = self.tasks.get_next_task_list(right_now)

        task_list = self.tasks.clear(task_list)
        await self._send_tasks(context, task_list)

        self._schedule_next_post(context.job_queue, task_list.next_run(right_now))

    async def _send_tasks(self, context: ContextTypes.DEFAULT_TYPE, task_list: TaskList) -> None:
        today = datetime.now(self.timezone).date()
        list_id = today.strftime('%Y_%m_%d').lower() + '_' + task_list.group
        announcement = (calendar.day_name[task_list.weekday] + ' ' + task_list.group).upper()

        for target in self.tasks_chats:
            await context.bot.send_message(
                target.chat_id,
                announcement,
                message_thread_id=target.thread_id,
                reply_markup=TasksModule._tasks_keyboard(task_list, list_id),
                parse_mode=ParseMode.HTML
            )

    def _schedule_next_post(self, job_queue: JobQueue, after: datetime) -> None:
        next_list = self.tasks.get_next_task_list(after)
        if not next_list:
            logger.warning("No task list found when attempting to schedule")
            return

        next_post = next_list.next_run(after) - HEADS_UP_TIME
        job_queue.run_once(self._send_scheduled_tasks, when=next_post)

    @staticmethod
    def _tasks_keyboard(task_list: TaskList, list_id: str) -> InlineKeyboardMarkup:
        buttons = [[InlineKeyboardButton(TasksModule._format_task(task), callback_data=f"tasks/toggle/{list_id}/{i}")] for i, task in enumerate(task_list.tasks)]

        return InlineKeyboardMarkup(buttons)

    async def _toggle(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            args = update.callback_query.data.split('/')
            if len(args) < 4:
                raise UserFriendlyError("There was an error and I could not understand your command. Please try again.")

            list_id = args[2]
            task_position = int(args[3])

            list_id_tokens = list_id.split('_')
            if len(list_id_tokens) < 4:
                raise UserFriendlyError("There was an error and I could not find the task you selected. Please try again.")

            list_date = date(
                year=int(list_id_tokens[0]),
                month=int(list_id_tokens[1]),
                day=int(list_id_tokens[2]),
            )
            list_group = list_id_tokens[3]

            task_list = self.tasks.get_task_list(list_date.weekday(), list_group)
            task_list = self.tasks.toggle(task_list, task_position)

            # For a better UX, we first update the message and then save the changes to the repository
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(update.effective_message.text, reply_markup=self._tasks_keyboard(task_list, list_id), parse_mode=ParseMode.HTML)

            self.tasks.save(task_list)
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
