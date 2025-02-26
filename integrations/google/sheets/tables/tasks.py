import logging
from typing import Optional, TYPE_CHECKING
from datetime import date, time

from gspread.utils import Dimension, rowcol_to_a1

from data.models.task import Task
from data.repositories.task import TaskRepository
from integrations.google.sheets.contracts.tables.readable_table import ReadableTable
from integrations.google.sheets.indexes.sorted_bucket_index import SortedBucketIndex

if TYPE_CHECKING:
    from integrations.google.sheets.contracts.database import Database

logger = logging.getLogger(__name__)

weekday_keys = {'time': 0, 'name': 1, 'is_done': 2}
weekday_cols = len(weekday_keys)


class TasksTable(
    ReadableTable[Task],
    TaskRepository
):
    _dimension = Dimension.cols

    def __init__(self, database: 'Database', sheet_name: str):
        super().__init__(database, sheet_name)

        self._by_weekday = SortedBucketIndex(
            key=lambda task: task.weekday,
            sorter=lambda task: task.id,
            shared_lock=self._lock
        )

    def get_task_list(self, day: date, ampm: str) -> list[Task]:
        weekday = day.weekday()
        cutoff_time = time(16, 25, 0, 0)

        task_list = self._by_weekday.get(weekday)

        cutoff_point = 0
        for i, task in enumerate(task_list):
            if task.time >= cutoff_time:
                cutoff_point = i
                break

        if ampm == 'am':
            return task_list[:cutoff_point]
        else:
            return task_list[cutoff_point:]

    def toggle(self, task: Task) -> Task:
        with self._lock.gen_wlock():
            existing_weekday_tasks = self._by_weekday.get_for_writing(task.weekday)
            if not task in existing_weekday_tasks:
                raise ValueError('Unable to find the task. Only existing tasks can be toggled.')

            new_task = task.copy(is_done=not task.is_done)
            self._by_weekday.update(task, new_task)
            self._remote_toggle(new_task)

            return new_task

    def _remote_toggle(self, task: Task) -> None:
        try:
            # Load the data from Google
            spreadsheet = self._database.load()
            worksheet = self._load_worksheet(spreadsheet)

            weekday = task.weekday
            row_number = task.id + 1
            column_number = weekday * weekday_cols + weekday_keys['is_done'] + 1

            worksheet.update_cell(row_number, column_number, 'x' if task.is_done else '')
        except Exception as e:
            logger.exception(e)

    def clear(self, target: date) -> None:
        with self._lock.gen_wlock():
            tasks = self._by_weekday.get_for_writing(target.weekday())
            changes = [(task, task.copy(is_done=False)) for task in tasks]
            self._by_weekday.update_all(changes)
            self._remote_clear(target.weekday())

    def _remote_clear(self, weekday: int) -> None:
        try:
            # Load the data from Google
            spreadsheet = self._database.load()
            worksheet = self._load_worksheet(spreadsheet)

            column_number = weekday * weekday_cols + weekday_keys['is_done'] + 1

            coordinate = rowcol_to_a1(1, column_number)
            column_name = ''.join([i for i in coordinate if not i.isdigit()])

            worksheet.batch_clear([f"{column_name}:{column_name}"])
        except Exception as e:
            logger.exception(e)

    def _parse(self, raw: list[list[str]]) -> list[Task]:
        tasks_by_weekday = [self._parse_weekday(raw, weekday) for weekday in range(0, 7)]
        flat = [task for weekday_tasks in tasks_by_weekday for task in weekday_tasks]
        return flat

    def _parse_weekday(self, raw: list[list], weekday: int) -> list[Task]:
        # This method preserves the row numbers from the Google Sheet as they are used for updating
        start = weekday * weekday_cols
        end = start + weekday_cols

        if len(raw) <= start:
            raise ValueError("The sheet does not contain the requested weekday")

        # Fill any missing columns at the end with blanks
        if len(raw) < end:
            for i in range(len(raw), end):
                raw.append([''] * len(raw[0]))

        raw_today = list(zip(*raw[start:end]))

        tasks = []

        last_time = None
        for i, weekday_values in enumerate(raw_today):
            row = dict(zip(weekday_keys.keys(), weekday_values))

            # Skip tasks with empty names; this handles the various table headers
            name = row.get('name', '').strip()
            if not name:
                continue

            # Fill in the missing time
            task_time = row.get('time', '').strip()
            if task_time:
                last_time = TasksTable._parse_time(task_time)
            if not last_time:
                continue

            tasks.append(Task(
                weekday=weekday,
                id=i,
                time=last_time,
                name=name,
                is_done=row.get('is_done', '') != ''
            ))

        return tasks

    @staticmethod
    def _parse_time(time_string: str) -> Optional[time]:
        try:
            return time.fromisoformat(time_string)
        except ValueError:
            return None