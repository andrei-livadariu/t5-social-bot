import logging
from operator import itemgetter
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING
from datetime import time, datetime
from time import strptime

from gspread import Cell
from gspread.utils import Dimension, ValueInputOption

from data.models.task import Task
from data.models.task_list import TaskList
from data.repositories.task import TaskRepository
from integrations.google.sheets.contracts.tables.readable_table import ReadableTable
from integrations.google.sheets.indexes.unique_index import UniqueIndex

if TYPE_CHECKING:
    from integrations.google.sheets.contracts.database import Database

logger = logging.getLogger(__name__)

group_keys = {'time': 0, 'name': 1, 'is_done': 2}
group_cols = len(group_keys)

# This dataclass adds cell-related information to the Task model,
# which means it's only relevant for this particular integration
@dataclass(frozen=True, kw_only=True)
class TaskEntry(Task):
    row: int
    col: int

    @property
    def cell(self) -> Cell:
        return Cell(self.row, self.col, value='x' if self.is_done else '')


class TasksTable(
    ReadableTable[TaskList],
    TaskRepository
):
    _dimension = Dimension.cols

    def __init__(self, database: 'Database', sheet_name: str):
        super().__init__(database, sheet_name)

        self._by_id = UniqueIndex[TaskList, str](lambda entry: entry.id, self._lock)

    def get_task_list(self, weekday: int, group: str) -> Optional[TaskList]:
        return self._by_id.get(f"{weekday}.{group}")

    def get_next_task_list(self, after: datetime) -> Optional[TaskList]:
        task_lists = list(self._by_id.raw().values())
        if not task_lists:
            return None

        task_lists_by_next_run = [(task_list.next_run(after), task_list) for task_list in task_lists]
        next_entry = min(task_lists_by_next_run, key=itemgetter(0))
        return next_entry[1]

    def toggle(self, task_list: TaskList, position: int) -> TaskList:
        entries = [task for task in task_list.tasks if isinstance(task, TaskEntry)]
        if position >= len(entries):
            raise ValueError('The given position was not found in the list')

        entries[position] = self._toggle_entry(entries[position])

        return task_list.copy(tasks=entries)

    def save(self, task_list: TaskList) -> None:
        with self._lock.gen_wlock():
            existing_list = self._by_id.get_for_writing(task_list.id)
            if not existing_list:
                raise ValueError('Trying to save a new task list - only existing task lists can be saved')

            changes = [(existing_list, task_list)]

            entries = [task for task in task_list.tasks if isinstance(task, TaskEntry)]
            changed_cells = [task.cell for i, task in enumerate(entries) if task.is_done != existing_list.tasks[i].is_done]

            self._update_indexes(changes)
            self._remote_toggle(changed_cells)

    def clear(self, task_list: TaskList) -> TaskList:
        changed_tasks = [self._toggle_entry(task, False) for task in task_list.tasks if isinstance(task, TaskEntry)]
        new_list = task_list.copy(tasks=changed_tasks)
        self.save(new_list)
        return new_list

    def _toggle_entry(self, entry: TaskEntry, value: Optional[bool] = None) -> TaskEntry:
        if value is None:
            value = not entry.is_done

        return entry.copy(is_done=value)

    def _update_indexes(self, changes: list[tuple[TaskList, TaskList]]) -> None:
        for index in self._get_indexes().values():
            index.update_all(changes)

    def _remote_toggle(self, cells: list[Cell]) -> None:
        if not cells:
            return

        try:
            # Load the data from Google
            spreadsheet = self._database.load()
            worksheet = self._load_worksheet(spreadsheet)
            worksheet.update_cells(cells, value_input_option=ValueInputOption.user_entered)
        except Exception as e:
            logger.exception(e)

    def _parse(self, raw: list[list[str]]) -> list[TaskList]:
        # Fill any missing columns at the end with blanks
        incomplete_days = len(raw) % group_cols
        if incomplete_days:
            missing_columns = group_cols - incomplete_days
            for i in range(missing_columns):
                raw.append([''] * len(raw[0]))

        tasks_by_weekday = [self._parse_weekday(raw, start_column) for start_column in range(0, len(raw), group_cols)]
        flat = [task for weekday_tasks in tasks_by_weekday for task in weekday_tasks]
        return flat

    def _parse_weekday(self, raw: list[list[str]], start_column: int) -> list[TaskList]:
        # This method preserves the row numbers from the Google Sheet as they are used for updating
        end_column = start_column + group_cols

        raw_day = list(zip(*raw[start_column:end_column]))
        if not raw_day:
            raise ValueError("The requested weekday column is empty")

        header = raw_day[0]
        weekday = strptime(header[0], '%A').tm_wday
        rows = raw_day[1:]

        task_lists = []
        current_group = None
        current_tasks = []

        last_time = None
        for i, weekday_values in enumerate(rows):
            row = dict(zip(group_keys.keys(), weekday_values))

            # Skip tasks with empty names; this handles the various table headers
            task_name = row.get('name', '').strip()
            task_time = row.get('time', '').strip()

            # Completely empty task - must be an error; skip
            if not task_time and not task_name:
                continue

            parsed_time = TasksTable._parse_time(task_time) if task_time else None

            # Keep any valid time as the last time
            if parsed_time:
                last_time = parsed_time

            if task_name: # Has a name -> regular task
                current_tasks.append(TaskEntry(
                    time=last_time,
                    name=task_name,
                    is_done=row.get('is_done', '') != '',
                    row=i + 1 + 1,  # 1 for the skipped header and 1 because rows are 1-based
                    col=start_column + group_keys['is_done'] + 1,  # Columns are 1-based
                ))
            elif task_time and not parsed_time: # Has the time, but it's not parseable -> group header
                # Push any current tasks to a new task list
                if current_group and current_tasks:
                    task_lists.append(TaskList(
                        weekday=weekday,
                        group=current_group,
                        tasks=current_tasks,
                    ))

                current_group = task_time.lower()
                current_tasks = []
            # Anything else -> skip

        # Group up the remaining tasks
        if current_group and current_tasks:
            task_lists.append(TaskList(
                weekday=weekday,
                group=current_group,
                tasks=current_tasks,
            ))

        return task_lists

    @staticmethod
    def _parse_time(time_string: str) -> Optional[time]:
        try:
            return time.fromisoformat(time_string)
        except ValueError:
            return None
