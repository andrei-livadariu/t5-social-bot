import pytz

from typing import Optional
from itertools import groupby
from datetime import time, date

from readerwriterlock import rwlock

from data.repositories.task import TaskRepository
from data.models.task import Task

from integrations.google.handle import Handle
from integrations.google.sheet_database_tasks_table import GoogleSheetDatabaseTasksTable

TaskHandle = Handle[Task]


class GoogleSheetTaskRepository(TaskRepository):
    def __init__(self, table: GoogleSheetDatabaseTasksTable, timezone: pytz.timezone = None):
        self.timezone = timezone

        self.tasks: list[TaskHandle] = []
        self.tasks_by_weekday: dict[int, list[TaskHandle]] = {}

        # The repository data can be read and refreshed from different threads,
        # so any data operation needs to be protected
        self.lock = rwlock.RWLockWrite()

        self._table = table
        self._table.data.subscribe(self._load)

    def get_task_list(self, day: date, ampm: str) -> list[Task]:
        weekday = day.weekday()
        cutoff_time = time(17, 0, 0, 0)

        task_list = self.tasks_by_weekday.get(weekday, [])

        cutoff_point = 0
        for i, task in enumerate(task_list):
            if task.inner.time >= cutoff_time:
                cutoff_point = i
                break

        if ampm == 'am':
            return [task.inner for task in task_list[:cutoff_point]]
        else:
            return [task.inner for task in task_list[cutoff_point:]]

    def toggle(self, task: Task) -> Task:
        new_task = task.copy(is_done=not task.is_done)

        with self.lock.gen_wlock():
            # Only existing tasks can be toggled
            existing = next((handle for handle in self.tasks if handle.inner == task), None)
            if existing:
                existing.inner = new_task

            self._table.toggle(self._to_row(new_task))

        return new_task

    def clear(self, target: date) -> None:
        with self.lock.gen_wlock():
            task_handles = self.tasks_by_weekday.get(target.weekday(), [])
            for handle in task_handles:
                handle.inner = handle.inner.copy(is_done=False)

            self._table.clear(target.weekday())


    def _load(self, raw_data: list[dict[str, str]]) -> None:
        with self.lock.gen_wlock():
            raw_tasks = [self._from_row(row) for row in raw_data]
            self.tasks = [TaskHandle(task) for task in raw_tasks if task]

            sorted_by_weekday = sorted(self.tasks, key=lambda handle: handle.inner.weekday)
            self.tasks_by_weekday = {key: list(group) for key, group in groupby(sorted_by_weekday, key=lambda handle: handle.inner.weekday)}

    def _from_row(self, row: dict[str, str]) -> Optional[Task]:
        # The task name is required because it's used for matching
        name = row.get('name', '').strip()
        if not name:
            return None

        task_time = GoogleSheetTaskRepository._parse_time(row.get('time', '').strip())
        if not task_time:
            return None

        return Task(
            weekday=int(row['weekday']),
            time=task_time,
            name=name,
            is_done=row.get('is_done', '') != ''
        )

    @staticmethod
    def _to_row(task: Task) -> dict[str, str]:
        return {
            'weekday': str(task.weekday),
            'time': task.time.strftime('%H:%M'),
            'name': task.name,
            'is_done': 'x' if task.is_done else ''
        }

    @staticmethod
    def _parse_time(time_string: str) -> Optional[time]:
        try:
            return time.fromisoformat(time_string)
        except ValueError:
            return None
