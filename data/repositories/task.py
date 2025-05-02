from abc import ABC, abstractmethod
from typing import Optional
from datetime import datetime

from data.models.task_list import TaskList


class TaskRepository(ABC):
    @abstractmethod
    def get_task_list(self, weekday: int, group: str) -> Optional[TaskList]:
        pass

    @abstractmethod
    def get_next_task_list(self, after: datetime) -> Optional[TaskList]:
        pass

    @abstractmethod
    def toggle(self, task_list: TaskList, position: int) -> TaskList:
        pass

    @abstractmethod
    def save(self, task_list: TaskList) -> TaskList:
        pass

    @abstractmethod
    def clear(self, task_list: TaskList) -> TaskList:
        pass
