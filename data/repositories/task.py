from datetime import date
from abc import ABC, abstractmethod

from data.models.task import Task


class TaskRepository(ABC):
    @abstractmethod
    def get_task_list(self, day: date, ampm: str) -> list[Task]:
        pass

    @abstractmethod
    def toggle(self, task: Task) -> Task:
        pass

    @abstractmethod
    def clear(self, target: date) -> None:
        pass
