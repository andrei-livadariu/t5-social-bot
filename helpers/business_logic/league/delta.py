from abc import ABC
from typing import Generic, TypeVar

T = TypeVar("T")

class Delta(ABC, Generic[T]):
    def __init__(self, before: T, after: T):
        self.before = before
        self.after = after
        self.delta = after - before
