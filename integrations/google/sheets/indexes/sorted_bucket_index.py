import bisect
from contextlib import nullcontext
from typing import TypeVar, Callable, Any, Optional

from readerwriterlock.rwlock import RWLockWrite

from integrations.google.sheets.contracts.index import Index

ModelType = TypeVar("ModelType")
KeyType = TypeVar("KeyType")

KeyPredicate = Callable[[ModelType], Optional[KeyType]]

class SortedBucketIndex(
    Index[ModelType, KeyType, list[ModelType]]
):
    def __init__(self, keys: KeyPredicate | list[KeyPredicate], sorter: Callable[[ModelType], Any], shared_lock: RWLockWrite = None):
        super().__init__(shared_lock)
        self._keys = keys.copy() if isinstance(keys, list) else [keys]
        self._sorter = sorter

    def update_bucket(self, key: KeyType, models: list[ModelType])-> None:
        with self._write_lock.gen_wlock() if self._write_lock else nullcontext():
            self._data[key] = []
            for model in models:
                self._insert_inner(model)

    def _insert_inner(self, model: ModelType)-> None:
        for predicate in self._keys:
            key = predicate(model)
            if key is None:
                return

            if key not in self._data:
                self._data[key] = []

            if model in self._data[key]:
                return

            bisect.insort_left(self._data[key], model, key=self._sorter)

    def _delete_inner(self, model: ModelType) -> None:
        for predicate in self._keys:
            key = predicate(model)
            if key is None:
                return

            if key not in self._data:
                return
            self._data[key].remove(model)

            if len(self._data[key]) == 0:
                del self._data[key]