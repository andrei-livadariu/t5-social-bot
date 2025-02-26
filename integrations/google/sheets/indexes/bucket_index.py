from typing import TypeVar, Callable, Optional

from readerwriterlock.rwlock import RWLockWrite

from integrations.google.sheets.contracts.index import Index

ModelType = TypeVar("ModelType")
KeyType = TypeVar("KeyType")

class BucketIndex(
    Index[ModelType, KeyType, set[ModelType]]
):
    def __init__(self, key: Callable[[ModelType], Optional[KeyType]], shared_lock: RWLockWrite = None):
        super().__init__(shared_lock)
        self._key = key

    def _insert_inner(self, model: ModelType) -> None:
        key = self._key(model)
        if key is None:
            return

        if key not in self._data:
            self._data[key] = set()

        self._data[key].add(model)

    def _delete_inner(self, model: ModelType) -> None:
        key = self._key(model)
        if key is None:
            return

        if key not in self._data:
            return

        self._data[key].remove(model)
        if len(self._data[key]) == 0:
            del self._data[key]