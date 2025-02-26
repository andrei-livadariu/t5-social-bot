from typing import TypeVar, Callable, Optional

from readerwriterlock.rwlock import RWLockWrite

from integrations.google.sheets.contracts.index import Index

ModelType = TypeVar("ModelType")
KeyType = TypeVar("KeyType")

class UniqueIndex(
    Index[ModelType, KeyType, ModelType]
):
    def __init__(self, key: Callable[[ModelType], Optional[KeyType]], shared_lock: RWLockWrite = None):
        super().__init__(shared_lock)
        self._key = key

    def _insert_inner(self, model: ModelType) -> None:
        key = self._key(model)
        if key is None:
            return

        self._data[key] = model

    def _delete_inner(self, model: ModelType) -> None:
        key = self._key(model)
        if key is None:
            return

        del self._data[key]