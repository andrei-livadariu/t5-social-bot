from typing import TypeVar, Callable, Optional

from readerwriterlock.rwlock import RWLockWrite

from integrations.google.sheets.contracts.index import Index

ModelType = TypeVar("ModelType")
KeyType = TypeVar("KeyType")

KeyPredicate = Callable[[ModelType], Optional[KeyType]]

class UniqueIndex(
    Index[ModelType, KeyType, ModelType]
):
    def __init__(self, key: KeyPredicate|list[KeyPredicate], shared_lock: RWLockWrite = None):
        super().__init__(shared_lock)
        self._keys = key.copy() if isinstance(key, list) else [key]

    def _insert_inner(self, model: ModelType) -> None:
        for predicate in self._keys:
            key = predicate(model)
            if key is None:
                return

            self._data[key] = model

    def _delete_inner(self, model: ModelType) -> None:
        for predicate in self._keys:
            key = predicate(model)
            if key is None:
                return

            del self._data[key]