from contextlib import nullcontext
from typing import TypeVar, Generic, Optional
from abc import ABC, abstractmethod

from readerwriterlock.rwlock import RWLockWrite

ModelType = TypeVar("ModelType")
KeyType = TypeVar("KeyType")
ValueType = TypeVar("ValueType")

StorageType = dict[KeyType, ValueType]

class Index(ABC, Generic[ModelType, KeyType, ValueType]):
    def __init__(self, shared_lock: RWLockWrite = None):
        self._data: StorageType = {}
        self._initialize()

        # The repository data can be read and refreshed from different threads,
        # so any data operation needs to be protected
        if shared_lock:
            self._read_lock = shared_lock
            self._write_lock = None
        else:
            self._read_lock = self._write_lock = RWLockWrite()

    def raw(self) -> StorageType:
        with self._read_lock.gen_rlock():
            return self._data.copy()

    def get(self, key: KeyType) -> Optional[ValueType]:
        with self._read_lock.gen_rlock():
            return self._get_inner(key)

    def get_all(self, keys: list[KeyType]) -> list[ValueType]:
        with self._read_lock.gen_rlock():
            raw_matches = [self._get_inner(key) for key in keys]
            return [match for match in raw_matches if match is not None]

    def get_for_writing(self, key: KeyType) -> Optional[ValueType]:
        with self._write_lock.gen_wlock() if self._write_lock else nullcontext():
            return self._get_inner(key)

    def insert(self, model: ModelType) -> None:
        with self._write_lock.gen_wlock() if self._write_lock else nullcontext():
            self._insert_inner(model)

    def insert_all(self, models: list[ModelType]) -> None:
        with self._write_lock.gen_wlock() if self._write_lock else nullcontext():
            self._insert_all_inner(models)

    def update(self, old: ModelType, new: ModelType) -> None:
        with self._write_lock.gen_wlock() if self._write_lock else nullcontext():
            self._delete_inner(old)
            self._insert_inner(new)

    def update_all(self, changes: list[tuple[ModelType, ModelType]]) -> None:
        old, new = zip(*changes)
        old = list(old)
        new = list(new)
        with self._write_lock.gen_wlock() if self._write_lock else nullcontext():
            self._delete_all_inner(old)
            self._insert_all_inner(new)

    def delete(self, model: ModelType) -> None:
        with self._write_lock.gen_wlock() if self._write_lock else nullcontext():
            self._delete_inner(model)

    def delete_all(self, model: ModelType) -> None:
        with self._write_lock.gen_wlock() if self._write_lock else nullcontext():
            self._delete_all_inner(model)

    def reset(self, initial: list[ModelType] = None) -> None:
        with self._write_lock.gen_wlock() if self._write_lock else nullcontext():
            self._initialize()
            self._insert_all_inner(initial or [])

    def _initialize(self) -> None:
        self._data = {}

    def _get_inner(self, key: KeyType) -> Optional[ValueType]:
        return self._data.get(key)

    @abstractmethod
    def _insert_inner(self, model: ModelType) -> None:
        pass

    def _insert_all_inner(self, models: list[ModelType]) -> None:
        for model in models:
            self._insert_inner(model)

    @abstractmethod
    def _delete_inner(self, model: ModelType) -> None:
        pass

    def _delete_all_inner(self, models: list[ModelType]) -> None:
        for model in models:
            self._delete_inner(model)