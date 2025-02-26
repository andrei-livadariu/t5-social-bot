from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from readerwriterlock.rwlock import RWLockable

from integrations.google.sheets.contracts.index import Index

ModelType = TypeVar("ModelType")
RowType = TypeVar("RowType")

# This is a mixin that adds insert functionality Google Sheets tables
# The serialization and insertion methods must be implemented
# Col-row tables already provide some of this functionality
class InsertableTable(ABC, Generic[ModelType, RowType]):
    def insert(self, model: ModelType) -> None:
        self.insert_all([model])

    def insert_all(self, models: list[ModelType]) -> None:
        rows = [self._serialize(model) for model in models]
        with self._get_lock().gen_wlock():
            self._insert_rows(rows)
            self._add_to_indexes(models)

    def _add_to_indexes(self, models: list[ModelType]) -> None:
        for index in self._get_indexes().values():
            index.insert_all(models)

    @abstractmethod
    def _serialize(self, model: ModelType) -> RowType:
        pass

    @abstractmethod
    def _insert_rows(self, rows: list[RowType]) -> None:
        pass

    @abstractmethod
    def _get_indexes(self) -> dict[str, Index]:
        pass

    @abstractmethod
    def _get_lock(self) -> RWLockable:
        pass