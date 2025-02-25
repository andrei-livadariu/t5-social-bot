from abc import ABC, abstractmethod
from typing import TypeVar, Generic

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
        self._insert_rows(rows)

    @abstractmethod
    def _serialize(self, model: ModelType) -> RowType:
        pass

    @abstractmethod
    def _insert_rows(self, rows: list[RowType]) -> None:
        pass
