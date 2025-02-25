from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Dict, Any, Optional

ModelType = TypeVar("ModelType")
KeyType = TypeVar("KeyType")
RowType = TypeVar("RowType", bound=Dict[str, Any])

# This is a mixin that adds insert functionality Google Sheets tables
# The serialization and updating methods must be implemented; a primary key must be declared as well
# Col-row tables already provide some of this functionality
class UpdatableTable(ABC, Generic[ModelType, KeyType, RowType]):
    def update(self, model: ModelType) -> None:
        self.update_all([model])

    def update_all(self, models: list[ModelType]) -> None:
        rows = {self._get_key(model): self._serialize(model) for model in models}
        self.update_partial_all(rows)

    def update_partial(self, key: KeyType, row: RowType, key_name: Optional[str] = None) -> None:
        self.update_partial_all({key: row}, key_name)

    def update_partial_all(self, rows: dict[KeyType, RowType], key_name: Optional[str] = None) -> None:
        self._update_rows(rows, key_name or self._get_key_name())

    def diff(self, a: ModelType, b: ModelType) -> RowType:
        a_row = self._serialize(a)
        b_row = self._serialize(b)
        return {k: v for k, v in b_row.items() if a_row.get(k) != b_row.get(k)}

    @abstractmethod
    def _get_key_name(self) -> str:
        pass

    @abstractmethod
    def _get_key(self, model: ModelType) -> KeyType:
        pass

    @abstractmethod
    def _serialize(self, model: ModelType) -> RowType:
        pass

    @abstractmethod
    def _update_rows(self, rows: dict[KeyType, list[RowType]], key_name: str) -> None:
        pass
