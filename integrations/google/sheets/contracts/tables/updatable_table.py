from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Dict, Any, Optional

from readerwriterlock.rwlock import RWLockable

from integrations.google.sheets.contracts.index import Index

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
        if not models:
            return

        index = self._get_key_index()

        with self._get_lock().gen_wlock():
            model_changes = []
            diff_data = {}
            for model in models:
                key = self._get_key(model)

                # Only existing models are saved
                existing = index.get_for_writing(key)
                if not existing:
                    continue

                # Only models with data changes will be saved
                diff = self._diff(existing, model)
                if diff:
                    model_changes.append((existing, model))
                    diff_data[key] = diff

            if model_changes:
                self._update_indexes(model_changes)

            if diff_data:
                # Save changes to the sheets as well
                self._update_rows(diff_data, self._get_key_name())

    def _update_indexes(self, changes: list[tuple[ModelType, ModelType]]) -> None:
        for index in self._get_indexes().values():
            index.update_all(changes)

    def _diff(self, a: ModelType, b: ModelType) -> RowType:
        a_row = self._serialize(a)
        b_row = self._serialize(b)
        return {k: v for k, v in b_row.items() if a_row.get(k) != b_row.get(k)}

    @abstractmethod
    def _get_key_index(self) -> Index:
        pass

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

    @abstractmethod
    def _get_indexes(self) -> dict[str, Index]:
        pass

    @abstractmethod
    def _get_lock(self) -> RWLockable:
        pass