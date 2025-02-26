from typing import TypeVar, Callable, Optional

from readerwriterlock.rwlock import RWLockWrite

from integrations.google.sheets.contracts.index import Index, ValueType

ModelType = TypeVar("ModelType")

# Prefix search functionality
# E.g. The entry for Alex will match Alex Uzan, Alexandru Ivanciu, and Alexandra Tudor
# The implementation is naive and is suboptimal for updates, so bulk operations are preferred
class PrefixSearchIndex(
    Index[ModelType, str, set[ModelType]]
):
    def __init__(self, keys: Callable[[ModelType], list[str]], shared_lock: RWLockWrite = None):
        self._unique: dict[str, set[ModelType]] = {}
        super().__init__(shared_lock)
        self._keys = keys

    def _initialize(self) -> None:
        super()._initialize()
        self._unique = {}

    def _get_inner(self, key: str) -> Optional[ValueType]:
        # An exact match is a successful prefix search; this is usually what we want
        # No exact matches -> do a full search
        return self._exact_match(key) or self._partial_match(key)

    def _exact_match(self, query: str) -> set[ModelType]:
        return self._data.get(query, set())

    def _partial_match(self, query: str) -> set[ModelType]:
        results: set[ModelType] = set()
        for key, handles in self._data.items():
            if query in key:
                results |= handles

        return results

    def _insert_inner(self, model: ModelType) -> None:
        self._insert_all_inner([model])

    def _insert_all_inner(self, models: list[ModelType]) -> None:
        for model in models:
            for key in [key for key in self._keys(model) if key]:
                if key not in self._data:
                    self._unique[key] = set()
                self._unique[key].add(model)

        self._merge_search_prefixes()

    def _delete_inner(self, model: ModelType) -> None:
        self._delete_all_inner([model])

    def _delete_all_inner(self, models: list[ModelType]) -> None:
        for model in models:
            for key in [key for key in self._keys(model) if key]:
                if key not in self._data:
                    continue
                self._unique[key].remove(model)
                if len(self._unique[key]) == 0:
                    del self._unique[key]

        self._merge_search_prefixes()

    def _merge_search_prefixes(self) -> None:
        self._data = self._unique.copy()
        sorted_keys = list(self._data.keys())
        sorted_keys.sort()

        prefix_i = 0
        current_i = 1

        while current_i < len(sorted_keys):
            prefix = sorted_keys[prefix_i]
            key = sorted_keys[current_i]
            if key.startswith(prefix):
                self._data[prefix] |= self._data[key]
                current_i = current_i + 1
            else:
                prefix_i = prefix_i + 1
                current_i = prefix_i + 1
