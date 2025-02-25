from typing import TypeVar, Generic

T = TypeVar("T")
K = TypeVar("K")


# This is an internal, helper class, used in the various indexes
# When we need to replace an immutable object instance we can replace them all at once without rebuilding the indexes
class Handle(Generic[T]):
    def __init__(self, inner: T):
        self.inner: T = inner

    def __eq__(self, other: T):
        return self.inner == other.inner

    def __hash__(self):
        return hash(self.inner)

    @staticmethod
    def unwrap(handle: 'Handle[T]') -> T:
        return handle.inner

    @staticmethod
    def wrap_list(inner_list: list[T]) -> list['Handle[T]']:
        return [Handle(inner) for inner in inner_list]

    @staticmethod
    def unwrap_list(handles: list['Handle[T]']) -> list[T]:
        return [handle.inner for handle in handles]

    @staticmethod
    def wrap_tuple(inner_tuple: tuple[T, ...]) -> tuple['Handle[T]', ...]:
        return tuple(Handle(inner) for inner in inner_tuple)

    @staticmethod
    def unwrap_tuple(handles: tuple['Handle[T]']) -> tuple[T, ...]:
        return tuple(handle.inner for handle in handles)

    @staticmethod
    def wrap_set(inner_set: set[T]) -> set['Handle[T]']:
        return {Handle(inner) for inner in inner_set}

    @staticmethod
    def unwrap_set(handles: set['Handle[T]']) -> set[T]:
        return {handle.inner for handle in handles}

    @staticmethod
    def wrap_dict(inner_dict: dict[K, T]) -> dict[K, 'Handle[T]']:
        return {k: Handle(inner) for k, inner in inner_dict.items()}

    @staticmethod
    def unwrap_dict(handles: dict[K, 'Handle[T]']) -> dict[K, T]:
        return {k: handle.inner for k, handle in handles.items()}
