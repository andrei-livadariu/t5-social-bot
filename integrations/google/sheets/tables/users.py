from datetime import datetime, date
from typing import Optional, TYPE_CHECKING, Union

from data.models.user import User
from data.models.user_role import UserRole
from data.repositories.user import UserRepository
from integrations.google.sheets.contracts.tables.col_row_table import ColRowTable
from integrations.google.sheets.contracts.tables.updatable_table import UpdatableTable
from integrations.google.sheets.indexes.bucket_index import BucketIndex
from integrations.google.sheets.indexes.prefix_search_index import PrefixSearchIndex
from integrations.google.sheets.indexes.unique_index import UniqueIndex

if TYPE_CHECKING:
    from integrations.google.sheets.contracts.database import Database


class UsersTable(
    ColRowTable[User],
    UpdatableTable[User, str, dict[str, str]],
    UserRepository,
):
    def __init__(self, database: 'Database', sheet_name: str):
        super().__init__(database, sheet_name)

        self._by_full_name = UniqueIndex[User, str](lambda user: user.full_name, self._lock)
        self._by_telegram_id = UniqueIndex(lambda user: user.telegram_id, self._lock)
        self._by_telegram_name = UniqueIndex(lambda user: user.telegram_username, self._lock)
        self._by_loyverse_id = UniqueIndex(lambda user: user.loyverse_id, self._lock)
        self._by_birthday = BucketIndex(lambda user: user.birthday, self._lock)
        self._by_prefix = PrefixSearchIndex(UsersTable._search_keys, self._lock)

    def get_all(self) -> list[User]:
        return list(self._by_full_name.raw().values())

    def get_by_full_name(self, full_name: str) -> Optional[User]:
        return self._by_full_name.get(full_name)

    def get_by_telegram_id(self, telegram_id: int) -> Optional[User]:
        return self._by_telegram_id.get(telegram_id)

    def get_by_telegram_name(self, telegram_name: str) -> Optional[User]:
        return self._by_telegram_name.get(telegram_name)

    def get_by_birthday(self, birthday: Union[str, date, datetime]) -> set[User]:
        date_string = birthday if isinstance(birthday, str) else birthday.strftime('%m-%d')
        return self._by_birthday.get(date_string)

    def get_by_loyverse_id(self, loyverse_id: str) -> Optional[User]:
        return self._by_loyverse_id.get(loyverse_id)

    def search(self, query: str) -> set[User]:
        return self._by_prefix.get(query.lower())

    def save(self, user: User) -> None:
        self.update(user)

    def save_all(self, models: list[User]) -> None:
        self.update_all(models)

    def _get_key_name(self) -> str:
        return 'full_name'

    def _get_key(self, model: User) -> str:
        return model.full_name

    def _get_key_index(self) -> UniqueIndex:
        return self._by_full_name

    def _serialize(self, model: User) -> dict[str, str]:
        return {
            'full_name': model.full_name,
            'aliases': ','.join(model.aliases),
            'role': model.role.value.capitalize(),
            'telegram_username': model.telegram_username,
            'birthday': model.birthday,
            'telegram_id': model.telegram_id,
            'loyverse_id': model.loyverse_id,
            'last_private_chat': self._database.to_datetime_string(model.last_private_chat),
            'telegram_blocked': 'BLOCKED' if model.telegram_blocked else '',
        }

    def _deserialize(self, row: dict[str, str]) -> Optional[User]:
        # The full name is the primary key so it's required
        full_name = row.get('full_name', '').strip()
        if not full_name:
            return None

        return User(
            full_name=full_name,
            aliases=UsersTable._parse_aliases(row.get('aliases', '')),
            role=UsersTable._parse_user_role(row.get('role', '')),
            telegram_username=row.get('telegram_username', '').strip(),
            birthday=row.get('birthday', ''),
            telegram_id=UsersTable._parse_int(row.get('telegram_id', '')),
            loyverse_id=row.get('loyverse_id', '').strip(),
            last_private_chat=self._database.from_datetime_string(row.get('last_private_chat', '')),
            telegram_blocked=row.get('telegram_blocked', '').strip() != '',
        )

    @staticmethod
    def _parse_int(int_string: str) -> Optional[int]:
        try:
            return int(int_string.strip())
        except ValueError:
            return None

    @staticmethod
    def _parse_aliases(alias_string: str) -> list[str]:
        clean = [alias.strip() for alias in alias_string.split(',')]
        return [alias for alias in clean if alias]

    @staticmethod
    def _parse_user_role(user_role_string: str) -> UserRole:
        try:
            return UserRole(user_role_string.strip().lower())
        except ValueError:
            return UserRole.COMMUNITY

    @staticmethod
    def _search_keys(user: User) -> list[str]:
        keys = []

        # Complete telegram username
        if user.telegram_username:
            keys.append(user.telegram_username.lower())

        # Complete alias list
        for alias in user.aliases:
            keys.append(alias.lower())

        # First name from full name
        keys.append(user.first_name.lower())
        # Complete full name
        keys.append(user.full_name.lower())

        return keys