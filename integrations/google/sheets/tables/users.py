from typing import Optional

from data.models.user import User
from data.models.user_role import UserRole
from integrations.google.sheets.contracts.tables.col_row_table import ColRowTable
from integrations.google.sheets.contracts.tables.updatable_table import UpdatableTable


class UsersTable(
    ColRowTable[User],
    UpdatableTable[User, str, dict[str, str]]
):
    def _get_key_name(self) -> str:
        return 'full_name'

    def _get_key(self, model: User) -> str:
        return model.full_name

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
            'last_visit': self._database.to_datetime_string(model.last_visit),
            'recent_visits': model.recent_visits,
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
            last_visit=self._database.from_datetime_string(row.get('last_visit', '')),
            recent_visits=UsersTable._parse_int(row.get('recent_visits', '')) or 0,
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
            return UserRole.CHAMPION