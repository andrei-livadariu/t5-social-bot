from dataclasses import dataclass, replace, field
from datetime import datetime
from copy import deepcopy
from typing import Optional

from data.models.user_role import UserRole


@dataclass(frozen=True)
class User:
    full_name: str
    aliases: list[str] = field(default_factory=list)
    role: UserRole = UserRole.COMMUNITY
    telegram_username: str = ''
    birthday: Optional[str] = None
    telegram_id: Optional[int] = None
    loyverse_id: Optional[str] = None
    last_private_chat: Optional[datetime] = None
    telegram_blocked: bool = False

    @property
    def first_name(self) -> str:
        return self.full_name.split(' ')[0]

    @property
    def main_alias(self) -> Optional[str]:
        return self.aliases[0] if self.aliases else None

    @property
    def friendly_first_name(self) -> str:
        return self.main_alias or self.first_name

    @property
    def friendly_name(self) -> str:
        return self.friendly_first_name + (f" / @{self.telegram_username}" if self.telegram_username else "")

    @property
    def specific_name(self) -> str:
        name = self.main_alias or self.full_name
        return name + (f" / @{self.telegram_username}" if self.telegram_username else "")

    @property
    def can_contact(self) -> bool:
        return self.telegram_id and self.role != UserRole.INACTIVE and not self.telegram_blocked

    def __eq__(self, other):
        return self.full_name == other.full_name

    def __hash__(self):
        return hash(self.full_name)

    def copy(self, **changes) -> 'User':
        return replace(deepcopy(self), **changes)

