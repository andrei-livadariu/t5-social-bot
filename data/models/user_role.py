from enum import Enum, unique


@unique
class UserRole(Enum):
    CHAMPION = "champion"
    COMMUNITY = "community"
    SUPPORT = "support"
    MANAGER = "manager"
    ALUMNI = "alumni"
    INACTIVE = "inactive"

    @property
    def is_staff(self) -> bool:
        return self in {UserRole.SUPPORT, UserRole.MANAGER}
