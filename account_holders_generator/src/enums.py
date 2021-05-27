from enum import Enum
from typing import List


class UserTypes(str, Enum):
    ZERO_BALANCE = "zero_balance"
    INT_BALANCE = "integer_balance"
    FLOAT_BALANCE = "float_balance"

    @property
    def initials(self) -> str:
        user_types: List = list(UserTypes)
        initials = f"0{user_types.index(self)}"

        return initials
