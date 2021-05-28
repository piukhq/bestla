from enum import Enum


class UserTypes(str, Enum):
    ZERO_BALANCE = "zero_balance"
    INT_BALANCE = "integer_balance"
    FLOAT_BALANCE = "float_balance"

    @property
    def user_type_index(self) -> str:
        user_types = list(UserTypes)
        return f"0{user_types.index(self)}"
