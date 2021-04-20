from enum import Enum


class UserTypes(str, Enum):
    ZERO_BALANCE = "zero_balance"
    INT_BALANCE = "integer_balance"
    FLOAT_BALANCE = "float_balance"

    @property
    def initials(self) -> str:
        return "".join(map(lambda x: x[0], self.upper().split("_")))
