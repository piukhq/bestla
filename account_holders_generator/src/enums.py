from enum import Enum


class AccountHolderTypes(str, Enum):
    ZERO_BALANCE = "zero_balance"
    INT_BALANCE = "integer_balance"
    FLOAT_BALANCE = "float_balance"

    @property
    def account_holder_type_index(self) -> str:
        account_holder_types = list(AccountHolderTypes)
        return f"0{account_holder_types.index(self)}"


class AccountHolderVoucherStatuses(Enum):
    ISSUED = "ISSUED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REDEEMED = "REDEEMED"
