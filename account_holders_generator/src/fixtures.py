from datetime import datetime, timedelta, timezone
from random import randint
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from faker import Faker

from .enums import AccountHolderRewardStatuses
from .polaris.utils import generate_account_number, generate_email

if TYPE_CHECKING:
    from .enums import AccountHolderTypes
    from .polaris.db import AccountHolder, RetailerConfig


fake = Faker(["en-GB"])

ACCOUNT_HOLDER_REWARD_SWITCHER: dict[int, list] = {
    1: [],
    2: [],
    3: [(1, AccountHolderRewardStatuses.ISSUED)],
    4: [
        (1, AccountHolderRewardStatuses.ISSUED),
        (1, AccountHolderRewardStatuses.EXPIRED),
        (1, AccountHolderRewardStatuses.REDEEMED),
        (1, AccountHolderRewardStatuses.CANCELLED),
    ],
    5: [
        (1, AccountHolderRewardStatuses.ISSUED),
        (1, AccountHolderRewardStatuses.EXPIRED),
    ],
    6: [
        (1, AccountHolderRewardStatuses.EXPIRED),
        (1, AccountHolderRewardStatuses.REDEEMED),
    ],
    7: [(1, AccountHolderRewardStatuses.ISSUED)],
    8: [
        (1, AccountHolderRewardStatuses.EXPIRED),
        (2, AccountHolderRewardStatuses.REDEEMED),
    ],
    9: [
        (2, AccountHolderRewardStatuses.ISSUED),
        (2, AccountHolderRewardStatuses.EXPIRED),
    ],
    0: [
        (3, AccountHolderRewardStatuses.ISSUED),
        (3, AccountHolderRewardStatuses.REDEEMED),
    ],
}


def account_holder_payload(
    account_holder_n: int, account_holder_type: "AccountHolderTypes", retailer: "RetailerConfig"
) -> dict:
    return {
        "email": generate_email(account_holder_type, account_holder_n),
        "retailer_id": retailer.id,
        "status": "ACTIVE",
        "account_number": generate_account_number(
            retailer.account_number_prefix, account_holder_type, account_holder_n
        ),
        "is_superuser": False,
        "is_active": True,
    }


def account_holder_profile_payload(account_holder: "AccountHolder") -> dict:
    phone_prefix = "0" if randint(0, 1) else "+44"
    address = fake.street_address().split("\n")
    address_1 = address[0]
    if len(address) > 1:
        address_2 = address[1]
    else:
        address_2 = ""

    return {
        "account_holder_id": account_holder.id,
        "date_of_birth": fake.date(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "phone": phone_prefix + fake.msisdn(),
        "address_line1": address_1,
        "address_line2": address_2,
        "postcode": fake.postcode(),
        "city": fake.city(),
    }


def account_holder_marketing_preference_payload(account_holder: "AccountHolder") -> dict:
    return {
        "account_holder_id": account_holder.id,
        "key_name": "marketing_pref",
        "value": "False",
        "value_type": "BOOLEAN",
    }


def account_holder_reward_payload(
    account_holder_id: int,
    retailer_slug: str,
    reward_uuid: UUID,
    reward_code: str,
    reward_slug: str,
    voucher_status: AccountHolderRewardStatuses,
    issue_date: datetime,
) -> dict:
    now = datetime.now(tz=timezone.utc)

    return {
        "account_holder_id": account_holder_id,
        "retailer_slug": retailer_slug,
        "reward_uuid": reward_uuid,
        "code": reward_code,
        "reward_slug": reward_slug,
        "status": AccountHolderRewardStatuses.ISSUED.value
        if voucher_status == AccountHolderRewardStatuses.EXPIRED
        else voucher_status.value,
        "issued_date": issue_date,
        "expiry_date": now - timedelta(days=randint(2, 10))
        if voucher_status == AccountHolderRewardStatuses.EXPIRED
        else datetime(2030, 1, 1),
        "redeemed_date": now - timedelta(days=randint(2, 10))
        if voucher_status == AccountHolderRewardStatuses.REDEEMED
        else None,
        "cancelled_date": now - timedelta(days=randint(2, 10))
        if voucher_status == AccountHolderRewardStatuses.CANCELLED
        else None,
        "idempotency_token": str(uuid4()),
    }


def reward_payload(reward_uuid: UUID, reward_code: str, voucher_config_id: int, retailer_slug: str) -> dict:
    return {
        "id": reward_uuid,
        "voucher_code": reward_code,
        "voucher_config_id": voucher_config_id,
        "allocated": True,
        "retailer_slug": retailer_slug,
        "deleted": False,
    }
