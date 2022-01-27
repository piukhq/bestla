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
    reward_status: AccountHolderRewardStatuses,
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
        if reward_status == AccountHolderRewardStatuses.EXPIRED
        else reward_status.value,
        "issued_date": issue_date,
        "expiry_date": now - timedelta(days=randint(2, 10))
        if reward_status == AccountHolderRewardStatuses.EXPIRED
        else datetime(2030, 1, 1),
        "redeemed_date": now - timedelta(days=randint(2, 10))
        if reward_status == AccountHolderRewardStatuses.REDEEMED
        else None,
        "cancelled_date": now - timedelta(days=randint(2, 10))
        if reward_status == AccountHolderRewardStatuses.CANCELLED
        else None,
        "idempotency_token": str(uuid4()),
    }


def reward_payload(reward_uuid: UUID, reward_code: str, reward_config_id: int, retailer_slug: str) -> dict:
    return {
        "id": reward_uuid,
        "code": reward_code,
        "reward_config_id": reward_config_id,
        "allocated": True,
        "retailer_slug": retailer_slug,
        "deleted": False,
    }


def retailer_config_payload(retailer_slug: str) -> dict:
    retailer_name = retailer_slug.replace("-", " ").title()
    return {
        "name": retailer_name,
        "slug": retailer_slug,
        "account_number_prefix": "RTST",
        "profile_config": (
            "email:"
            "\n  required: true"
            "\nfirst_name:"
            "\n  required: true"
            "\nlast_name:"
            "\n  required: true"
            "\ndate_of_birth:"
            "\n  required: true"
            "\nphone:"
            "\n  required: true"
            "\naddress_line1:"
            "\n  required: true"
            "\naddress_line2:"
            "\n  required: true"
            "\npostcode:"
            "\n  required: true"
            "\ncity:"
            "\n  required: true"
        ),
        "marketing_preference_config": "marketing_pref:\n  type: boolean\n  label: Sample Question?",
        "loyalty_name": retailer_name,
        "email_header_image": f"{retailer_slug}-banner.png",
        "welcome_email_from": f"{retailer_name} <no-reply@test.com>",
        "welcome_email_subject": f"Welcome to {retailer_name}!",
    }


def campaign_payload(retailer_id: int, campaign_slug: str) -> dict:
    return {
        "retailer_id": retailer_id,
        "status": "ACTIVE",
        "name": campaign_slug.replace("-", " ").title(),
        "slug": campaign_slug,
        "start_date": datetime.now(tz=timezone.utc) - timedelta(minutes=5),
        "earn_inc_is_tx_value": False,
    }


def reward_rule_payload(campaign_id: int, reward_slug: str) -> dict:
    return {
        "campaign_id": campaign_id,
        "reward_slug": reward_slug,
        "reward_goal": 200,
    }


def earn_rule_payload(campaign_id: int) -> dict:
    return {
        "campaign_id": campaign_id,
        "threshold": 500,
        "increment": 300,
        "increment_multiplier": 1.25,
    }


def reward_config_payload(retailer_slug: str, reward_slug: str) -> dict:
    return {
        "reward_slug": reward_slug,
        "validity_days": 15,
        "retailer_slug": retailer_slug,
        "status": "ACTIVE",
        "fetch_type": "PRE_LOADED",
    }