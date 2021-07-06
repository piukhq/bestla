from datetime import datetime, timedelta
from random import randint
from typing import TYPE_CHECKING, List, Tuple, Union
from uuid import uuid4

import click

from faker import Faker
from hashids import Hashids
from progressbar import ProgressBar

from .db.polaris import AccountHolder, AccountHolderProfile, RetailerConfig, UserVoucher
from .db.polaris import load_models as load_polaris_models
from .db.vela import Campaign, RetailerRewards
from .db.vela import load_models as load_vela_models
from .enums import UserTypes, UserVoucherStatuses

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


BATCH_SIZE = 1000
fake = Faker(["en-GB"])


def _generate_account_number(prefix: str, user_type: UserTypes, user_n: int) -> str:
    user_n_str = str(user_n)
    return prefix + user_type.user_type_index + "0" * (8 - len(user_n_str)) + user_n_str


def _generate_balance(campaign: str, user_type: UserTypes, max_val: int) -> dict:

    if user_type == UserTypes.ZERO_BALANCE:
        value = 0
    else:
        value = randint(1, max_val) * 100
        if user_type == UserTypes.FLOAT_BALANCE:
            value += randint(1, 99)

    return {
        "value": value,
        "campaign_slug": campaign,
    }


def _create_user_vouchers(
    user_n: Union[int, str], account_holder: AccountHolder, batch_voucher_salt: str
) -> List[UserVoucher]:
    hashids = Hashids(batch_voucher_salt, min_length=15)

    def _make_vouchers(vouchers_required: List[Tuple[int, UserVoucherStatuses]]) -> list[UserVoucher]:
        vouchers: list[UserVoucher] = []
        for i, (how_many, voucher_status) in enumerate(vouchers_required):
            issue_date = datetime.utcnow() - timedelta(days=14)
            for j in range(how_many):
                vouchers.append(
                    UserVoucher(
                        account_holder_id=str(account_holder.id),
                        voucher_code=hashids.encode(i, j, user_n),
                        voucher_type_slug="accumulator",
                        status=voucher_status.value,
                        issued_date=issue_date,
                        expiry_date=datetime.utcnow() - timedelta(days=randint(2, 10))
                        if voucher_status == UserVoucherStatuses.EXPIRED
                        else datetime(2030, 1, 1),
                        redeemed_date=datetime.utcnow() - timedelta(days=randint(2, 10))
                        if voucher_status == UserVoucherStatuses.REDEEMED
                        else None,
                        cancelled_date=datetime.utcnow() - timedelta(days=randint(2, 10))
                        if voucher_status == UserVoucherStatuses.CANCELLED
                        else None,
                    )
                )
        return vouchers

    user_voucher_type = int(user_n) % 10
    switcher: dict[int, List] = {
        1: [],
        2: [],
        3: [(1, UserVoucherStatuses.ISSUED)],
        4: [
            (1, UserVoucherStatuses.ISSUED),
            (1, UserVoucherStatuses.EXPIRED),
            (1, UserVoucherStatuses.REDEEMED),
            (1, UserVoucherStatuses.CANCELLED),
        ],
        5: [
            (1, UserVoucherStatuses.ISSUED),
            (1, UserVoucherStatuses.EXPIRED),
        ],
        6: [
            (1, UserVoucherStatuses.EXPIRED),
            (1, UserVoucherStatuses.REDEEMED),
        ],
        7: [(1, UserVoucherStatuses.ISSUED)],
        8: [
            (1, UserVoucherStatuses.EXPIRED),
            (2, UserVoucherStatuses.REDEEMED),
        ],
        9: [
            (2, UserVoucherStatuses.ISSUED),
            (2, UserVoucherStatuses.EXPIRED),
        ],
        0: [
            (3, UserVoucherStatuses.ISSUED),
            (3, UserVoucherStatuses.REDEEMED),
        ],
    }
    return _make_vouchers(switcher[user_voucher_type])


def _generate_balances(active_campaigns: List[str], user_type: UserTypes, max_val: int) -> dict:
    return {campaign: _generate_balance(campaign, user_type, max_val) for campaign in active_campaigns}


def _generate_email(user_type: UserTypes, user_n: Union[int, str]) -> str:
    user_n = str(user_n).rjust(2, "0")
    return f"test_{user_type.value}_user_{user_n}@autogen.bpl"


def _clear_existing_account_holders(db_session: "Session", retailer_id: int) -> None:
    db_session.query(AccountHolder).filter(
        AccountHolder.email.like(r"test_%_user_%@autogen.bpl"), AccountHolder.retailer_id == retailer_id
    ).delete(synchronize_session=False)


def _account_holder_payload(
    user_n: int, user_type: UserTypes, retailer: RetailerConfig, active_campaigns: List[str], max_val: int
) -> dict:
    return {
        "id": uuid4(),
        "email": _generate_email(user_type, user_n),
        "retailer_id": retailer.id,
        "status": "ACTIVE",
        "account_number": _generate_account_number(retailer.account_number_prefix, user_type, user_n),
        "is_superuser": False,
        "is_active": True,
        "current_balances": _generate_balances(active_campaigns, user_type, max_val),
    }


def _account_holder_profile_payload(account_holder: AccountHolder) -> dict:
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


def _get_retailer_by_slug(db_session: "Session", retailer_slug: str) -> RetailerConfig:
    retailer = db_session.query(RetailerConfig).filter_by(slug=retailer_slug).first()
    if not retailer:
        click.echo("requested retailer [%s] does not exists in DB.")
        exit(-1)

    return retailer


def _batch_create_account_holders(
    *,
    db_session: "Session",
    batch_start: int,
    batch_end: int,
    user_type: UserTypes,
    retailer: RetailerConfig,
    active_campaigns: List[str],
    max_val: int,
    bar: ProgressBar,
    progress_counter: int,
    user_type_voucher_code_salt: str,
) -> int:

    account_holders_batch = []
    account_holders_profile_batch = []
    user_voucher_batch = []
    for i in range(batch_start, batch_end, -1):
        account_holder = AccountHolder(**_account_holder_payload(i, user_type, retailer, active_campaigns, max_val))
        account_holders_batch.append(account_holder)
        account_holders_profile_batch.append(AccountHolderProfile(**_account_holder_profile_payload(account_holder)))
        user_voucher_batch.extend(_create_user_vouchers(i, account_holder, user_type_voucher_code_salt))
        progress_counter += 1
        bar.update(progress_counter)

    db_session.bulk_save_objects(account_holders_batch)
    db_session.bulk_save_objects(account_holders_profile_batch)
    db_session.bulk_save_objects(user_voucher_batch)
    db_session.commit()

    return progress_counter


def get_active_campaigns(db_session: "Session", retailer: RetailerConfig, campaign_default: str) -> List[str]:
    campaigns = (
        db_session.query(Campaign.slug)
        .join(RetailerRewards)
        .filter(
            RetailerRewards.slug == retailer.slug,
            Campaign.status == "ACTIVE",
        )
        .all()
    )
    if not campaigns:
        return [campaign_default]
    else:
        return [campaign[0] for campaign in campaigns]


def generate_account_holders(
    ah_to_create: int, retailer_slug: str, campaign: str, max_val: int, polaris_db_uri: str, vela_db_uri: str
) -> None:

    polaris_db_session = load_polaris_models(polaris_db_uri)
    vela_db_session = load_vela_models(vela_db_uri)

    try:
        retailer = _get_retailer_by_slug(polaris_db_session, retailer_slug)
        click.echo("Selected retailer: %s" % retailer.name)
        active_campaigns = get_active_campaigns(vela_db_session, retailer, campaign)
        click.echo("Selected campaign %s." % campaign)
        click.echo("Deleting previously generated account holders for requested retailer.")
        _clear_existing_account_holders(polaris_db_session, retailer.id)

        for user_type in UserTypes:
            click.echo("\ncreating %s users." % user_type.value)
            batch_start = ah_to_create
            progress_counter = 0

            with ProgressBar(max_value=ah_to_create) as bar:
                while batch_start > 0:

                    if batch_start <= BATCH_SIZE:
                        batch_end = 0
                    else:
                        batch_end = batch_start - BATCH_SIZE

                    progress_counter = _batch_create_account_holders(
                        db_session=polaris_db_session,
                        batch_start=batch_start,
                        batch_end=batch_end,
                        user_type=user_type,
                        retailer=retailer,
                        active_campaigns=active_campaigns,
                        max_val=max_val,
                        bar=bar,
                        progress_counter=progress_counter,
                        user_type_voucher_code_salt=str(uuid4()),  # to stop hashid clashes
                    )
                    batch_start = batch_end
    finally:
        polaris_db_session.close()
        vela_db_session.close()
