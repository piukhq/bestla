import sys

from datetime import datetime, timedelta
from random import randint
from typing import TYPE_CHECKING, List, Tuple, Union
from uuid import uuid4

import click

from faker import Faker
from hashids import Hashids
from progressbar import ProgressBar

from .db.carina import Voucher, VoucherConfig
from .db.carina import load_models as load_carina_models
from .db.polaris import (
    AccountHolder,
    AccountHolderCampaignBalance,
    AccountHolderProfile,
    AccountHolderVoucher,
    RetailerConfig,
)
from .db.polaris import load_models as load_polaris_models
from .db.vela import Campaign, RetailerRewards
from .db.vela import load_models as load_vela_models
from .enums import AccountHolderTypes, AccountHolderVoucherStatuses

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


BATCH_SIZE = 1000
fake = Faker(["en-GB"])


def _generate_account_number(prefix: str, account_holder_type: AccountHolderTypes, account_holder_n: int) -> str:
    account_holder_n_str = str(account_holder_n)
    return (
        prefix
        + account_holder_type.account_holder_type_index
        + "0" * (8 - len(account_holder_n_str))
        + account_holder_n_str
    )


def _generate_balance(account_holder_type: AccountHolderTypes, max_val: int) -> int:

    if account_holder_type == AccountHolderTypes.ZERO_BALANCE:
        value = 0
    else:
        value = randint(1, max_val) * 100
        if account_holder_type == AccountHolderTypes.FLOAT_BALANCE:
            value += randint(1, 99)

    return value


def _create_unallocated_vouchers(
    unallocated_vouchers_to_create: int, batch_voucher_salt: str, voucher_config: VoucherConfig, retailer_slug: str
) -> list[Voucher]:
    hashids = Hashids(batch_voucher_salt, min_length=15)
    unallocated_vouchers = []
    for i in range(unallocated_vouchers_to_create):
        voucher_code = (hashids.encode(i),)
        unallocated_vouchers.append(
            Voucher(
                voucher_code=voucher_code,
                voucher_config_id=voucher_config.id,
                allocated=False,
                retailer_slug=retailer_slug,
                deleted=False,
            )
        )

    return unallocated_vouchers


def _create_account_holder_vouchers(
    account_holder_n: Union[int, str],
    account_holder: AccountHolder,
    batch_voucher_salt: str,
    voucher_config: VoucherConfig,
    retailer: RetailerConfig,
) -> tuple[list[Voucher], list[AccountHolderVoucher]]:
    hashids = Hashids(batch_voucher_salt, min_length=15)

    def _make_vouchers(
        vouchers_required: List[Tuple[int, AccountHolderVoucherStatuses]]
    ) -> tuple[list[Voucher], list[AccountHolderVoucher]]:
        account_holder_vouchers: list[AccountHolderVoucher] = []
        vouchers: list[Voucher] = []
        for i, (how_many, voucher_status) in enumerate(vouchers_required):
            issue_date = datetime.utcnow() - timedelta(days=14)
            for j in range(how_many):
                voucher_id = uuid4()
                voucher_code = (hashids.encode(i, j, account_holder_n),)
                voucher_type_slug = voucher_config.voucher_type_slug
                vouchers.append(
                    Voucher(
                        id=voucher_id,
                        voucher_code=voucher_code,
                        voucher_config_id=voucher_config.id,
                        allocated=True,
                        retailer_slug=retailer.slug,
                        deleted=False,
                    )
                )
                account_holder_vouchers.append(
                    AccountHolderVoucher(
                        account_holder_id=str(account_holder.id),
                        retailer_slug=retailer.slug,
                        voucher_id=voucher_id,
                        voucher_code=voucher_code,
                        voucher_type_slug=voucher_type_slug,
                        status=AccountHolderVoucherStatuses.ISSUED.value
                        if voucher_status == AccountHolderVoucherStatuses.EXPIRED
                        else voucher_status.value,
                        issued_date=issue_date,
                        expiry_date=datetime.utcnow() - timedelta(days=randint(2, 10))
                        if voucher_status == AccountHolderVoucherStatuses.EXPIRED
                        else datetime(2030, 1, 1),
                        redeemed_date=datetime.utcnow() - timedelta(days=randint(2, 10))
                        if voucher_status == AccountHolderVoucherStatuses.REDEEMED
                        else None,
                        cancelled_date=datetime.utcnow() - timedelta(days=randint(2, 10))
                        if voucher_status == AccountHolderVoucherStatuses.CANCELLED
                        else None,
                        idempotency_token=uuid4(),
                    )
                )
        return vouchers, account_holder_vouchers

    account_holder_voucher_type = int(account_holder_n) % 10
    switcher: dict[int, List] = {
        1: [],
        2: [],
        3: [(1, AccountHolderVoucherStatuses.ISSUED)],
        4: [
            (1, AccountHolderVoucherStatuses.ISSUED),
            (1, AccountHolderVoucherStatuses.EXPIRED),
            (1, AccountHolderVoucherStatuses.REDEEMED),
            (1, AccountHolderVoucherStatuses.CANCELLED),
        ],
        5: [
            (1, AccountHolderVoucherStatuses.ISSUED),
            (1, AccountHolderVoucherStatuses.EXPIRED),
        ],
        6: [
            (1, AccountHolderVoucherStatuses.EXPIRED),
            (1, AccountHolderVoucherStatuses.REDEEMED),
        ],
        7: [(1, AccountHolderVoucherStatuses.ISSUED)],
        8: [
            (1, AccountHolderVoucherStatuses.EXPIRED),
            (2, AccountHolderVoucherStatuses.REDEEMED),
        ],
        9: [
            (2, AccountHolderVoucherStatuses.ISSUED),
            (2, AccountHolderVoucherStatuses.EXPIRED),
        ],
        0: [
            (3, AccountHolderVoucherStatuses.ISSUED),
            (3, AccountHolderVoucherStatuses.REDEEMED),
        ],
    }
    return _make_vouchers(switcher[account_holder_voucher_type])


def _generate_account_holder_campaign_balances(
    account_holder: AccountHolder, active_campaigns: List[str], account_holder_type: AccountHolderTypes, max_val: int
) -> List[AccountHolderCampaignBalance]:
    return [
        AccountHolderCampaignBalance(
            account_holder_id=account_holder.id,
            campaign_slug=campaign,
            balance=_generate_balance(account_holder_type, max_val),
        )
        for campaign in active_campaigns
    ]


def _generate_email(account_holder_type: AccountHolderTypes, account_holder_n: Union[int, str]) -> str:
    account_holder_n = str(account_holder_n).rjust(2, "0")
    return f"test_{account_holder_type.value}_user_{account_holder_n}@autogen.bpl"


def _clear_existing_account_holders(db_session: "Session", retailer_id: int) -> None:
    db_session.query(AccountHolder).filter(
        AccountHolder.email.like(r"test_%_user_%@autogen.bpl"), AccountHolder.retailer_id == retailer_id
    ).delete(synchronize_session=False)


def _account_holder_payload(
    account_holder_n: int, account_holder_type: AccountHolderTypes, retailer: RetailerConfig
) -> dict:
    return {
        "id": uuid4(),
        "email": _generate_email(account_holder_type, account_holder_n),
        "retailer_id": retailer.id,
        "status": "ACTIVE",
        "account_number": _generate_account_number(
            retailer.account_number_prefix, account_holder_type, account_holder_n
        ),
        "is_superuser": False,
        "is_active": True,
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
        sys.exit(-1)

    return retailer


def _get_voucher_config_by_retailer(db_session: "Session", retailer_slug: str) -> VoucherConfig:
    voucher_config = db_session.query(VoucherConfig).filter_by(retailer_slug=retailer_slug).first()
    if not voucher_config:
        click.echo(f"No voucher config found for retailer: {retailer_slug}")
        sys.exit(-1)

    return voucher_config


def _batch_create_account_holders(
    *,
    polaris_db_session: "Session",
    carina_db_session: "Session",
    batch_start: int,
    batch_end: int,
    account_holder_type: AccountHolderTypes,
    retailer: RetailerConfig,
    active_campaigns: List[str],
    max_val: int,
    bar: ProgressBar,
    progress_counter: int,
    account_holder_type_voucher_code_salt: str,
    voucher_config: VoucherConfig,
) -> int:

    account_holders_batch = []
    account_holders_profile_batch = []
    account_holder_balance_batch = []
    account_holder_voucher_batch = []
    voucher_batch = []
    for i in range(batch_start, batch_end, -1):
        account_holder = AccountHolder(**_account_holder_payload(i, account_holder_type, retailer))
        account_holders_batch.append(account_holder)
        account_holder_balance_batch.extend(
            _generate_account_holder_campaign_balances(account_holder, active_campaigns, account_holder_type, max_val)
        )
        account_holders_profile_batch.append(AccountHolderProfile(**_account_holder_profile_payload(account_holder)))
        vouchers, account_holder_vouchers = _create_account_holder_vouchers(
            i, account_holder, account_holder_type_voucher_code_salt, voucher_config, retailer
        )
        voucher_batch.extend(vouchers)
        account_holder_voucher_batch.extend(account_holder_vouchers)
        progress_counter += 1
        bar.update(progress_counter)

    polaris_db_session.bulk_save_objects(account_holders_batch)
    polaris_db_session.bulk_save_objects(account_holders_profile_batch)
    carina_db_session.bulk_save_objects(voucher_batch)
    carina_db_session.commit()

    polaris_db_session.bulk_save_objects(account_holder_voucher_batch)
    polaris_db_session.bulk_save_objects(account_holder_balance_batch)
    polaris_db_session.commit()

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
    ah_to_create: int,
    retailer_slug: str,
    campaign: str,
    max_val: int,
    polaris_db_uri: str,
    vela_db_uri: str,
    carina_db_uri: str,
    unallocated_vouchers_to_create: int,
) -> None:

    carina_db_session = load_carina_models(carina_db_uri)
    polaris_db_session = load_polaris_models(polaris_db_uri)
    vela_db_session = load_vela_models(vela_db_uri)

    try:
        retailer = _get_retailer_by_slug(polaris_db_session, retailer_slug)
        click.echo("Selected retailer: %s" % retailer.name)
        voucher_config = _get_voucher_config_by_retailer(carina_db_session, retailer_slug)
        click.echo(f"Voucher type slug for {retailer.name}: {voucher_config.voucher_type_slug}")
        active_campaigns = get_active_campaigns(vela_db_session, retailer, campaign)
        click.echo("Selected campaign %s." % campaign)
        click.echo("Deleting previously generated account holders for requested retailer.")
        _clear_existing_account_holders(polaris_db_session, retailer.id)
        unallocated_voucher_batch = _create_unallocated_vouchers(
            unallocated_vouchers_to_create=unallocated_vouchers_to_create,
            batch_voucher_salt=str(uuid4()),
            voucher_config=voucher_config,
            retailer_slug=retailer_slug,
        )
        carina_db_session.bulk_save_objects(unallocated_voucher_batch)
        carina_db_session.commit()

        for account_holder_type in AccountHolderTypes:
            click.echo("\ncreating %s users." % account_holder_type.value)
            batch_start = ah_to_create
            progress_counter = 0

            with ProgressBar(max_value=ah_to_create) as bar:
                while batch_start > 0:

                    if batch_start <= BATCH_SIZE:
                        batch_end = 0
                    else:
                        batch_end = batch_start - BATCH_SIZE

                    progress_counter = _batch_create_account_holders(
                        polaris_db_session=polaris_db_session,
                        carina_db_session=carina_db_session,
                        batch_start=batch_start,
                        batch_end=batch_end,
                        account_holder_type=account_holder_type,
                        retailer=retailer,
                        active_campaigns=active_campaigns,
                        max_val=max_val,
                        bar=bar,
                        progress_counter=progress_counter,
                        account_holder_type_voucher_code_salt=str(uuid4()),
                        voucher_config=voucher_config,
                    )
                    batch_start = batch_end
    finally:
        carina_db_session.close()
        polaris_db_session.close()
        vela_db_session.close()
