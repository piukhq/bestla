from random import randint
from typing import TYPE_CHECKING

import click
from progressbar import progressbar

from .db import AccountHolder, Retailer, load_models
from .enums import UserTypes

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _generate_account_number(prefix: str, user_type: UserTypes, user_n: int) -> str:
    user_n_str = str(user_n)
    return prefix + user_type.initials + "0" * (8 - len(user_n_str)) + user_n_str


def _generate_balance(campaign: str, user_type: UserTypes, max_val: int):

    if user_type == UserTypes.ZERO_BALANCE:
        value = 0
    else:
        value = randint(1, max_val) * 100
        if user_type == UserTypes.FLOAT_BALANCE:
            value += randint(1, 99)

    return {
        campaign: {
            "value": value,
            "campaign_slug": campaign,
        }
    }


def _generate_email(user_type: UserTypes, user_n: int) -> str:
    return f"test_{user_type.value}_user_{user_n}@autogen.bpl"


def _clear_existing_account_holders(db_session: "Session", retailer: Retailer) -> None:
    db_session.query(AccountHolder).filter(
        AccountHolder.retailer == retailer,
        AccountHolder.email.like(r"test_%_user_%@autogen.bpl"),
    ).delete(synchronize_session=False)


def _account_holder_payload(user_n: int, user_type: UserTypes, retailer: Retailer, campaign: str, max_val: int) -> dict:
    return {
        "email": _generate_email(user_type, user_n),
        "retailer_id": retailer.id,
        "status": "ACTIVE",
        "account_number": _generate_account_number(retailer.account_number_prefix, user_type, user_n),
        "is_superuser": False,
        "is_active": True,
        "current_balances": _generate_balance(campaign, user_type, max_val),
    }


def _create_account_holder(
    db_session: "Session", retailer: Retailer, campaign: str, user_n: int, user_type: UserTypes, max_val: int
) -> AccountHolder:
    account_holder = AccountHolder(**_account_holder_payload(user_n, user_type, retailer, campaign, max_val))
    db_session.add(account_holder)
    db_session.commit()

    return account_holder


def _get_retailer_by_slug(db_session: "Session", retailer_slug: str) -> Retailer:
    retailer = db_session.query(Retailer).filter_by(slug=retailer_slug).first()
    if not retailer:
        click.echo("requested retailer [%s] does not exists in DB.")
        exit(-1)

    return retailer


def generate_account_holders(ah_to_create: int, retailer_slug: str, campaign: str, max_val: int, db_uri: str) -> None:
    with load_models(db_uri) as db_session:
        retailer = _get_retailer_by_slug(db_session, retailer_slug)
        click.echo("Selected retailer: %s" % retailer.name)
        _clear_existing_account_holders(db_session, retailer)

        for user_type in UserTypes:
            click.echo("\ncreating %s users." % user_type.value)
            for i in progressbar(range(ah_to_create)):
                _create_account_holder(db_session, retailer, campaign, i, user_type, max_val)
