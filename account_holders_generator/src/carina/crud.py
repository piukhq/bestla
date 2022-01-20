import sys

from typing import TYPE_CHECKING

import click

from hashids import Hashids

from .db import Voucher, VoucherConfig

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def get_reward_config_by_retailer(db_session: "Session", retailer_slug: str) -> VoucherConfig:
    reward_config = db_session.query(VoucherConfig).filter_by(retailer_slug=retailer_slug).first()
    if not reward_config:
        click.echo(f"No reward config found for retailer: {retailer_slug}")
        sys.exit(-1)

    return reward_config


def create_unallocated_rewards(
    unallocated_rewards_to_create: int, batch_reward_salt: str, voucher_config: VoucherConfig, retailer_slug: str
) -> list[Voucher]:
    hashids = Hashids(batch_reward_salt, min_length=15)
    unallocated_rewards = []
    for i in range(unallocated_rewards_to_create):
        voucher_code = (hashids.encode(i),)
        unallocated_rewards.append(
            Voucher(
                voucher_code=voucher_code,
                voucher_config_id=voucher_config.id,
                allocated=False,
                retailer_slug=retailer_slug,
                deleted=False,
            )
        )

    return unallocated_rewards


def persist_allocated_rewards(db_session: "Session", matching_rewards_payloads: list[dict]) -> None:
    db_session.bulk_save_objects([Voucher(**payload) for payload in matching_rewards_payloads])
    db_session.commit()
