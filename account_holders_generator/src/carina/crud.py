import sys

from typing import TYPE_CHECKING

import click

from hashids import Hashids
from sqlalchemy import delete
from sqlalchemy.future import select

from ..fixtures import reward_config_payload
from .db import Reward, RewardConfig

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def get_reward_config_by_retailer(db_session: "Session", retailer_slug: str) -> RewardConfig:
    reward_config = db_session.scalar(select(RewardConfig).where(RewardConfig.retailer_slug == retailer_slug))
    if not reward_config:
        click.echo(f"No reward config found for retailer: {retailer_slug}")
        sys.exit(-1)

    return reward_config


def create_unallocated_rewards(
    unallocated_rewards_to_create: int, batch_reward_salt: str, reward_config: RewardConfig, retailer_slug: str
) -> list[Reward]:
    hashids = Hashids(batch_reward_salt, min_length=15)
    unallocated_rewards = []
    for i in range(unallocated_rewards_to_create):
        code = (hashids.encode(i),)
        unallocated_rewards.append(
            Reward(
                code=code,
                reward_config_id=reward_config.id,
                allocated=False,
                retailer_slug=retailer_slug,
                deleted=False,
            )
        )

    return unallocated_rewards


def persist_allocated_rewards(db_session: "Session", matching_rewards_payloads: list[dict]) -> None:
    db_session.bulk_save_objects([Reward(**payload) for payload in matching_rewards_payloads])
    db_session.commit()


def setup_reward_config(db_session: "Session", retailer_slug: str, reward_slug: str) -> None:
    db_session.execute(
        delete(Reward)
        .where(
            Reward.reward_config_id == RewardConfig.id,
            RewardConfig.retailer_slug == retailer_slug,
        )
        .execution_options(synchronize_session=False)
    )
    db_session.execute(
        delete(RewardConfig)
        .where(RewardConfig.retailer_slug == retailer_slug)
        .execution_options(synchronize_session=False)
    )
    db_session.add(RewardConfig(**reward_config_payload(retailer_slug, reward_slug)))
    db_session.commit()
