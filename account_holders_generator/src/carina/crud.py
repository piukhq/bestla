import sys

from typing import TYPE_CHECKING

import click

from hashids import Hashids
from sqlalchemy import delete
from sqlalchemy.future import select

from ..enums import FetchTypesEnum
from ..fixtures import carina_retailer_payload, retailer_fetch_type_payload, reward_config_payload
from .db import FetchType, Retailer, RetailerFetchType, Reward, RewardConfig

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _get_retailer(db_session: "Session", retailer_slug: str) -> Retailer:
    return db_session.execute(select(Retailer).where(Retailer.slug == retailer_slug)).scalar()


def get_reward_config_and_retailer(db_session: "Session", retailer_slug: str) -> tuple[RewardConfig, Retailer]:
    retailer = _get_retailer(db_session, retailer_slug)
    reward_config = db_session.scalar(select(RewardConfig).where(RewardConfig.retailer_id == retailer.id))
    if not reward_config:
        click.echo(f"No reward config found for retailer: {retailer_slug}")
        sys.exit(-1)

    return reward_config, retailer


def create_unallocated_rewards(
    unallocated_rewards_to_create: int, batch_reward_salt: str, reward_config: RewardConfig
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
                retailer_id=reward_config.retailer_id,
                deleted=False,
            )
        )

    return unallocated_rewards


def persist_allocated_rewards(db_session: "Session", matching_rewards_payloads: list[dict]) -> None:
    db_session.bulk_save_objects([Reward(**payload) for payload in matching_rewards_payloads])
    db_session.commit()


def _clean_carina_db(db_session: "Session", retailer_slug: str) -> None:
    retailer = _get_retailer(db_session, retailer_slug)
    if retailer:
        db_session.execute(
            delete(Retailer).where(Retailer.slug == retailer_slug).execution_options(synchronize_session=False)
        )
        db_session.execute(
            delete(RetailerFetchType)
            .where(RetailerFetchType.retailer_id == retailer.id)
            .execution_options(synchronize_session=False)
        )
        db_session.execute(
            delete(Reward).where(Reward.retailer_id == retailer.id).execution_options(synchronize_session=False)
        )
        db_session.execute(
            delete(RewardConfig)
            .where(RewardConfig.retailer_id == retailer.id)
            .execution_options(synchronize_session=False)
        )
    else:
        pass


def _get_fetch_type(db_session: "Session", fetch_type_name: str) -> FetchType:
    if not hasattr(FetchTypesEnum, fetch_type_name):
        raise Exception("Unknown fetch type")
    return db_session.execute(select(FetchType).where(FetchType.name == fetch_type_name)).scalar()


def _create_reward_config(db_session: "Session", retailer_id: int, fetch_type_id: int, reward_slug: str) -> None:
    db_session.add(RetailerFetchType(**retailer_fetch_type_payload(retailer_id, fetch_type_id)))
    db_session.add(RewardConfig(**reward_config_payload(retailer_id, reward_slug, fetch_type_id)))


def setup_reward_config(db_session: "Session", retailer_slug: str, reward_slug: str, fetch_type_name: str) -> None:
    _clean_carina_db(db_session, retailer_slug)
    db_session.add(Retailer(**carina_retailer_payload(retailer_slug)))
    retailer = _get_retailer(db_session, retailer_slug)
    fetch_type = _get_fetch_type(db_session, fetch_type_name)
    _create_reward_config(db_session, retailer.id, fetch_type.id, reward_slug)
    db_session.commit()
