import sys

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Union
from uuid import uuid4

import click

from hashids import Hashids
from sqlalchemy import delete
from sqlalchemy.future import select

from account_holders_generator.src.enums import AccountHolderRewardStatuses, AccountHolderTypes

from ..fixtures import (
    ACCOUNT_HOLDER_REWARD_SWITCHER,
    account_holder_marketing_preference_payload,
    account_holder_payload,
    account_holder_profile_payload,
    account_holder_reward_payload,
    retailer_config_payload,
    reward_payload,
)
from .db import (
    AccountHolder,
    AccountHolderMarketingPreference,
    AccountHolderProfile,
    AccountHolderReward,
    RetailerConfig,
)
from .utils import generate_account_holder_campaign_balances

if TYPE_CHECKING:
    from progressbar import ProgressBar
    from sqlalchemy.orm import Session

    from ..carina.db import RewardConfig


def get_retailer_by_slug(db_session: "Session", retailer_slug: str) -> RetailerConfig:
    retailer = db_session.scalar(select(RetailerConfig).where(RetailerConfig.slug == retailer_slug))
    if not retailer:
        click.echo("requested retailer '%s' does not exists in DB." % retailer_slug)
        sys.exit(-1)

    return retailer


def batch_create_account_holders_and_rewards(
    *,
    db_session: "Session",
    batch_start: int,
    batch_end: int,
    account_holder_type: AccountHolderTypes,
    retailer: RetailerConfig,
    active_campaigns: list[str],
    max_val: int,
    bar: "ProgressBar",
    progress_counter: int,
    account_holder_type_reward_code_salt: str,
    reward_config: "RewardConfig",
) -> tuple[int, list[dict]]:

    account_holders_batch = []
    account_holders_profile_batch = []
    account_holders_marketing_batch = []
    account_holder_balance_batch = []

    account_holder_rewards_batch = []
    matching_rewards_payloads_batch = []
    batch_range = range(batch_start, batch_end, -1)

    account_holders_batch = [
        AccountHolder(**account_holder_payload(i, account_holder_type, retailer)) for i in batch_range
    ]
    db_session.add_all(account_holders_batch)
    db_session.flush()

    for account_holder, i in zip(account_holders_batch, batch_range):
        account_holder_balance_batch.extend(
            generate_account_holder_campaign_balances(account_holder, active_campaigns, account_holder_type, max_val)
        )
        account_holders_profile_batch.append(AccountHolderProfile(**account_holder_profile_payload(account_holder)))
        account_holders_marketing_batch.append(
            AccountHolderMarketingPreference(**account_holder_marketing_preference_payload(account_holder))
        )
        account_holder_rewards, matching_rewards_payloads = _generate_account_holder_rewards(
            i, account_holder, account_holder_type_reward_code_salt, reward_config, retailer
        )
        matching_rewards_payloads_batch.extend(matching_rewards_payloads)
        account_holder_rewards_batch.extend(account_holder_rewards)
        progress_counter += 1
        bar.update(progress_counter)

    db_session.bulk_save_objects(account_holders_profile_batch)
    db_session.bulk_save_objects(account_holders_marketing_batch)
    db_session.bulk_save_objects(account_holder_rewards_batch)
    db_session.bulk_save_objects(account_holder_balance_batch)
    db_session.commit()

    return progress_counter, matching_rewards_payloads_batch


def _generate_account_holder_rewards(
    account_holder_n: Union[int, str],
    account_holder: AccountHolder,
    batch_reward_salt: str,
    reward_config: "RewardConfig",
    retailer: RetailerConfig,
) -> tuple[list[AccountHolderReward], list[dict]]:
    hashids = Hashids(batch_reward_salt, min_length=15)

    def _generate_rewards(
        rewards_required: list[tuple[int, AccountHolderRewardStatuses]]
    ) -> tuple[list[AccountHolderReward], list[dict]]:
        account_holder_rewards: list[AccountHolderReward] = []
        matching_rewards_payloads: list[dict] = []
        for i, (how_many, reward_status) in enumerate(rewards_required):
            issue_date = datetime.now(tz=timezone.utc) - timedelta(days=14)
            for j in range(how_many):
                reward_uuid = uuid4()
                reward_code = hashids.encode(i, j, account_holder_n)
                reward_slug = reward_config.reward_slug
                account_holder_rewards.append(
                    AccountHolderReward(
                        **account_holder_reward_payload(
                            account_holder_id=account_holder.id,
                            retailer_slug=retailer.slug,
                            reward_uuid=reward_uuid,
                            reward_code=reward_code,
                            reward_slug=reward_slug,
                            reward_status=reward_status,
                            issue_date=issue_date,
                        )
                    )
                )

                matching_rewards_payloads.append(
                    reward_payload(
                        reward_uuid=reward_uuid,
                        reward_code=reward_code,
                        reward_config_id=reward_config.id,
                        retailer_slug=retailer.slug,
                    )
                )

        return account_holder_rewards, matching_rewards_payloads

    account_holder_reward_type = int(account_holder_n) % 10
    return _generate_rewards(ACCOUNT_HOLDER_REWARD_SWITCHER[account_holder_reward_type])


def clear_existing_account_holders(db_session: "Session", retailer_id: int) -> None:
    db_session.execute(
        delete(AccountHolder)
        .where(
            AccountHolder.retailer_id == retailer_id,
            AccountHolder.email.like(r"test_%_user_%@autogen.bpl"),
        )
        .execution_options(synchronize_session=False)
    )
    db_session.commit()


def setup_retailer_config(db_session: "Session", retailer_slug: str) -> None:
    db_session.execute(
        delete(AccountHolder)
        .where(AccountHolder.retailer_id == RetailerConfig.id, RetailerConfig.slug == retailer_slug)
        .execution_options(synchronize_session=False)
    )
    db_session.execute(
        delete(RetailerConfig).where(RetailerConfig.slug == retailer_slug).execution_options(synchronize_session=False)
    )
    db_session.add(RetailerConfig(**retailer_config_payload(retailer_slug)))
    db_session.commit()