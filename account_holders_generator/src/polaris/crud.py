import sys

from datetime import datetime, timedelta, timezone
from random import randint
from typing import TYPE_CHECKING
from uuid import uuid4

import click

from hashids import Hashids
from sqlalchemy import delete
from sqlalchemy.future import select

from account_holders_generator.src.carina.db import Retailer
from account_holders_generator.src.enums import AccountHolderRewardStatuses, AccountHolderTypes

from ..fixtures import (
    ACCOUNT_HOLDER_REWARD_SWITCHER,
    account_holder_marketing_preference_payload,
    account_holder_payload,
    account_holder_pending_reward_payload,
    account_holder_profile_payload,
    account_holder_reward_payload,
    account_holder_transaction_history_payload,
    generate_tx_rows,
    retailer_config_payload,
    reward_payload,
)
from .db import (
    AccountHolder,
    AccountHolderMarketingPreference,
    AccountHolderPendingReward,
    AccountHolderProfile,
    AccountHolderReward,
    AccountHolderTransactionHistory,
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
    retailer: Retailer,
    retailer_config: RetailerConfig,
    active_campaigns: list[str],
    max_val: int,
    bar: "ProgressBar",
    progress_counter: int,
    account_holder_type_reward_code_salt: str,
    reward_config: "RewardConfig",
    refund_window: int | None,
    tx_history: bool,
    reward_goal: int,
    loyalty_type: str,
) -> tuple[int, list[dict]]:
    if refund_window is None:
        refund_window = 0
    account_holders_batch = []
    account_holders_profile_batch = []
    account_holders_marketing_batch = []
    account_holder_balance_batch = []

    account_holder_rewards_batch = []
    matching_rewards_payloads_batch = []
    account_holder_transaction_history_batch = []
    batch_range = range(batch_start, batch_end, -1)

    account_holders_batch = [
        AccountHolder(**account_holder_payload(i, account_holder_type, retailer_config)) for i in batch_range
    ]
    db_session.add_all(account_holders_batch)
    db_session.flush()

    for account_holder, i in zip(account_holders_batch, batch_range):
        if tx_history:
            account_holder_transaction_history_batch.extend(
                _generate_account_holder_transaction_history(account_holder, retailer_config, reward_goal, loyalty_type)
            )
        account_holder_balance_batch.extend(
            generate_account_holder_campaign_balances(account_holder, active_campaigns, account_holder_type, max_val)
        )
        account_holders_profile_batch.append(AccountHolderProfile(**account_holder_profile_payload(account_holder)))
        account_holders_marketing_batch.append(
            AccountHolderMarketingPreference(**account_holder_marketing_preference_payload(account_holder))
        )
        account_holder_rewards, matching_rewards_payloads = _generate_account_holder_rewards(
            i, account_holder, account_holder_type_reward_code_salt, reward_config, retailer, retailer_config
        )
        matching_rewards_payloads_batch.extend(matching_rewards_payloads)
        account_holder_rewards_batch.extend(account_holder_rewards)
        if refund_window > 0:
            account_holder_pending_rewards = _generate_account_holder_pending_rewards(
                i, account_holder, reward_config, retailer_config, active_campaigns, refund_window
            )
            db_session.bulk_save_objects(account_holder_pending_rewards)
        progress_counter += 1
        bar.update(progress_counter)

    db_session.bulk_save_objects(account_holder_transaction_history_batch)
    db_session.bulk_save_objects(account_holders_profile_batch)
    db_session.bulk_save_objects(account_holders_marketing_batch)
    db_session.bulk_save_objects(account_holder_rewards_batch)
    db_session.bulk_save_objects(account_holder_balance_batch)
    db_session.commit()

    return progress_counter, matching_rewards_payloads_batch


def _generate_account_holder_rewards(
    account_holder_n: int | str,
    account_holder: AccountHolder,
    batch_reward_salt: str,
    reward_config: "RewardConfig",
    retailer: Retailer,
    retailer_config: RetailerConfig,
) -> tuple[list[AccountHolderReward], list[dict]]:
    hashids = Hashids(batch_reward_salt, min_length=15)

    def _generate_rewards(
        rewards_required: list[tuple[int, AccountHolderRewardStatuses]]
    ) -> tuple[list[AccountHolderReward], list[dict]]:
        account_holder_rewards: list[AccountHolderReward] = []
        matching_rewards_payloads: list[dict] = []
        for i, (how_many, reward_status) in enumerate(rewards_required):
            if reward_status == AccountHolderRewardStatuses.PENDING:
                continue
            issue_date = datetime.now(tz=timezone.utc) - timedelta(days=14)
            for reward_n in range(how_many):
                reward_uuid = uuid4()
                reward_code = hashids.encode(i, reward_n, account_holder_n)
                reward_slug = reward_config.reward_slug
                account_holder_rewards.append(
                    AccountHolderReward(
                        **account_holder_reward_payload(
                            account_holder_id=account_holder.id,
                            retailer_slug=retailer_config.slug,
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
                        retailer_id=retailer.id,
                    )
                )

        return account_holder_rewards, matching_rewards_payloads

    account_holder_reward_type = int(account_holder_n) % 11
    return _generate_rewards(ACCOUNT_HOLDER_REWARD_SWITCHER[account_holder_reward_type])


def _generate_account_holder_pending_rewards(
    account_holder_n: int | str,
    account_holder: AccountHolder,
    reward_config: "RewardConfig",
    retailer_config: RetailerConfig,
    active_campaigns: list[str],
    refund_window: int,
) -> list[AccountHolderPendingReward]:
    def _generate_pending_rewards(
        pending_rewards_required: list[tuple[int, AccountHolderRewardStatuses]]
    ) -> list[AccountHolderPendingReward]:
        account_holder_pending_rewards: list[AccountHolderPendingReward] = []
        for _, (how_many, reward_status) in enumerate(pending_rewards_required):
            if reward_status != AccountHolderRewardStatuses.PENDING:
                continue
            for _ in range(how_many):
                reward_slug = reward_config.reward_slug
                account_holder_pending_rewards.append(
                    AccountHolderPendingReward(
                        **account_holder_pending_reward_payload(
                            account_holder_id=account_holder.id,
                            retailer_slug=retailer_config.slug,
                            reward_slug=reward_slug,
                            campaign_slug=active_campaigns[0],
                            refund_window=refund_window,
                            enqueued=False,
                        )
                    )
                )

        return account_holder_pending_rewards

    account_holder_reward_type = int(account_holder_n) % 11
    return _generate_pending_rewards(ACCOUNT_HOLDER_REWARD_SWITCHER[account_holder_reward_type])


def _generate_account_holder_transaction_history(
    account_holder: AccountHolder,
    retailer_config: RetailerConfig,
    reward_goal: int,
    loyalty_type: str,
) -> list[AccountHolderTransactionHistory]:
    account_holder_transaction_history: list[AccountHolderTransactionHistory] = []
    how_many = randint(1, 10)
    tx_history_rows = generate_tx_rows(reward_goal, retailer_slug=retailer_config.slug)
    for tx_history in tx_history_rows[:how_many]:
        account_holder_transaction_history.append(
            AccountHolderTransactionHistory(
                **account_holder_transaction_history_payload(
                    account_holder_id=account_holder.id,
                    tx_amount=str(tx_history.tx_amount),
                    location=tx_history.location,
                    loyalty_type=loyalty_type,
                )
            )
        )

    return account_holder_transaction_history


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
