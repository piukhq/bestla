from typing import TYPE_CHECKING
from uuid import uuid4

import click

from progressbar import ProgressBar

from .carina.crud import (
    create_unallocated_rewards,
    get_reward_config_and_retailer,
    persist_allocated_rewards,
    setup_reward_config,
)
from .enums import AccountHolderTypes
from .polaris.crud import (
    batch_create_account_holders_and_rewards,
    clear_existing_account_holders,
    get_retailer_by_slug,
    setup_retailer_config,
)
from .vela.crud import get_active_campaigns, get_reward_rule, setup_retailer_reward_and_campaign

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

BATCH_SIZE = 1000


def _generate_account_holders_and_rewards_data(
    carina_db_session: "Session",
    polaris_db_session: "Session",
    vela_db_session: "Session",
    ah_to_create: int,
    retailer_slug: str,
    campaign_slug: str,
    max_val: int,
    unallocated_rewards_to_create: int,
    refund_window: int | None,
    tx_history: bool,
    loyalty_type: str,
) -> None:
    if loyalty_type == "STAMPS":
        refund_window = None
    retailer_config = get_retailer_by_slug(polaris_db_session, retailer_slug)
    click.echo("Selected retailer: %s" % retailer_config.name)
    reward_config, retailer = get_reward_config_and_retailer(carina_db_session, retailer_slug)
    click.echo(f"Reward slug for {retailer_config.name}: {reward_config.reward_slug}")
    active_campaigns = get_active_campaigns(vela_db_session, retailer_config, campaign_slug, loyalty_type)
    click.echo("Selected campaign %s." % campaign_slug)
    click.echo("Deleting previously generated account holders for requested retailer.")
    reward_rule = get_reward_rule(vela_db_session, campaign_slug)
    clear_existing_account_holders(polaris_db_session, retailer_config.id)
    unallocated_rewards_batch = create_unallocated_rewards(
        unallocated_rewards_to_create=unallocated_rewards_to_create,
        batch_reward_salt=str(uuid4()),
        reward_config=reward_config,
    )
    carina_db_session.bulk_save_objects(unallocated_rewards_batch)
    carina_db_session.commit()

    for account_holder_type in AccountHolderTypes:
        click.echo("\ncreating %s users." % account_holder_type.value)
        batch_start = ah_to_create
        progress_counter = 0

        with ProgressBar(max_value=ah_to_create) as progress_bar:
            while batch_start > 0:

                if batch_start <= BATCH_SIZE:
                    batch_end = 0
                else:
                    batch_end = batch_start - BATCH_SIZE

                progress_counter, matching_reward_payloads_batch, = batch_create_account_holders_and_rewards(
                    db_session=polaris_db_session,
                    batch_start=batch_start,
                    batch_end=batch_end,
                    account_holder_type=account_holder_type,
                    retailer=retailer,
                    retailer_config=retailer_config,
                    active_campaigns=active_campaigns,
                    max_val=max_val,
                    bar=progress_bar,
                    progress_counter=progress_counter,
                    account_holder_type_reward_code_salt=str(uuid4()),
                    reward_config=reward_config,
                    refund_window=refund_window,
                    tx_history=tx_history,
                    reward_goal=reward_rule.reward_goal,
                    loyalty_type=loyalty_type,
                )
                persist_allocated_rewards(carina_db_session, matching_reward_payloads_batch)
                batch_start = batch_end


def generate_account_holders_and_rewards(
    carina_db_session: "Session",
    polaris_db_session: "Session",
    vela_db_session: "Session",
    ah_to_create: int,
    retailer_slug: str,
    campaign_slug: str,
    max_val: int,
    unallocated_rewards_to_create: int,
    refund_window: int,
    tx_history: bool,
    loyalty_type: str,
) -> None:
    if loyalty_type == "BOTH":
        for loyalty in ["ACCUMULATOR", "STAMPS"]:
            loyalty_retailer_slug = retailer_slug + "-" + loyalty
            loyalty_campaign_slug = campaign_slug + "-" + loyalty
            _generate_account_holders_and_rewards_data(
                carina_db_session,
                polaris_db_session,
                vela_db_session,
                ah_to_create,
                loyalty_retailer_slug,
                loyalty_campaign_slug,
                max_val,
                unallocated_rewards_to_create,
                refund_window,
                tx_history,
                loyalty_type=loyalty,
            )
    else:
        _generate_account_holders_and_rewards_data(
            carina_db_session,
            polaris_db_session,
            vela_db_session,
            ah_to_create,
            retailer_slug,
            campaign_slug,
            max_val,
            unallocated_rewards_to_create,
            refund_window,
            tx_history,
            loyalty_type,
        )


def generate_retailer_base_config(
    carina_db_session: "Session",
    polaris_db_session: "Session",
    vela_db_session: "Session",
    retailer_slug: str,
    campaign_slug: str,
    reward_slug: str,
    refund_window: int,
    fetch_type: str,
    loyalty_type: str,
) -> None:
    if loyalty_type == "BOTH":
        for loyalty in ["ACCUMULATOR", "STAMPS"]:
            loyalty_retailer_slug = retailer_slug + "-" + loyalty
            loyalty_campaign_slug = campaign_slug + "-" + loyalty
            loyalty_reward_slug = reward_slug + "-" + loyalty
            click.echo("Creating '%s' retailer in Polaris." % loyalty_retailer_slug)
            setup_retailer_config(polaris_db_session, loyalty_retailer_slug)
            click.echo("Creating '%s' campaign in Vela." % loyalty_campaign_slug)
            setup_retailer_reward_and_campaign(
                vela_db_session,
                loyalty_retailer_slug,
                loyalty_campaign_slug,
                loyalty_reward_slug,
                refund_window,
                loyalty,
            )
            click.echo("Creating '%s' reward config in Carina." % loyalty_reward_slug)
            setup_reward_config(carina_db_session, loyalty_retailer_slug, loyalty_reward_slug, fetch_type)
    else:
        click.echo("Creating '%s' retailer in Polaris." % retailer_slug)
        setup_retailer_config(polaris_db_session, retailer_slug)
        click.echo("Creating '%s' campaign in Vela." % campaign_slug)
        setup_retailer_reward_and_campaign(
            vela_db_session, retailer_slug, campaign_slug, reward_slug, refund_window, loyalty_type
        )
        click.echo("Creating '%s' reward config in Carina." % reward_slug)
        setup_reward_config(carina_db_session, retailer_slug, reward_slug, fetch_type)
