from typing import TYPE_CHECKING
from uuid import uuid4

import click

from progressbar import ProgressBar

from .carina.crud import (
    create_unallocated_rewards,
    get_reward_config_by_retailer,
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
from .vela.crud import get_active_campaigns, setup_retailer_reward_and_campaign

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

BATCH_SIZE = 1000


def generate_account_holders_and_rewards(
    carina_db_session: "Session",
    polaris_db_session: "Session",
    vela_db_session: "Session",
    ah_to_create: int,
    retailer_slug: str,
    campaign_slug: str,
    max_val: int,
    unallocated_rewards_to_create: int,
) -> None:

    retailer = get_retailer_by_slug(polaris_db_session, retailer_slug)
    click.echo("Selected retailer: %s" % retailer.name)
    reward_config = get_reward_config_by_retailer(carina_db_session, retailer_slug)
    click.echo(f"Reward slug for {retailer.name}: {reward_config.reward_slug}")
    active_campaigns = get_active_campaigns(vela_db_session, retailer, campaign_slug)
    click.echo("Selected campaign %s." % campaign_slug)
    click.echo("Deleting previously generated account holders for requested retailer.")
    clear_existing_account_holders(polaris_db_session, retailer.id)
    unallocated_rewards_batch = create_unallocated_rewards(
        unallocated_rewards_to_create=unallocated_rewards_to_create,
        batch_reward_salt=str(uuid4()),
        reward_config=reward_config,
        retailer_slug=retailer_slug,
    )
    carina_db_session.bulk_save_objects(unallocated_rewards_batch)
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

                progress_counter, matching_reward_payloads_batch, = batch_create_account_holders_and_rewards(
                    db_session=polaris_db_session,
                    batch_start=batch_start,
                    batch_end=batch_end,
                    account_holder_type=account_holder_type,
                    retailer=retailer,
                    active_campaigns=active_campaigns,
                    max_val=max_val,
                    bar=bar,
                    progress_counter=progress_counter,
                    account_holder_type_reward_code_salt=str(uuid4()),
                    reward_config=reward_config,
                )
                persist_allocated_rewards(carina_db_session, matching_reward_payloads_batch)
                batch_start = batch_end


def generate_retailer_base_config(
    carina_db_session: "Session",
    polaris_db_session: "Session",
    vela_db_session: "Session",
    retailer_slug: str,
    campaign_slug: str,
    reward_slug: str,
) -> None:
    click.echo("Creating '%s' retailer in Polaris." % retailer_slug)
    setup_retailer_config(polaris_db_session, retailer_slug)
    click.echo("Creating '%s' campaign in Vela." % campaign_slug)
    setup_retailer_reward_and_campaign(vela_db_session, retailer_slug, campaign_slug, reward_slug)
    click.echo("Creating '%s' reward config in Carina." % reward_slug)
    setup_reward_config(carina_db_session, retailer_slug, reward_slug)
