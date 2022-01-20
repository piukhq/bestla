from uuid import uuid4

import click

from progressbar import ProgressBar

from .carina.crud import create_unallocated_rewards, get_reward_config_by_retailer, persist_allocated_rewards
from .carina.db import load_models as load_carina_models
from .enums import AccountHolderTypes
from .polaris.crud import batch_create_account_holders_and_rewards, clear_existing_account_holders, get_retailer_by_slug
from .polaris.db import load_models as load_polaris_models
from .vela.crud import get_active_campaigns
from .vela.db import load_models as load_vela_models

BATCH_SIZE = 1000


def generate_account_holders_and_rewards(
    ah_to_create: int,
    retailer_slug: str,
    campaign: str,
    max_val: int,
    polaris_db_uri: str,
    vela_db_uri: str,
    carina_db_uri: str,
    unallocated_rewards_to_create: int,
) -> None:

    carina_db_session = load_carina_models(carina_db_uri)
    polaris_db_session = load_polaris_models(polaris_db_uri)
    vela_db_session = load_vela_models(vela_db_uri)

    try:
        retailer = get_retailer_by_slug(polaris_db_session, retailer_slug)
        click.echo("Selected retailer: %s" % retailer.name)
        voucher_config = get_reward_config_by_retailer(carina_db_session, retailer_slug)
        click.echo(f"Voucher type slug for {retailer.name}: {voucher_config.voucher_type_slug}")
        active_campaigns = get_active_campaigns(vela_db_session, retailer, campaign)
        click.echo("Selected campaign %s." % campaign)
        click.echo("Deleting previously generated account holders for requested retailer.")
        clear_existing_account_holders(polaris_db_session, retailer.id)
        unallocated_rewards_batch = create_unallocated_rewards(
            unallocated_rewards_to_create=unallocated_rewards_to_create,
            batch_reward_salt=str(uuid4()),
            voucher_config=voucher_config,
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
                        voucher_config=voucher_config,
                    )
                    persist_allocated_rewards(carina_db_session, matching_reward_payloads_batch)
                    batch_start = batch_end
    finally:
        carina_db_session.close()
        polaris_db_session.close()
        vela_db_session.close()
