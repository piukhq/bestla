import sys

import click

from .src.carina.db import load_models as load_carina_models
from .src.generator import generate_account_holders_and_rewards, generate_retailer_base_config
from .src.hubble.crud import populate_activity_table
from .src.hubble.db import load_models as load_hubble_models
from .src.polaris.db import load_models as load_polaris_models
from .src.vela.db import load_models as load_vela_models


@click.command()
@click.option(
    "-n",
    "account_holders_to_create",
    default=10,
    prompt="account holders of each type to create:",
    help="number of account holders of each type to create.",
)
@click.option(
    "-r",
    "--retailer",
    default="test-retailer",
    prompt="retailer slug:",
    help="retailer used for generated account holders.",
)
@click.option(
    "--max-val",
    default=100,
    prompt="maximum balance value:",
    help="maximum balance value, decimals will be added at random.",
)
@click.option(
    "-c",
    "--campaign",
    default="test-campaign-1",
    help="backup campaign name used for generating balances if no active campaign is found.",
)
@click.option(
    "--reward-slug",
    default="10percentoff",
    help="reward_slug to use in case of a --bootstrap-new-retailer.",
)
@click.option(
    "--host",
    "db_host",
    default="localhost",
    help="database port.",
)
@click.option(
    "--port",
    "db_port",
    default="5432",
    help="database port.",
)
@click.option(
    "--user",
    "db_user",
    default="postgres",
    help="database user.",
)
@click.option(
    "--password",
    "db_pass",
    default="",
    help="database password.",
)
@click.option(
    "--polaris-db-name",
    "polaris_db_name",
    default="polaris",
    help="polaris database name.",
)
@click.option(
    "--vela-db-name",
    "vela_db_name",
    default="vela",
    help="vela database name.",
)
@click.option(
    "--carina-db-name",
    "carina_db_name",
    default="carina",
    help="carina database name.",
)
@click.option(
    "--hubble-db-name",
    "hubble_db_name",
    default="hubble",
    help="hubble database name.",
)
@click.option(
    "--unallocated-rewards",
    "unallocated_rewards_to_create",
    default=10,
    prompt="number of unallocated rewards:",
    help="total number of unallocated rewards to create.",
)
@click.option(
    "--bootstrap-new-retailer/--no-bootstrap-new-retailer",
    "setup_retailer",
    default=False,
    help=("Sets up retailer, campaign, and reward config in addition to the usual account holders and rewards."),
)
@click.option(
    "--refund-window",
    "refund_window",
    default=0,
    help="Sets a refund window for reward rule. If reward goal is reached, pending rewards are created.",
)
@click.option(
    "--fetch-type",
    "fetch_type",
    default="PRE_LOADED",
    help="Sets a fetch type for rewards that are generated. There are only a select few types",
)
@click.option(
    "--activity",
    "activity",
    default=True,
    help="Generates data for activity table. Default is activity type of TX_HISTORY",
)
def main(
    account_holders_to_create: int,
    retailer: str,
    max_val: int,
    campaign: str,
    reward_slug: str,
    db_host: str,
    db_port: str,
    db_user: str,
    db_pass: str,
    polaris_db_name: str,
    vela_db_name: str,
    carina_db_name: str,
    hubble_db_name: str,
    unallocated_rewards_to_create: int,
    setup_retailer: bool,
    refund_window: int,
    fetch_type: str,
    activity: bool,
) -> None:

    if max_val < 0:
        click.echo("maximum balance value must be an integer greater than 1.")
        sys.exit(-1)

    if not (1000000000 > account_holders_to_create > 0):
        click.echo("the number of account holders to create must be between 1 and 1,000,000,000.")
        sys.exit(-1)

    db_uri = "postgresql+psycopg2://%s:%s@%s:%s/" % (
        db_user,
        db_pass,
        db_host,
        db_port,
    )

    carina_db_session = load_carina_models(db_uri + carina_db_name)
    polaris_db_session = load_polaris_models(db_uri + polaris_db_name)
    vela_db_session = load_vela_models(db_uri + vela_db_name)
    hubble_db_session = load_hubble_models(db_uri + hubble_db_name)
    try:
        if setup_retailer is True:
            generate_retailer_base_config(
                carina_db_session,
                polaris_db_session,
                vela_db_session,
                retailer,
                campaign,
                reward_slug,
                refund_window,
                fetch_type,
            )

        retailer_config, active_campaigns = generate_account_holders_and_rewards(
            carina_db_session,
            polaris_db_session,
            vela_db_session,
            account_holders_to_create,
            retailer,
            campaign,
            max_val,
            unallocated_rewards_to_create,
            refund_window,
        )

        if activity:
            populate_activity_table(hubble_db_session, retailer_config.slug, active_campaigns, rows=50)

    finally:
        carina_db_session.close()
        polaris_db_session.close()
        vela_db_session.close()
        hubble_db_session.close()

    click.echo("\naccount holders and rewards created.")
    sys.exit(0)


if __name__ == "__main__":
    main()
