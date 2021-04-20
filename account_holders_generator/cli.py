import click

from .src.generator import generate_account_holders


@click.command()
@click.option(
    "-n",
    "users_to_create",
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
    "-c",
    "--campaign",
    default="test-campaign",
    prompt="campaign:",
    help="campaign name used for generated balances.",
)
@click.option(
    "--max-val",
    default=100,
    prompt="maximum balance value:",
    help="maximum balance value, decimals will be added at random.",
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
    "--db-name",
    "db_name",
    default="polaris",
    help="database name.",
)
def main(
    users_to_create: int,
    retailer: str,
    campaign: str,
    max_val: int,
    db_host: str,
    db_port: str,
    db_user: str,
    db_pass: str,
    db_name: str,
) -> None:

    if max_val < 0:
        click.echo("maximum balance value must be an integer greater than 1.")
        exit(-1)

    if not (1000000000 > users_to_create > 0):
        click.echo("the number of account holders to create must be between 1 and 1,000,000,000.")
        exit(-1)

    db_uri = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
        db_user,
        db_pass,
        db_host,
        db_port,
        db_name,
    )
    generate_account_holders(users_to_create, retailer, campaign, max_val, db_uri)
    click.echo("\nAccount holders created.")
    exit(0)


if __name__ == "__main__":
    main()
