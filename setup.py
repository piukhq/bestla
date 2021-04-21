from setuptools import find_packages, setup

setup(
    name="Account Holders Generator",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "click == 7.1.2",
        "progressbar2 == 3.53.1",
        "SQLAlchemy == 1.4.9",
        "psycopg2-binary == 2.8.6",
        "Faker == 8.1.0",
    ],
    entry_points={
        "console_scripts": [
            "account-holders-generator=account_holders_generator.cli:main",
        ],
    },
)
