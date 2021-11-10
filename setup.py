from setuptools import find_packages, setup

setup(
    name="Account Holders Generator",
    version="0.2.0",
    packages=find_packages(),
    install_requires=[
        "click == 8.0.3",
        "progressbar2 == 3.55.0",
        "SQLAlchemy == 1.4.26",
        "psycopg2-binary == 2.9.1",
        "Faker == 9.8.0",
    ],
    entry_points={
        "console_scripts": [
            "account-holders-generator=account_holders_generator.cli:main",
        ],
    },
)
