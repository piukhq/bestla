#!/bin/sh

black .
isort .
mypy .
pylint account_holders_generator config.py
