#!/bin/bash


sudo apt install -y curl python python3 python3-venv python3-pip
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3
source "$HOME/.poetry/env"
poetry install
