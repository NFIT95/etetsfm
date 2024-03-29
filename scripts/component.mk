MODULE = $(shell basename '$(CURDIR)')

.PHONY: install-poetry test format check validate-lock lock install-deps

install-poetry:
	curl -sSL https://install.python-poetry.org | python3 - --version 1.4.0

test:
	poetry run python -m pytest

format:
	poetry run black .
	poetry run isort .

check:
	poetry run black --check .
	poetry run isort -c .
	poetry run pylint --recursive=y .

validate-lock:
	poetry lock --check

lock:
	poetry lock --no-update

install-deps:
	poetry install --no-root --sync
