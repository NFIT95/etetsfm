MODULE = $(shell basename '$(CURDIR)')

.PHONY: test format check lock install-deps

install-poetry:
	curl -sSL https://install.python-poetry.org | python3 - --version 1.4.1

test:
	poetry run python -m pytest

format:
	poetry run black .
	poetry run isort .

check:
	poetry run black --check .
	poetry run isort -c .
	poetry run pylint --recursive=y .

lock:
	poetry lock --no-update

install-deps:
	poetry install --no-root --sync
