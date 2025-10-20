PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python

.PHONY: venv install test run fmt clean

venv:
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install --upgrade pip

install: venv
	$(PIP) install -e .[dev]

test:
	$(PYTEST) -q

PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff

fmt:
	$(RUFF) check backlog_enricher tests
	$(RUFF) format backlog_enricher tests

run:
	$(PY) -m backlog_enricher.cli $(CMD)

clean:
	rm -rf $(VENV) .pytest_cache .ruff_cache build dist *.egg-info

