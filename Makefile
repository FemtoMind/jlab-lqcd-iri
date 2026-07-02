PYTHON      := python3.12
VENV        := .venv
BIN         := $(VENV)/bin
UV          := uv
PIP         := $(BIN)/pip
#LOG_FILE    := runtime-logs.log
LOG_FILE    := /tmp/runtime-logs.log
IRI_LOG_FILE ?= $(LOG_FILE)
LOG_ROTATION_DAYS := 5
IRI_LOG_ROTATION_DAYS ?= $(LOG_ROTATION_DAYS)
# Use bash
SHELL := /bin/bash

STAMP_VENV  := $(VENV)/.created
STAMP_DEPS  := $(VENV)/.deps
PROXY_DIR   ?= /home/chen/amsc/fastmcp/jlab-lqcd-mcp-proxy-vscode
PROXY_PKGS  ?= $(PROXY_DIR)/requirements-fastmcp-3.txt

.DEFAULT_GOAL := dev

$(STAMP_VENV):
	$(UV) venv $(VENV)
	touch $(STAMP_VENV)

.venv: $(STAMP_VENV)


$(STAMP_DEPS): $(STAMP_VENV) pyproject.toml
	$(UV) pip install --python $(BIN)/python -e .
	$(UV) pip install --python $(BIN)/python \
		ruff \
		pylint \
		bandit
	touch $(STAMP_DEPS)

deps: $(STAMP_DEPS)

dev: deps
	@source $(BIN)/activate && \
	[ -f local.env ] && source local.env || true && \
	LQCD_PROXY_DIR=$(PROXY_DIR) \
	PYTHONPATH=$(PROXY_DIR):$$PYTHONPATH \
	IRI_API_ADAPTER_facility=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_status=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_account=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_compute=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_filesystem=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_storage=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_task=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_LOG_FILE="$${IRI_LOG_FILE:-$${LOG_FILE:-$(IRI_LOG_FILE)}}" \
	IRI_LOG_ROTATION_DAYS="$${IRI_LOG_ROTATION_DAYS:-$${LOG_ROTATION_DAYS:-$(IRI_LOG_ROTATION_DAYS)}}" \
	DEMO_QUEUE_UPDATE_SECS=2 \
	OPENTELEMETRY_ENABLED=false \
	GLOBUS_RS_ID=1cbc3307-9e6a-4730-a4b5-9e6d8ec37326 \
	GLOBUS_RS_SECRET=xxxxxxxx (replace with actual secret) \
	GLOBUS_RS_SCOPE_SUFFIX=iri_api \
	API_URL_ROOT='http://localhost:8000' fastapi dev

# Install LQCD proxy requirements
PROXY_ENV: 
	@echo "Installing LQCD proxy requirements from $(PROXY_PKGS)" && \
	$(UV) pip install --python $(BIN)/python -r $(PROXY_PKGS)

mcp-int-dev: deps PROXY_ENV
	@source $(BIN)/activate && \
	[ -f local.env ] && source local.env || true && \
	LQCD_PROXY_DIR=$(PROXY_DIR) \
	PYTHONPATH=$(PROXY_DIR):$$PYTHONPATH \
	IRI_API_ADAPTER_facility=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_status=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_account=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_compute=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_filesystem=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_storage=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_API_ADAPTER_task=app.jlab_lqcd_impl.JlabLQCDImpl \
	IRI_LOG_FILE="$${IRI_LOG_FILE:-$${LOG_FILE:-$(IRI_LOG_FILE)}}" \
	IRI_LOG_ROTATION_DAYS="$${IRI_LOG_ROTATION_DAYS:-$${LOG_ROTATION_DAYS:-$(IRI_LOG_ROTATION_DAYS)}}" \
	DEMO_QUEUE_UPDATE_SECS=2 \
	OPENTELEMETRY_ENABLED=false \
	GLOBUS_RS_ID=1cbc3307-9e6a-4730-a4b5-9e6d8ec37326 \
	GLOBUS_RS_SECRET=xxxxxxxxx (replace with actual secret) \
	GLOBUS_RS_SCOPE_SUFFIX=iri_api \
	API_URL_ROOT='http://localhost:8000' \
	python3 $(PROXY_DIR)/lqcd_proxy_server.py --port 8000

.PHONY: clean
clean:
	rm -rf iri_sandbox
	rm -rf .venv

# Format and lint
format: deps
	$(BIN)/ruff format --line-length 200 .

ruff: deps
	$(BIN)/ruff check . --fix || true

pylint: deps
	find . -path ./$(VENV) -prune -o -type f -name "*.py" -print0 | while IFS= read -r -d '' f; do \
		echo "Pylint $$f"; \
		$(BIN)/pylint $$f --rcfile pylintrc || true; \
	done

# Security
audit: deps
	uv pip compile pyproject.toml -o requirements.txt
	uv pip sync requirements.txt
	uv pip install pip-audit
	$(BIN)/pip-audit || true
	rm -f requirements.txt

bandit: deps
	$(BIN)/bandit -r app || true

# Full validation bundle
lint: clean format ruff pylint audit bandit

globus: deps
	@source local.env && $(BIN)/python ./tools/globus.py

ARGS ?=

# call it via: make manage-globus ARGS=scopes-show
manage-globus: deps
	@source local.env && $(BIN)/python ./tools/manage_globus.py $(ARGS)
