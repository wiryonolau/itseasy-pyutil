.DEFAULT_GOAL := help
SHELL := /bin/bash
.SHELLFLAGS = -eu -o pipefail -c

PYTHON := /usr/bin/env python3
PIP := /usr/bin/env pip3
PWD := $(shell pwd)

.PHONY: help install all

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_-]+:.*?##/ \
	{printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
all:
	@echo ""
install:
	rm -f requirements.txt || true
	$(PYTHON) -m pip install --upgrade uv
	uv pip compile setup.cfg -o requirements.txt
	uv pip install --cache-dir "$(PWD)/.cache" -r requirements.txt
%:
	@