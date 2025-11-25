.PHONY: help

help: ## This help.
    @awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

SHELL := /bin/bash
THIS_FILE := $(lastword $(MAKEFILE_LIST))

%:
	@echo ""
all:
	@echo ""
install:
	rm -f requirements.txt || true
	/usr/bin/env python -m pip install "pip==25.1.1" --upgrade
	/usr/bin/env pip3 install -U pip-tools setuptools
	pip-compile -v
	/usr/bin/env pip3 install --cache-dir $$(pwd)/.cache -r requirements.txt
