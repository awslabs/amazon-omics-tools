SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

install:  # Install the app locally
	poetry install
.PHONY: install

ci: lint test check-types check-dependencies ## Run all checks (test, lint, check-types, check-dependencies)
.PHONY: ci

test:  ## Run tests
	poetry run pytest --cov=omics_transfer tests --reruns 5
.PHONY: test

lint:  ## Run linting
	poetry run black --check omics_transfer tests
	poetry run isort -c omics_transfer tests
	poetry run flake8 omics_transfer tests
.PHONY: lint

lint-fix:  ## Run autoformatters
	poetry run black omics_transfer tests
	poetry run isort omics_transfer tests
.PHONY: lint-fix

check-types:  ## Run type check
	poetry run mypy omics_transfer tests --show-error-codes
.PHONY: check-types

check-dependencies:  ## Run security checks on dependencies
	poetry run bandit -r omics_transfer/*
	poetry run pip-audit
.PHONY: check-dependencies

generate-stubs:  ## Generate type stubs
	mkdir -p local-dependencies/stubs/mypy_boto3_omics_package
	poetry run python -m mypy_boto3_builder local-dependencies/stubs -s omics --skip-published
.PHONY: generate-stubs

.DEFAULT_GOAL := help
help: Makefile
	@grep -E '(^[a-zA-Z_-]+:.*?##.*$$)|(^##)' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[32m%-30s\033[0m %s\n", $$1, $$2}' | sed -e 's/\[32m##/[33m/'
