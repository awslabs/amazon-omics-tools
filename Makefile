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
	poetry run pytest --cov=omics --cov-report term-missing tests --reruns 10
.PHONY: test

lint:  ## Run linting
	poetry run black --check omics tests
	poetry run isort -c omics tests
	poetry run flake8 omics tests
	poetry run mypy omics --show-error-codes
.PHONY: lint

lint-fix:  ## Run autoformatters
	poetry run black omics tests
	poetry run isort omics tests
.PHONY: lint-fix

check-types:  ## Run type check
	poetry run mypy omics --show-error-codes
.PHONY: check-types

check-dependencies:  ## Run security checks on dependencies
	poetry run pip-audit
.PHONY: check-dependencies

.DEFAULT_GOAL := help
help: Makefile
	@grep -E '(^[a-zA-Z_-]+:.*?##.*$$)|(^##)' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[32m%-30s\033[0m %s\n", $$1, $$2}' | sed -e 's/\[32m##/[33m/'
