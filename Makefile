# COMMON ######################################################################
.DEFAULT_GOAL := help

# Project settings
ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

PACKAGE := snowconn
DEV_PACKAGES := $(PACKAGE) tests
MODULES := $(wildcard $(PACKAGE)/**/*.py)

# Virtual environment paths
VIRTUAL_ENV_NAME ?= .venv

# Style makefile outputs
ECHO_COLOUR=\033[0;34m
NC=\033[0m # No Color

# Define macro to print which target is running
define INFO
    @echo "$(ECHO_COLOUR)##### Running $1 target #####$(NC)"
endef

# Store the macro call in a variable
PRINT_INFO = $(call INFO,$@)

.PHONY: help
help:
	@ printf "\nusage : make <commands> \n\nthe following commands are available : \n\n"
	@ grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sed -e "s/^Makefile://" | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


# PROJECT DEPENDENCIES ########################################################

.PHONY: install
install: .cache install-git-hooks  ## Install project dependencies and tools
	@ uv sync --all-extras --all-packages --group dev

install-git-hooks: .git/hooks/pre-commit .git/hooks/pre-push .git/hooks/commit-msg

.git/hooks/pre-commit .git/hooks/pre-push .git/hooks/commit-msg:
	@ uv run pre-commit install

.cache:
	@ mkdir -p .cache

.PHONY: uninstall
uninstall: clean  ## Delete virtual environment and all generated and temporary files
	rm -rf $(VIRTUAL_ENV_NAME)

.PHONY: reinstall
reinstall: uninstall install  ## Reinstall project (uninstall + install)


# CHECKS ######################################################################

format-ruff:
	$(PRINT_INFO)
	uv run ruff format --config ${ROOT_DIR}/pyproject.toml $(DEV_PACKAGES)
	uv run ruff check --config ${ROOT_DIR}/pyproject.toml --fix-only $(DEV_PACKAGES)

.PHONY: format
format: format-ruff  ## Run formatters (ruff)

.PHONY: check-packages
check-packages:  ## Run package check
	@ echo "$(ECHO_COLOUR)Checking packages$(NC)"
	uv lock --locked -q
	uv run deptry $(PACKAGE)

lint-mypy:
	$(PRINT_INFO)
	uv run mypy --config-file ${ROOT_DIR}/pyproject.toml $(DEV_PACKAGES)

lint-ruff:
	$(PRINT_INFO)
	uv run ruff check --config ${ROOT_DIR}/pyproject.toml --no-fix $(DEV_PACKAGES)
	uv run ruff format --config ${ROOT_DIR}/pyproject.toml --check $(DEV_PACKAGES)

.PHONY: lint
lint: lint-mypy lint-ruff  ## Run linters (mypy, ruff)

.PHONY: pre-commit
pre-commit:  ## Run pre-commit on all files
	uv run pre-commit run --all-files


# TESTS #######################################################################

.PHONY: test
test:  ## Run unit and integration tests
	uv run pytest $(PYTEST_OPTIONS)

.PHONY: read-coverage
read-coverage:  ## Open last coverage report in html page
	open .cache/htmlcov/index.html


# BUILD #######################################################################

DIST_FILES := dist/*.tar.gz dist/*.whl

.PHONY: dist
dist: install $(DIST_FILES)  ## Builds the package, as a tarball and a wheel
$(DIST_FILES): $(MODULES) pyproject.toml
	rm -f $(DIST_FILES)
	uv build


# CLEANUP #####################################################################

.PHONY: clean
clean: .clean-build .clean-docs .clean-cache .clean-install  ## Delete all generated and temporary files

.PHONY: .clean-install
.clean-install:
	find $(DEV_PACKAGES) -name '__pycache__' -delete
	rm -rf *.egg-info

.PHONY: .clean-cache
.clean-cache:
	rm -rf .cache .ruff_cache .mypy_cache .pytest .coverage htmlcov

.PHONY: .clean-docs
.clean-docs:
	rm -rf site

.PHONY: .clean-build
.clean-build:
	rm -rf *.spec dist build


# OTHER TASKS #################################################################

.PHONY: ci
ci: check-packages lint test format  ## Run all tasks that determine CI status
notebooks:
	mkdir -p notebooks
