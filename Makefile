.DEFAULT_GOAL := help

# AutoDoc
# -------------------------------------------------------------------------
.PHONY: help
help: ## This help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
.DEFAULT_GOAL := help

.PHONY: protos
protos: ## Generate proto files
	protoc -I proto --python_out=sdk/sdk --mypy_out=sdk/sdk proto/kai_nats_msg.proto

.PHONY: tests
tests: ## Run unit tests
	poetry run pytest sdk runner --cov --cov-report=term-missing

.PHONY: tidy
tidy: ## Run black, isort and codespell
	poetry run black sdk runner \
	&& poetry run isort sdk runner \
	&& poetry run codespell sdk runner -I dictionary.txt \
	--skip="*.git,*.json,kai_nats_msg_pb2.py,.venv,*.lock,__init__.py" \

.PHONY: mypy
mypy: ## Run mypy
	poetry run mypy --pretty --warn-redundant-casts --warn-unused-ignores --warn-unreachable --disallow-untyped-decorators --disallow-incomplete-defs --disallow-untyped-calls --check-untyped-defs --disallow-incomplete-defs --python-version 3.11 sdk runner --config-file pyproject.toml

.PHONY: update-poetries
update-poetries: ## Update all dependencies
	./scripts/update_poetries.sh
