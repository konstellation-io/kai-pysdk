.DEFAULT_GOAL := help

# AutoDoc
# -------------------------------------------------------------------------
.PHONY: help
help: ## This help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
.DEFAULT_GOAL := help

.PHONY: protos
protos: ## Generate proto files
	poetry run protoc -I proto --python_out=kaisdk/sdk --mypy_out=kaisdk/sdk proto/kai_nats_msg.proto

.PHONY: tests
tests: ## Run unit tests
	poetry run pytest kaisdk --cov --cov-report=term-missing

.PHONY: tidy
tidy: ## Run black, isort and codespell
	poetry run black kaisdk \
	&& poetry run isort kaisdk \
	&& poetry run codespell kaisdk -I dictionary.txt \
	--skip="*.git,*.json,kai_nats_msg_pb2.py,.venv,*.lock,__init__.py"

.PHONY: mypy
mypy: ## Run mypy
	poetry run mypy --pretty --warn-redundant-casts --warn-unused-ignores --warn-unreachable --disallow-untyped-decorators --disallow-incomplete-defs --disallow-untyped-calls --check-untyped-defs --disallow-incomplete-defs --python-version 3.13 kaisdk --config-file pyproject.toml
