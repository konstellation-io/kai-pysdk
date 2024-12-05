from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import loguru
from loguru import logger
from nats.js.client import JetStreamContext
from nats.js.errors import KeyNotFoundError
from nats.js.kv import KeyValue
from vyper import v

from sdk.centralized_config.exceptions import (
    FailedToDeleteConfigError,
    FailedToGetConfigError,
    FailedToInitializeConfigError,
    FailedToSetConfigError,
)


class Scope(Enum):
    ProcessScope = "process"
    WorkflowScope = "workflow"
    ProductScope = "product"
    GlobalScope = "global"


@dataclass
class CentralizedConfigABC(ABC):
    @abstractmethod
    async def initialize(self) -> None:
        pass

    @abstractmethod
    async def get_config(self, key: str, scope: Optional[Scope]) -> tuple[str, bool]:
        pass

    @abstractmethod
    async def set_config(self, key: str, value: str, scope: Optional[Scope]) -> None:
        pass

    @abstractmethod
    async def delete_config(self, key: str, scope: Optional[Scope]) -> bool:
        pass


@dataclass
class CentralizedConfig(CentralizedConfigABC):
    js: JetStreamContext
    global_kv: KeyValue = field(init=False)
    product_kv: KeyValue = field(init=False)
    workflow_kv: KeyValue = field(init=False)
    process_kv: KeyValue = field(init=False)
    logger: loguru.Logger = field(init=False)

    def __post_init__(self) -> None:
        origin = logger._core.extra["origin"]
        self.logger = logger.bind(context=f"{origin}.[CENTRALIZED CONFIGURATION]")
        self.global_kv = None
        self.product_kv = None
        self.workflow_kv = None
        self.process_kv = None

    async def initialize(self) -> None:
        self.global_kv, self.product_kv, self.workflow_kv, self.process_kv = await self._init_kv_stores()

    async def _init_kv_stores(self) -> tuple[KeyValue, KeyValue, KeyValue, KeyValue]:
        try:
            name = v.get_string("centralized_configuration.global.bucket")
            self.logger.debug(f"initializing global key-value store with name {name}...")
            global_kv = await self.js.key_value(bucket=name)
            self.logger.debug("global key-value store initialized")

            name = v.get_string("centralized_configuration.product.bucket")
            self.logger.debug(f"initializing product key-value store with name {name}...")
            product_kv = await self.js.key_value(bucket=name)
            self.logger.debug("product key-value store initialized")

            name = v.get_string("centralized_configuration.workflow.bucket")
            self.logger.debug(f"initializing workflow key-value store with name {name}...")
            workflow_kv = await self.js.key_value(bucket=name)
            self.logger.debug("workflow key-value store initialized")

            name = v.get_string("centralized_configuration.process.bucket")
            self.logger.debug(f"initializing process key-value store with name {name}...")
            process_kv = await self.js.key_value(bucket=name)
            self.logger.debug("process key-value store initialized")

            return global_kv, product_kv, workflow_kv, process_kv
        except Exception as e:
            self.logger.warning(f"failed to initialize configuration: {e}")
            raise FailedToInitializeConfigError(error=e)

    async def get_config(self, key: str, scope: Optional[Scope] = None) -> tuple[str, bool]:
        if scope:
            try:
                config = await self._get_config_from_scope(key, scope)
            except KeyNotFoundError as e:
                self.logger.debug(f"key '{key}' not found in scope {scope}: {e}")
                return None, False
            except Exception as e:
                self.logger.warning(f"failed to get config for '{key}': {e}")
                raise FailedToGetConfigError(key=key, scope=scope, error=e)

            return config, True

        for _scope in Scope:
            try:
                config = await self._get_config_from_scope(key, _scope)
            except KeyNotFoundError as e:
                self.logger.debug(f"key '{key}' not found in scope {_scope}: {e}")
                continue
            except Exception as e:
                self.logger.warning(f"failed to get config for '{key}': {e}")
                raise FailedToGetConfigError(key=key, scope=_scope, error=e)

            return config, True

        self.logger.warning(f"key '{key}' not found in any scope")
        return None, False

    async def set_config(self, key: str, value: str, scope: Optional[Scope] = None) -> None:
        scope = scope or Scope.ProcessScope
        kv_store = self._get_scoped_config(scope)

        try:
            await kv_store.put(key, bytes(value, "utf-8"))
        except Exception as e:
            self.logger.warning(f"failed to set config for key '{key}': {e}")
            raise FailedToSetConfigError(key=key, scope=scope, error=e)

    async def delete_config(self, key: str, scope: Optional[Scope] = None) -> bool:
        scope = scope or Scope.ProcessScope
        kv_store = self._get_scoped_config(scope)

        try:
            return await kv_store.delete(key)
        except Exception as e:
            self.logger.warning(f"failed to delete config for key '{key}': {e}")
            raise FailedToDeleteConfigError(key=key, scope=scope, error=e)

    async def _get_config_from_scope(self, key: str, scope: Optional[Scope] = None) -> str:
        scope = scope or Scope.ProcessScope
        kv_store = self._get_scoped_config(scope)
        entry = await kv_store.get(key)

        return entry.value.decode("utf-8")

    def _get_scoped_config(self, scope: Scope) -> KeyValue:
        if scope == Scope.GlobalScope:
            return self.global_kv
        elif scope == Scope.ProductScope:
            return self.product_kv
        elif scope == Scope.WorkflowScope:
            return self.workflow_kv
        elif scope == Scope.ProcessScope:
            return self.process_kv
