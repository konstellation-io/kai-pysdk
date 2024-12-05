from __future__ import annotations

from typing import Awaitable, Callable

from google.protobuf.any_pb2 import Any
from loguru import logger
from vyper import v

from sdk.centralized_config.centralized_config import CentralizedConfig
from sdk.kai_sdk import KaiSDK

Initializer = Finalizer = Task = Callable[[KaiSDK], Awaitable[None] | None]
Handler = Callable[[KaiSDK, Any], Awaitable[None] | None]


async def initialize_process_configuration(sdk: KaiSDK) -> None:
    values = v.get("centralized_configuration.process.config")

    assert sdk.logger is not None
    origin = logger._core.extra["origin"]
    logger_ = sdk.logger.bind(context=f"{origin}.[CONFIG INITIALIZER]")
    logger_.info("initializing process configuration")

    if isinstance(values, dict):
        for key, value in values.items():
            try:
                assert isinstance(sdk.centralized_config, CentralizedConfig)
                await sdk.centralized_config.set_config(key, value)
            except Exception as e:
                logger.error(f"error initializing process configuration with key {key}: {e}")

    logger_.info("process configuration initialized")
