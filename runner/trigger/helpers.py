from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Optional

from google.protobuf.any_pb2 import Any
from loguru import logger

from runner.common.common import Finalizer, Initializer, initialize_process_configuration

if TYPE_CHECKING:
    from runner.trigger.trigger_runner import ResponseHandler, RunnerFunc, TriggerRunner

import inspect

from sdk.kai_sdk import KaiSDK


def compose_initializer(initializer: Optional[Initializer] = None) -> Initializer:
    async def initializer_func(sdk: KaiSDK) -> None:
        assert sdk.logger is not None

        origin = logger._core.extra["origin"]
        logger_ = sdk.logger.bind(context=f"{origin}.[INITIALIZER]")
        logger_.info("initializing...")
        await sdk.initialize()
        await initialize_process_configuration(sdk)

        if initializer is not None:
            logger_.info("executing user initializer...")
            if inspect.iscoroutinefunction(initializer):
                await initializer(sdk)
            else:
                initializer(sdk)
            logger_.info("user initializer executed")

        logger_.info("initialized")

    return initializer_func


def compose_runner(user_runner: RunnerFunc) -> RunnerFunc:
    async def runner_func(trigger_runner: TriggerRunner, sdk: KaiSDK) -> None:
        assert sdk.logger is not None

        origin = logger._core.extra["origin"]
        logger_ = sdk.logger.bind(context=f"{origin}.[RUNNER]")
        logger_.info("executing...")

        logger_.info("executing user runner...")
        await user_runner(trigger_runner, sdk)
        logger_.info("user runner executed")

        logger_.info("shutdown")

    return runner_func


def get_response_handler(handlers: dict[str, asyncio.Queue]) -> ResponseHandler:
    async def response_handler_func(sdk: KaiSDK, response: Any) -> None:
        assert sdk.logger is not None

        request_id = sdk.get_request_id()
        assert request_id is not None
        origin = logger._core.extra["origin"]
        logger_ = sdk.logger.bind(context=f"{origin}.[RESPONSE HANDLER]")
        logger_.info(f"message received with request id {request_id}")

        handler = handlers.pop(request_id, None)
        if handler:
            await handler.put(response)
            return

        logger_.debug(f"no response handler found for request id {request_id}")

    return response_handler_func


def compose_finalizer(user_finalizer: Optional[Finalizer] = None) -> Finalizer:
    async def finalizer_func(sdk: KaiSDK) -> None:
        assert sdk.logger is not None

        origin = logger._core.extra["origin"]
        logger_ = sdk.logger.bind(context=f"{origin}.[FINALIZER]")
        logger_.info("finalizing...")

        if user_finalizer is not None:
            logger_.info("executing user finalizer...")
            if inspect.iscoroutinefunction(user_finalizer):
                await user_finalizer(sdk)
            else:
                user_finalizer(sdk)
            logger_.info("user finalizer executed")

        logger_.info("finalized")

    return finalizer_func
