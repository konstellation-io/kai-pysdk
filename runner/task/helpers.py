from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from google.protobuf.any_pb2 import Any
from loguru import logger

from runner.common.common import Finalizer, Handler, Initializer, initialize_process_configuration

if TYPE_CHECKING:
    from runner.task.task_runner import Preprocessor, Postprocessor

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


def compose_preprocessor(preprocessor: Preprocessor) -> Preprocessor:
    async def preprocessor_func(sdk: KaiSDK, response: Any) -> None:
        assert sdk.logger is not None

        origin = logger._core.extra["origin"]
        logger_ = sdk.logger.bind(context=f"{origin}.[PREPROCESSOR]")
        logger_.info("preprocessing...")

        logger_.info("executing user preprocessor...")
        if inspect.iscoroutinefunction(preprocessor):
            await preprocessor(sdk, response)
        else:
            preprocessor(sdk, response)

    return preprocessor_func


def compose_handler(handler: Handler) -> Handler:
    async def handler_func(sdk: KaiSDK, response: Any) -> None:
        assert sdk.logger is not None

        origin = logger._core.extra["origin"]
        logger_ = sdk.logger.bind(context=f"{origin}.[HANDLER]")
        logger_.info("handling...")

        logger_.info("executing user handler...")
        if inspect.iscoroutinefunction(handler):
            await handler(sdk, response)
        else:
            handler(sdk, response)

    return handler_func


def compose_postprocessor(postprocessor: Postprocessor) -> Postprocessor:
    async def postprocessor_func(sdk: KaiSDK, response: Any) -> None:
        assert sdk.logger is not None

        origin = logger._core.extra["origin"]
        logger_ = sdk.logger.bind(context=f"{origin}.[POSTPROCESSOR]")
        logger_.info("postprocessing...")

        logger_.info("executing user postprocessor...")
        if inspect.iscoroutinefunction(postprocessor):
            await postprocessor(sdk, response)
        else:
            postprocessor(sdk, response)

    return postprocessor_func


def compose_finalizer(finalizer: Optional[Finalizer] = None) -> Finalizer:
    async def finalizer_func(sdk: KaiSDK) -> None:
        assert sdk.logger is not None

        origin = logger._core.extra["origin"]
        logger_ = sdk.logger.bind(context=f"{origin}.[FINALIZER]")
        logger_.info("finalizing...")

        if finalizer is not None:
            logger_.info("executing user finalizer...")
            if inspect.iscoroutinefunction(finalizer):
                await finalizer(sdk)
            else:
                finalizer(sdk)
            logger_.info("user finalizer executed")

        logger_.info("finalized")

    return finalizer_func
