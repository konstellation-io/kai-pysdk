import asyncio
from typing import Callable
from unittest.mock import AsyncMock, Mock, call, patch

import pytest
from google.protobuf.any_pb2 import Any
from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext
from vyper import v

from runner.exit.helpers import (
    compose_finalizer,
    compose_handler,
    compose_initializer,
    compose_postprocessor,
    compose_preprocessor,
)
from sdk.centralized_config.centralized_config import CentralizedConfig
from sdk.kai_nats_msg_pb2 import KaiNatsMessage
from sdk.kai_sdk import KaiSDK
from sdk.model_registry.model_registry import ModelRegistry
from sdk.persistent_storage.persistent_storage import PersistentStorage
from sdk.predictions.store import Predictions

CENTRALIZED_CONFIG = "centralized_configuration.process.config"


@pytest.fixture(scope="function")
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
async def m_sdk(_: ModelRegistry, __: PersistentStorage, ___: Predictions) -> KaiSDK:
    nc = AsyncMock(spec=NatsClient)
    js = Mock(spec=JetStreamContext)
    request_msg = KaiNatsMessage()

    sdk = KaiSDK(nc=nc, js=js)
    sdk.set_request_msg(request_msg)

    return sdk


async def m_user_initializer_awaitable(sdk):
    assert sdk is not None
    await asyncio.sleep(0.00001)


def m_user_initializer(sdk):
    assert sdk is not None


async def m_user_preprocessor_awaitable(sdk, response):
    assert sdk is not None
    assert response is not None
    await asyncio.sleep(0.00001)


def m_user_preprocessor(sdk, response):
    assert sdk is not None
    assert response is not None


def m_user_handler(sdk, response):
    assert sdk is not None
    assert response is not None


async def m_user_handler_awaitable(sdk, response):
    assert sdk is not None
    assert response is not None
    await asyncio.sleep(0.00001)


async def m_user_postprocessor_awaitable(sdk, response):
    assert sdk is not None
    assert response is not None
    await asyncio.sleep(0.00001)


def m_user_postprocessor(sdk, response):
    assert sdk is not None
    assert response is not None


def m_user_finalizer(sdk):
    assert sdk is not None


async def m_user_finalizer_awaitable(sdk):
    assert sdk is not None
    await asyncio.sleep(0.00001)


async def test_compose_initializer_ok(m_sdk):
    v.set(CENTRALIZED_CONFIG, {"key": "value"})
    m_sdk.centralized_config = Mock(spec=CentralizedConfig)
    m_sdk.centralized_config.set_config = AsyncMock()
    initializer: Callable = compose_initializer(m_user_initializer)

    await initializer(m_sdk)

    assert m_sdk.centralized_config.set_config.called
    assert m_sdk.centralized_config.set_config.call_args == call("key", "value")


async def test_compose_initializer_with_awaitable_ok(m_sdk):
    v.set(CENTRALIZED_CONFIG, {"key": "value"})
    m_sdk.centralized_config = Mock(spec=CentralizedConfig)
    m_sdk.centralized_config.set_config = AsyncMock()
    initializer: Callable = compose_initializer(m_user_initializer_awaitable)

    await initializer(m_sdk)

    assert m_sdk.centralized_config.set_config.called
    assert m_sdk.centralized_config.set_config.call_args == call("key", "value")


async def test_compose_initializer_with_none_ok(m_sdk):
    v.set(CENTRALIZED_CONFIG, {"key": "value"})
    m_sdk.centralized_config = Mock(spec=CentralizedConfig)
    m_sdk.centralized_config.set_config = AsyncMock()
    initializer: Callable = compose_initializer()

    await initializer(m_sdk)

    assert m_sdk.centralized_config.set_config.called
    assert m_sdk.centralized_config.set_config.call_args == call("key", "value")


async def test_compose_preprocessor_ok(m_sdk):
    preprocessor: Callable = compose_preprocessor(m_user_preprocessor)
    await preprocessor(m_sdk, Any())


async def test_compose_preprocessor_with_awaitable_ok(m_sdk):
    preprocessor: Callable = compose_preprocessor(m_user_preprocessor_awaitable)
    await preprocessor(m_sdk, Any())


async def test_compose_handler_ok(m_sdk):
    handler: Callable = compose_handler(m_user_handler)
    await handler(m_sdk, Any())


async def test_compose_handler_with_awaitable_ok(m_sdk):
    handler: Callable = compose_handler(m_user_handler_awaitable)
    await handler(m_sdk, Any())


async def test_compose_postprocessor_ok(m_sdk):
    postprocessor: Callable = compose_postprocessor(m_user_postprocessor)
    await postprocessor(m_sdk, Any())


async def test_compose_postprocessor_with_awaitable_ok(m_sdk):
    postprocessor: Callable = compose_postprocessor(m_user_postprocessor_awaitable)
    await postprocessor(m_sdk, Any())


async def test_compose_finalizer_ok(m_sdk):
    finalizer: Callable = compose_finalizer(m_user_finalizer)
    await finalizer(m_sdk)


async def test_compose_finalizer_with_awaitable_ok(m_sdk):
    finalizer: Callable = compose_finalizer(m_user_finalizer_awaitable)
    await finalizer(m_sdk)


async def test_compose_finalizer_with_none_ok(m_sdk):
    finalizer: Callable = compose_finalizer()
    await finalizer(m_sdk)
