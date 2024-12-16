from unittest.mock import AsyncMock, Mock, call, patch

import pytest
from loguru import logger
from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext
from opentelemetry.metrics._internal.instrument import Histogram

from kaisdk.runner.common.common import Finalizer, Handler, Initializer
from kaisdk.runner.exit.exceptions import FailedToInitializeMetricsError, UndefinedDefaultHandlerFunctionError
from kaisdk.runner.exit.exit_runner import ExitRunner, Postprocessor, Preprocessor
from kaisdk.runner.exit.subscriber import ExitSubscriber
from kaisdk.sdk.kai_nats_msg_pb2 import KaiNatsMessage
from kaisdk.sdk.kai_sdk import KaiSDK
from kaisdk.sdk.measurements.measurements import Measurements
from kaisdk.sdk.metadata.metadata import Metadata
from kaisdk.sdk.model_registry.model_registry import ModelRegistry
from kaisdk.sdk.persistent_storage.persistent_storage import PersistentStorage
from kaisdk.sdk.predictions.store import Predictions


@pytest.fixture(scope="function")
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
async def m_sdk(_: ModelRegistry, __: PersistentStorage, ___: Predictions, ____: Measurements) -> KaiSDK:
    nc = AsyncMock(spec=NatsClient)
    js = Mock(spec=JetStreamContext)
    request_msg = KaiNatsMessage()

    sdk = KaiSDK(nc=nc, js=js, logger=logger)
    sdk.set_request_msg(request_msg)

    return sdk


@pytest.fixture(scope="function")
@patch.object(ExitRunner, "_init_metrics")
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
def m_exit_runner(
    _: ModelRegistry, __: PersistentStorage, ___: Predictions, ____: Measurements, _____: Mock, m_sdk: KaiSDK
) -> ExitRunner:
    nc = AsyncMock(spec=NatsClient)
    js = Mock(spec=JetStreamContext)

    exit_runner = ExitRunner(nc=nc, js=js, logger=logger)

    exit_runner.sdk = m_sdk
    exit_runner.sdk.metadata = Mock(spec=Metadata)
    exit_runner.sdk.metadata.get_process = Mock(return_value="test.process")
    exit_runner.subscriber = Mock(spec=ExitSubscriber)
    exit_runner.metrics = Mock(spec=Histogram)

    return exit_runner


@patch.object(ExitRunner, "_init_metrics")
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
def test_ok(_, __, ___, ____, _____):
    nc = NatsClient()
    js = nc.jetstream()

    runner = ExitRunner(nc=nc, js=js)

    assert runner.sdk is not None
    assert runner.subscriber is not None


@patch.object(ExitRunner, "_init_metrics", side_effect=FailedToInitializeMetricsError)
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
def test_initializing_metrics_ko(_, __, ___, ____, _____):
    nc = NatsClient()
    js = nc.jetstream()

    with pytest.raises(FailedToInitializeMetricsError):
        ExitRunner(nc=nc, js=js)


def test_with_initializer_ok(m_exit_runner):
    m_exit_runner.with_initializer(AsyncMock(spec=Initializer))

    assert m_exit_runner.initializer is not None


def test_with_prepocessor_ok(m_exit_runner):
    m_exit_runner.with_preprocessor(AsyncMock(spec=Preprocessor))

    assert m_exit_runner.preprocessor is not None


def test_with_handler_ok(m_exit_runner):
    m_exit_runner.with_handler(Mock(spec=Handler))

    assert m_exit_runner.response_handlers["default"] is not None


def test_with_custom_handler_ok(m_exit_runner):
    m_exit_runner.with_custom_handler("test-subject", Mock(spec=Handler))

    assert m_exit_runner.response_handlers["test-subject"] is not None


def test_with_postprocessor_ok(m_exit_runner):
    m_exit_runner.with_postprocessor(AsyncMock(spec=Postprocessor))

    assert m_exit_runner.postprocessor is not None


def test_with_finalizer_ok(m_exit_runner):
    m_exit_runner.with_finalizer(Mock(spec=Finalizer))

    assert m_exit_runner.finalizer is not None


async def test_run_ok(m_exit_runner):
    m_exit_runner.initializer = AsyncMock(spec=Initializer)
    m_exit_runner.finalizer = AsyncMock(spec=Finalizer)
    m_exit_runner.with_handler(Mock(spec=Handler))

    await m_exit_runner.run()

    assert m_exit_runner.initializer.called
    assert m_exit_runner.initializer.call_args == call(m_exit_runner.sdk)
    assert m_exit_runner.subscriber.start.called
    assert not m_exit_runner.finalizer.called


async def test_run_undefined_runner_ko(m_exit_runner):
    with pytest.raises(UndefinedDefaultHandlerFunctionError):
        await m_exit_runner.run()


@patch("kaisdk.runner.exit.exit_runner.compose_initializer", return_value=AsyncMock(spec=Initializer))
@patch("kaisdk.runner.exit.exit_runner.compose_finalizer", return_value=AsyncMock(spec=Finalizer))
async def test_run_undefined_initializer_finalizer_ok(m_finalizer, m_initializer, m_exit_runner):
    m_exit_runner.with_handler(Mock(spec=Handler))

    await m_exit_runner.run()

    assert m_exit_runner.initializer.called
    assert m_exit_runner.initializer.call_args == call(m_exit_runner.sdk)
    assert m_exit_runner.subscriber.start.called
    assert not m_exit_runner.finalizer.called
