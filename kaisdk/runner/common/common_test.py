from unittest.mock import AsyncMock, Mock, call, patch

import pytest
from loguru import logger
from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext
from nats.js.kv import KeyValue
from vyper import v

from kaisdk.runner.common.common import initialize_process_configuration
from kaisdk.sdk.centralized_config.centralized_config import CentralizedConfig
from kaisdk.sdk.kai_nats_msg_pb2 import KaiNatsMessage
from kaisdk.sdk.kai_sdk import KaiSDK
from kaisdk.sdk.measurements.measurements import Measurements
from kaisdk.sdk.model_registry.model_registry import ModelRegistry
from kaisdk.sdk.persistent_storage.persistent_storage import PersistentStorage
from kaisdk.sdk.predictions.store import Predictions


@pytest.fixture(scope="function")
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
async def m_sdk(
    _: ModelRegistry,
    __: PersistentStorage,
    ___: ModelRegistry,
    m_centralized_config: CentralizedConfig,
) -> KaiSDK:
    nc = AsyncMock(spec=NatsClient)
    js = Mock(spec=JetStreamContext)
    global_kv = AsyncMock(spec=KeyValue)
    product_kv = AsyncMock(spec=KeyValue)
    workflow_kv = AsyncMock(spec=KeyValue)
    process_kv = AsyncMock(spec=KeyValue)

    request_msg = KaiNatsMessage()

    sdk = KaiSDK(nc=nc, js=js, logger=logger)
    sdk.set_request_msg(request_msg)
    sdk.centralized_config.global_kv = global_kv
    sdk.centralized_config.product_kv = product_kv
    sdk.centralized_config.workflow_kv = workflow_kv
    sdk.centralized_config.process_kv = process_kv

    return sdk


async def test_initialize_process_configuration_ok(m_sdk):
    v.set("centralized_configuration.process.config", {"test_key": "test_value"})

    await initialize_process_configuration(m_sdk)

    assert m_sdk.centralized_config is not None
    assert m_sdk.centralized_config.process_kv.put.call_count == 1
    assert m_sdk.centralized_config.process_kv.put.call_args == call("test_key", b"test_value")


async def test_initialize_process_configuration_ko(m_sdk):
    v.set("centralized_configuration.process.config", {"test_key": "test_value"})
    m_sdk.centralized_config.process_kv.put.side_effect = Exception("test exception")

    await initialize_process_configuration(m_sdk)

    assert m_sdk.centralized_config is not None
    assert m_sdk.centralized_config.process_kv.put.call_count == 1
    assert m_sdk.centralized_config.process_kv.put.call_args == call("test_key", b"test_value")
