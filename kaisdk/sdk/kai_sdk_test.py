import pytest
from loguru import logger
from mock import AsyncMock, Mock, patch
from nats.aio.client import Client as NatsClient
from nats.js.kv import KeyValue
from nats.js.object_store import ObjectStore as NatsObjectStore
from vyper import v

from kaisdk.sdk.centralized_config.centralized_config import CentralizedConfig
from kaisdk.sdk.ephemeral_storage.ephemeral_storage import EphemeralStorage
from kaisdk.sdk.ephemeral_storage.exceptions import FailedToInitializeEphemeralStorageError
from kaisdk.sdk.kai_nats_msg_pb2 import KaiNatsMessage
from kaisdk.sdk.kai_sdk import KaiSDK, Storage
from kaisdk.sdk.measurements.measurements import Measurements
from kaisdk.sdk.messaging.messaging import Messaging
from kaisdk.sdk.metadata.metadata import Metadata
from kaisdk.sdk.model_registry.model_registry import ModelRegistry
from kaisdk.sdk.persistent_storage.persistent_storage import PersistentStorage
from kaisdk.sdk.predictions.store import Predictions

GLOBAL_BUCKET = "centralized_configuration.global.bucket"
PRODUCT_BUCKET = "centralized_configuration.product.bucket"
WORKFLOW_BUCKET = "centralized_configuration.workflow.bucket"
PROCESS_BUCKET = "centralized_configuration.process.bucket"
NATS_OBJECT_STORE = "nats.object_store"
MEASUREMENTS_ENDPOINT = "measurements.endpoint"
MEASUREMENTS_INSECURE = "measurements.insecure"
MEASUREMENTS_TIMEOUT = "measurements.timeout"
MEASUREMENTS_METRICS_INTERVAL = "measurements.metrics_interval"
MEASUREMENTS_ENDPOINT_VALUE = "localhost:4317"


@patch.object(
    CentralizedConfig,
    "_init_kv_stores",
    return_value=(
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
    ),
)
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
async def test_initialize_ok(_, __, ___, ____, _____):
    nc = NatsClient()
    js = nc.jetstream()
    v.set(NATS_OBJECT_STORE, None)
    v.set(GLOBAL_BUCKET, "test_global")
    v.set(PRODUCT_BUCKET, "test_product")
    v.set(WORKFLOW_BUCKET, "test_workflow")
    v.set(PROCESS_BUCKET, "test_process")
    v.set(MEASUREMENTS_ENDPOINT, MEASUREMENTS_ENDPOINT_VALUE)
    v.set(MEASUREMENTS_INSECURE, True)
    v.set(MEASUREMENTS_TIMEOUT, 5)
    v.set(MEASUREMENTS_METRICS_INTERVAL, 1)

    sdk = KaiSDK(nc=nc, js=js, logger=logger)
    await sdk.initialize()

    assert isinstance(sdk.metadata, Metadata)
    assert isinstance(sdk.messaging, Messaging)
    assert isinstance(sdk.storage, Storage)
    assert isinstance(sdk.storage.ephemeral, EphemeralStorage)
    assert isinstance(sdk.storage.persistent, PersistentStorage)
    assert isinstance(sdk.centralized_config, CentralizedConfig)
    assert isinstance(sdk.model_registry, ModelRegistry)
    assert sdk.nc is not None
    assert sdk.js is not None
    assert getattr(sdk, "request_msg", None) is None
    assert sdk.logger is not None
    assert sdk.metadata is not None
    assert sdk.messaging is not None
    assert getattr(sdk.messaging, "request_msg", None) is None
    assert sdk.storage is not None
    assert sdk.storage.ephemeral is not None
    assert sdk.storage.ephemeral.ephemeral_storage_name == ""
    assert sdk.centralized_config is not None
    assert isinstance(sdk.centralized_config.global_kv, KeyValue)
    assert isinstance(sdk.centralized_config.product_kv, KeyValue)
    assert isinstance(sdk.centralized_config.workflow_kv, KeyValue)
    assert isinstance(sdk.centralized_config.process_kv, KeyValue)
    assert isinstance(sdk.measurements, Measurements)
    assert isinstance(sdk.predictions, Predictions)
    assert sdk.predictions is not None


@patch.object(CentralizedConfig, "_init_kv_stores", side_effect=Exception)
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
async def test_initialize_ko(_, __, ___, ____, _____):
    nc = NatsClient()
    js = nc.jetstream()
    v.set(NATS_OBJECT_STORE, None)
    v.set(GLOBAL_BUCKET, "test_global")
    v.set(PRODUCT_BUCKET, "test_product")
    v.set(WORKFLOW_BUCKET, "test_workflow")
    v.set(PROCESS_BUCKET, "test_process")
    v.set(MEASUREMENTS_ENDPOINT, MEASUREMENTS_ENDPOINT_VALUE)
    v.set(MEASUREMENTS_INSECURE, True)
    v.set(MEASUREMENTS_TIMEOUT, 5)
    v.set(MEASUREMENTS_METRICS_INTERVAL, 1)

    with pytest.raises(SystemExit):
        sdk = KaiSDK(nc=nc, js=js, logger=logger)
        await sdk.initialize()


@patch.object(EphemeralStorage, "_init_object_store", return_value=Mock(spec=NatsObjectStore))
@patch.object(
    CentralizedConfig,
    "_init_kv_stores",
    return_value=(
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
    ),
)
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
async def test_nats_initialize_ok(_, __, ___, ____, _____, ______):
    nc = NatsClient()
    js = nc.jetstream()
    v.set(NATS_OBJECT_STORE, "test_object_store")
    v.set(GLOBAL_BUCKET, "test_global")
    v.set(PRODUCT_BUCKET, "test_product")
    v.set(WORKFLOW_BUCKET, "test_workflow")
    v.set(PROCESS_BUCKET, "test_process")
    v.set(MEASUREMENTS_ENDPOINT, MEASUREMENTS_ENDPOINT_VALUE)
    v.set(MEASUREMENTS_INSECURE, True)
    v.set(MEASUREMENTS_TIMEOUT, 5)
    v.set(MEASUREMENTS_METRICS_INTERVAL, 1)

    sdk = KaiSDK(nc=nc, js=js, logger=logger)
    await sdk.initialize()

    assert isinstance(sdk.centralized_config, CentralizedConfig)
    assert isinstance(sdk.storage, Storage)
    assert isinstance(sdk.storage.persistent, PersistentStorage)
    assert isinstance(sdk.storage.ephemeral, EphemeralStorage)
    assert sdk.centralized_config is not None
    assert isinstance(sdk.centralized_config.global_kv, KeyValue)
    assert isinstance(sdk.centralized_config.product_kv, KeyValue)
    assert isinstance(sdk.centralized_config.workflow_kv, KeyValue)
    assert isinstance(sdk.centralized_config.process_kv, KeyValue)
    assert sdk.storage is not None
    assert sdk.storage.ephemeral.object_store is not None
    assert sdk.storage.ephemeral.ephemeral_storage_name == "test_object_store"


@patch.object(EphemeralStorage, "_init_object_store", side_effect=Exception)
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
async def test_nats_initialize_ko(_, __, ___, ____, _____):
    nc = NatsClient()
    js = nc.jetstream()
    v.set(MEASUREMENTS_ENDPOINT, MEASUREMENTS_ENDPOINT_VALUE)
    v.set(MEASUREMENTS_INSECURE, True)
    v.set(MEASUREMENTS_TIMEOUT, 5)
    v.set(MEASUREMENTS_METRICS_INTERVAL, 1)

    with pytest.raises(SystemExit):
        with pytest.raises(FailedToInitializeEphemeralStorageError):
            sdk = KaiSDK(nc=nc, js=js, logger=logger)
            await sdk.initialize()


@patch.object(
    CentralizedConfig,
    "_init_kv_stores",
    return_value=(
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
    ),
)
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
async def test_get_request_id_ok(_, __, ___, ____, _____):
    nc = NatsClient()
    js = nc.jetstream()
    request_msg = KaiNatsMessage(request_id="test_request_id")
    v.set(NATS_OBJECT_STORE, None)
    v.set(GLOBAL_BUCKET, "test_global")
    v.set(PRODUCT_BUCKET, "test_product")
    v.set(WORKFLOW_BUCKET, "test_workflow")
    v.set(PROCESS_BUCKET, "test_process")
    v.set(MEASUREMENTS_ENDPOINT, MEASUREMENTS_ENDPOINT_VALUE)
    v.set(MEASUREMENTS_INSECURE, True)
    v.set(MEASUREMENTS_TIMEOUT, 5)
    v.set(MEASUREMENTS_METRICS_INTERVAL, 1)

    sdk = KaiSDK(nc=nc, js=js, logger=logger)
    await sdk.initialize()

    assert sdk.get_request_id() is None

    sdk.set_request_msg(request_msg)

    assert sdk.get_request_id() == "test_request_id"


@patch.object(
    CentralizedConfig,
    "_init_kv_stores",
    return_value=(
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
        AsyncMock(spec=KeyValue),
    ),
)
@patch.object(Measurements, "__new__", return_value=Mock(spec=Measurements))
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
async def test_set_request_msg_ok(_, __, ___, ____, _____):
    nc = NatsClient()
    js = nc.jetstream()
    request_msg = KaiNatsMessage(request_id="test_request_id")
    v.set(NATS_OBJECT_STORE, None)
    v.set(GLOBAL_BUCKET, "test_global")
    v.set(PRODUCT_BUCKET, "test_product")
    v.set(WORKFLOW_BUCKET, "test_workflow")
    v.set(PROCESS_BUCKET, "test_process")
    v.set(MEASUREMENTS_ENDPOINT, MEASUREMENTS_ENDPOINT_VALUE)
    v.set(MEASUREMENTS_INSECURE, True)
    v.set(MEASUREMENTS_TIMEOUT, 5)
    v.set(MEASUREMENTS_METRICS_INTERVAL, 1)

    sdk = KaiSDK(nc=nc, js=js, logger=logger)
    await sdk.initialize()
    sdk.set_request_msg(request_msg)

    assert sdk.request_msg == request_msg
    assert isinstance(sdk.messaging, Messaging)
    assert sdk.messaging.request_msg == request_msg
