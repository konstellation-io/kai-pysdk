import pytest
from mock import AsyncMock, Mock, patch
from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext
from nats.js.kv import KeyValue
from vyper import v

from runner.exceptions import FailedLoadingConfigError
from runner.exit.exit_runner import ExitRunner
from runner.runner import Runner
from runner.task.task_runner import TaskRunner
from runner.trigger.trigger_runner import TriggerRunner
from sdk.centralized_config.centralized_config import CentralizedConfig
from sdk.ephemeral_storage.ephemeral_storage import EphemeralStorage
from sdk.kai_nats_msg_pb2 import KaiNatsMessage
from sdk.kai_sdk import KaiSDK, Storage
from sdk.measurements.measurements import Measurements
from sdk.messaging.messaging import Messaging
from sdk.metadata.metadata import Metadata
from sdk.model_registry.model_registry import ModelRegistry
from sdk.persistent_storage.persistent_storage import PersistentStorage
from sdk.predictions.store import Predictions

GLOBAL_BUCKET = "centralized_configuration.global.bucket"
PRODUCT_BUCKET = "centralized_configuration.product.bucket"
WORKFLOW_BUCKET = "centralized_configuration.workflow.bucket"
PROCESS_BUCKET = "centralized_configuration.process.bucket"
NATS_OBJECT_STORE = "nats.object_store"
NATS_URL = "nats.url"


@pytest.fixture(scope="function")
@patch("runner.runner.Runner._validate_config")
def m_runner(_: Mock) -> Runner:
    nc = AsyncMock(spec=NatsClient)
    js = Mock(spec=JetStreamContext)
    v.set(NATS_URL, "test_url")
    v.set("APP_CONFIG_PATH", "test_path")
    v.set("runner.logger.output_paths", ["stdout"])
    v.set("runner.logger.error_output_paths", ["stderr"])
    v.set("runner.logger.level", "INFO")

    runner = Runner(nc=nc)
    runner.js = js

    return runner


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
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
async def test_sdk_import_ok(_, __, ___, ____):
    nc = NatsClient()
    js = nc.jetstream()
    request_msg = KaiNatsMessage()
    v.set(NATS_OBJECT_STORE, None)
    v.set(GLOBAL_BUCKET, "test_global")
    v.set(PRODUCT_BUCKET, "test_product")
    v.set(WORKFLOW_BUCKET, "test_workflow")
    v.set(PROCESS_BUCKET, "test_process")

    sdk = KaiSDK(nc=nc, js=js)
    await sdk.initialize()
    sdk.set_request_msg(request_msg)

    assert isinstance(sdk.metadata, Metadata)
    assert isinstance(sdk.messaging, Messaging)
    assert isinstance(sdk.storage, Storage)
    assert isinstance(sdk.centralized_config, CentralizedConfig)
    assert sdk.nc is not None
    assert sdk.js is not None
    assert sdk.request_msg == request_msg
    assert sdk.logger is not None
    assert sdk.metadata is not None
    assert sdk.messaging is not None
    assert sdk.messaging.request_msg == request_msg
    assert sdk.storage is not None
    assert isinstance(sdk.storage.ephemeral, EphemeralStorage)
    assert sdk.storage.ephemeral.ephemeral_storage_name == ""
    assert sdk.storage.ephemeral.object_store is None
    assert isinstance(sdk.storage.persistent, PersistentStorage)
    assert sdk.centralized_config is not None
    assert isinstance(sdk.centralized_config.global_kv, KeyValue)
    assert isinstance(sdk.centralized_config.product_kv, KeyValue)
    assert isinstance(sdk.centralized_config.workflow_kv, KeyValue)
    assert isinstance(sdk.centralized_config.process_kv, KeyValue)
    assert isinstance(sdk.measurements, Measurements)
    assert isinstance(sdk.predictions, Predictions)
    assert sdk.predictions is not None


@patch("runner.runner.Runner._validate_config")
async def test_runner_ok(_):
    nc = NatsClient()
    v.set(NATS_URL, "test_url")
    v.set("APP_CONFIG_PATH", "test_path")
    v.set("runner.logger.output_paths", ["stdout"])
    v.set("runner.logger.error_output_paths", ["stderr"])
    v.set("runner.logger.level", "INFO")

    runner = Runner(nc=nc)

    assert runner.nc is not None
    assert getattr(runner, "js", None) is None
    assert runner.logger is not None


@patch("runner.runner.Runner._validate_config")
async def test_runner_initialize_ok(_):
    nc = AsyncMock(spec=NatsClient)
    nc.connect.return_value = None
    v.set(NATS_URL, "test_url")
    m_js = Mock(spec=JetStreamContext)
    nc.jetstream = Mock(return_value=m_js)

    runner = Runner(nc=nc)
    await runner.initialize()

    assert runner.nc is nc
    assert runner.js is m_js


@patch("runner.runner.Runner._validate_config")
async def test_runner_initialize_nats_ko(_):
    nc = AsyncMock(spec=NatsClient)
    nc.connect.side_effect = Exception("test exception")
    v.set(NATS_URL, "test_url")
    m_js = Mock(spec=JetStreamContext)
    nc.jetstream = Mock(return_value=m_js)

    runner = Runner(nc=nc)
    with pytest.raises(Exception):
        await runner.initialize()


@patch("runner.runner.Runner._validate_config")
async def test_runner_initialize_jetstream_ko(_):
    nc = AsyncMock(spec=NatsClient)
    nc.connect.return_value = None
    v.set(NATS_URL, "test_url")
    nc.jetstream = Mock(side_effect=Exception("test exception"))

    runner = Runner(nc=nc)
    with pytest.raises(Exception):
        await runner.initialize()

    assert runner.nc is nc
    assert getattr(runner, "js", None) is None


@pytest.mark.parametrize(
    "runner_type, runner_method",
    [(TriggerRunner, "trigger_runner"), (TaskRunner, "task_runner"), (ExitRunner, "exit_runner")],
)
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
def test_get_runner_ok(_, __, ___, runner_type, runner_method, m_runner):
    result = getattr(m_runner, runner_method)()

    assert isinstance(result, runner_type)


class MockVyper:
    def __init__(self) -> None:
        self.automatic_env = Mock()
        self.is_set = Mock()
        self.add_config_path = Mock()
        self.set_config_name = Mock()
        self.set_config_type = Mock()
        self.read_in_config = Mock()
        self.merge_in_config = Mock()
        self.all_keys = Mock()
        self.get_string = Mock()
        self.set_default = Mock()
        self.all_settings = Mock()


@patch("runner.runner.v", return_value=MockVyper())
def test_initialize_config_ok(m_vyper, m_runner):
    m_vyper.is_set.side_effect = [True, True]
    m_vyper.get_string.side_effect = ["test_url", "test_path"]
    m_vyper.all_keys.return_value = [NATS_URL, "app.config_path", "app.other_config_path"]

    m_runner.initialize_config()

    assert m_vyper.automatic_env.called
    assert m_vyper.is_set.call_count == 2
    assert m_vyper.add_config_path.call_count == 4
    assert m_vyper.set_config_name.call_count == 2
    assert m_vyper.set_config_type.call_count == 2
    assert m_vyper.read_in_config.called
    assert m_vyper.merge_in_config.called
    assert m_vyper.get_string.call_count == 2
    assert m_vyper.all_keys.call_count == 1
    assert m_vyper.all_settings.call_count == 1
    assert m_vyper.set_default.call_count == 7


@patch("runner.runner.v", return_value=MockVyper())
def test_initialize_config_no_config_ko(m_vyper, m_runner):
    m_vyper.is_set.side_effect = [True, True]
    m_vyper.all_keys.return_value = []
    with pytest.raises(FailedLoadingConfigError):
        m_runner.initialize_config()

        assert m_vyper.automatic_env.called
        assert m_vyper.is_set.call_count == 2
        assert m_vyper.add_config_path.call_count == 2
        assert m_vyper.set_config_name.call_count == 2
        assert m_vyper.set_config_type.call_count == 2
        assert m_vyper.read_in_config.called
        assert m_vyper.merge_in_config.called
        assert m_vyper.get_string.call_count == 2
        assert m_vyper.all_keys.call_count == 1
        assert m_vyper.all_settings.call_count == 0


@patch("runner.runner.v", return_value=MockVyper())
def test_initialize_config_read_in_config_merge_in_config_ko(m_vyper, m_runner):
    m_vyper.is_set.side_effect = [False, False]
    m_vyper.all_keys.return_value = []
    m_vyper.read_in_config.side_effect = Exception("read exception")
    m_vyper.merge_in_config.side_effect = Exception("merge exception")

    with pytest.raises(FailedLoadingConfigError):
        m_runner.initialize_config()

        assert m_vyper.automatic_env.called
        assert m_vyper.is_set.call_count == 2
        assert m_vyper.add_config_path.call_count == 2
        assert m_vyper.set_config_name.call_count == 2
        assert m_vyper.set_config_type.call_count == 2
        assert m_vyper.read_in_config.called
        assert m_vyper.merge_in_config.called
        assert m_vyper.all_keys.call_count == 1
        assert m_vyper.all_settings.call_count == 0


@patch("runner.runner.v", return_value=MockVyper())
def test_initialize_config_read_one_exception_ok(m_vyper, m_runner):
    m_vyper.is_set.side_effect = [False, False]
    m_vyper.all_keys.return_value = [NATS_URL]
    m_vyper.read_in_config.side_effect = Exception("read exception")

    m_runner.initialize_config()

    assert m_vyper.automatic_env.called
    assert m_vyper.is_set.call_count == 2
    assert m_vyper.add_config_path.call_count == 2
    assert m_vyper.set_config_name.call_count == 2
    assert m_vyper.set_config_type.call_count == 2
    assert m_vyper.read_in_config.called
    assert m_vyper.merge_in_config.called
    assert m_vyper.all_keys.call_count == 1
    assert m_vyper.all_settings.call_count == 1
    assert m_vyper.set_default.call_count == 7


@patch("runner.runner.v", return_value=MockVyper())
@patch("runner.runner.reduce", side_effect=Exception("reduce exception"))
def test_missing_configuration_key_ko(_, m_vyper, m_runner):
    m_vyper.is_set.side_effect = [False, False]
    m_vyper.all_keys.return_value = [NATS_URL, "app.config_path"]

    with pytest.raises(FailedLoadingConfigError):
        m_runner.initialize_config()

        assert m_vyper.automatic_env.called
        assert m_vyper.is_set.call_count == 2
        assert m_vyper.add_config_path.call_count == 2
        assert m_vyper.set_config_name.call_count == 2
        assert m_vyper.set_config_type.call_count == 2
        assert m_vyper.read_in_config.called
        assert m_vyper.merge_in_config.called
        assert m_vyper.all_keys.call_count == 1
        assert m_vyper.all_settings.call_count == 1
        assert m_vyper.set_default.call_count == 0
