import uuid
from unittest.mock import AsyncMock, Mock, call, patch

import mock
import pytest
from google.protobuf.any_pb2 import Any
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy
from nats.js.client import JetStreamContext
from opentelemetry.metrics._internal.instrument import Histogram
from vyper import v

from runner.trigger.exceptions import NewRequestMsgError
from runner.trigger.subscriber import TriggerSubscriber
from runner.trigger.trigger_runner import ResponseHandler, TriggerRunner
from sdk.kai_nats_msg_pb2 import KaiNatsMessage, MessageType
from sdk.kai_sdk import KaiSDK
from sdk.messaging.messaging_utils import compress
from sdk.metadata.metadata import Metadata
from sdk.model_registry.model_registry import ModelRegistry
from sdk.persistent_storage.persistent_storage import PersistentStorage
from sdk.predictions.store import Predictions

NATS_INPUT = "nats.inputs"
SUBJECT = "test.subject"
SUBJECT_LIST = [SUBJECT, "test.subject2"]
SUBJECT_LIST_STR = ",".join(SUBJECT_LIST)
ACK_TIME_KEY = "runner.subscriber.ack_wait_time"
ACK_HOURS = 22
ACK_TIME_SECONDS = float(ACK_HOURS * 3600)
PROCESS = "test process id"


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


@pytest.fixture(scope="function")
@patch.object(TriggerRunner, "_init_metrics")
@patch.object(Predictions, "__new__", return_value=Mock(spec=Predictions))
@patch.object(PersistentStorage, "__new__", return_value=Mock(spec=PersistentStorage))
@patch.object(ModelRegistry, "__new__", return_value=Mock(spec=ModelRegistry))
def m_trigger_runner(
    _: ModelRegistry, __: PersistentStorage, ___: Predictions, ____: Mock, m_sdk: KaiSDK
) -> TriggerRunner:
    nc = AsyncMock(spec=NatsClient)
    js = Mock(spec=JetStreamContext)

    trigger_runner = TriggerRunner(nc=nc, js=js)

    trigger_runner.response_handler = Mock(spec=ResponseHandler)
    trigger_runner.sdk = m_sdk
    trigger_runner.sdk.metadata = Mock(spec=Metadata)
    trigger_runner.sdk.metadata.get_process = Mock(return_value="test.process")
    trigger_runner.messages_metric = Mock(spec=Histogram)

    return trigger_runner


@pytest.fixture(scope="function")
def m_trigger_subscriber(m_trigger_runner: TriggerRunner) -> TriggerSubscriber:
    trigger_subscriber = TriggerSubscriber(m_trigger_runner)

    return trigger_subscriber


@pytest.fixture(scope="function")
def m_msg() -> Msg:
    m_msg = Mock(spec=Msg)
    m_msg.ack = AsyncMock()
    m_msg.subject = "test.subject"

    return m_msg


async def test_start_ok_str_input(m_trigger_subscriber):
    v.set(NATS_INPUT, SUBJECT)
    consumer_name = f"{SUBJECT.replace('.', '-')}-test-process-id"
    v.set(ACK_TIME_KEY, ACK_HOURS)
    m_trigger_subscriber.trigger_runner.sdk.metadata.get_process = Mock(return_value=PROCESS)
    cb_mock = m_trigger_subscriber._process_message = AsyncMock()
    m_trigger_subscriber.trigger_runner.js.subscribe = AsyncMock()

    await m_trigger_subscriber.start()

    assert m_trigger_subscriber.trigger_runner.js.subscribe.called
    assert m_trigger_subscriber.trigger_runner.js.subscribe.call_args == call(
        subject=SUBJECT,
        durable=mock.ANY,
        cb=cb_mock,
        config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW, ack_wait=ACK_TIME_SECONDS),
        manual_ack=True,
    )
    assert isinstance(
        m_trigger_subscriber.trigger_runner.js.subscribe.call_args[1]["durable"], str
    ) and m_trigger_subscriber.trigger_runner.js.subscribe.call_args[1]["durable"].startswith(consumer_name)
    assert m_trigger_subscriber.subscriptions == [m_trigger_subscriber.trigger_runner.js.subscribe.return_value]


async def test_start_ok_list_input(m_trigger_subscriber):
    v.set(NATS_INPUT, SUBJECT_LIST)
    v.set(ACK_TIME_KEY, ACK_HOURS)
    m_trigger_subscriber.trigger_runner.sdk.metadata.get_process = Mock(return_value=PROCESS)
    cb_mock = m_trigger_subscriber._process_message = AsyncMock()
    m_trigger_subscriber.trigger_runner.js.subscribe = AsyncMock()

    await m_trigger_subscriber.start()

    assert m_trigger_subscriber.trigger_runner.js.subscribe.called
    consumer_name = f"{SUBJECT_LIST[0].replace('.', '-')}-test-process-id"
    consumer_name2 = f"{SUBJECT_LIST[1].replace('.', '-')}-test-process-id"
    assert m_trigger_subscriber.trigger_runner.js.subscribe.call_args_list == [
        call(
            subject=SUBJECT_LIST[0],
            durable=mock.ANY,
            cb=cb_mock,
            config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW, ack_wait=ACK_TIME_SECONDS),
            manual_ack=True,
        ),
        call(
            subject=SUBJECT_LIST[1],
            durable=mock.ANY,
            cb=cb_mock,
            config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW, ack_wait=ACK_TIME_SECONDS),
            manual_ack=True,
        ),
    ]
    assert isinstance(
        m_trigger_subscriber.trigger_runner.js.subscribe.call_args_list[0][1]["durable"], str
    ) and m_trigger_subscriber.trigger_runner.js.subscribe.call_args_list[0][1]["durable"].startswith(consumer_name)
    assert isinstance(
        m_trigger_subscriber.trigger_runner.js.subscribe.call_args_list[1][1]["durable"], str
    ) and m_trigger_subscriber.trigger_runner.js.subscribe.call_args_list[1][1]["durable"].startswith(consumer_name2)
    assert m_trigger_subscriber.subscriptions == [
        m_trigger_subscriber.trigger_runner.js.subscribe.return_value,
        m_trigger_subscriber.trigger_runner.js.subscribe.return_value,
    ]


async def test_start_ok_str_list_input(m_trigger_subscriber):
    v.set(NATS_INPUT, SUBJECT_LIST_STR)
    input_subjects = SUBJECT_LIST_STR.replace(" ", "").split(",")
    v.set(ACK_TIME_KEY, ACK_HOURS)
    m_trigger_subscriber.trigger_runner.sdk.metadata.get_process = Mock(return_value=PROCESS)
    cb_mock = m_trigger_subscriber._process_message = AsyncMock()
    m_trigger_subscriber.trigger_runner.js.subscribe = AsyncMock()

    await m_trigger_subscriber.start()

    assert m_trigger_subscriber.trigger_runner.js.subscribe.called
    consumer_name = f"{input_subjects[0].replace('.', '-')}-test-process-id"
    consumer_name2 = f"{input_subjects[1].replace('.', '-')}-test-process-id"
    assert m_trigger_subscriber.trigger_runner.js.subscribe.call_args_list == [
        call(
            subject=input_subjects[0],
            durable=mock.ANY,
            cb=cb_mock,
            config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW, ack_wait=ACK_TIME_SECONDS),
            manual_ack=True,
        ),
        call(
            subject=input_subjects[1],
            durable=mock.ANY,
            cb=cb_mock,
            config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW, ack_wait=ACK_TIME_SECONDS),
            manual_ack=True,
        ),
    ]
    assert isinstance(
        m_trigger_subscriber.trigger_runner.js.subscribe.call_args_list[0][1]["durable"], str
    ) and m_trigger_subscriber.trigger_runner.js.subscribe.call_args_list[0][1]["durable"].startswith(consumer_name)
    assert isinstance(
        m_trigger_subscriber.trigger_runner.js.subscribe.call_args_list[1][1]["durable"], str
    ) and m_trigger_subscriber.trigger_runner.js.subscribe.call_args_list[1][1]["durable"].startswith(consumer_name2)
    assert m_trigger_subscriber.subscriptions == [
        m_trigger_subscriber.trigger_runner.js.subscribe.return_value,
        m_trigger_subscriber.trigger_runner.js.subscribe.return_value,
    ]


async def test_start_nats_subscribing_ko(m_trigger_subscriber):
    v.set(NATS_INPUT, [SUBJECT])
    v.set(ACK_TIME_KEY, ACK_HOURS)
    m_trigger_subscriber.trigger_runner.js.subscribe = AsyncMock(side_effect=Exception("Subscription error"))

    with pytest.raises(SystemExit):
        await m_trigger_subscriber.start()

        assert m_trigger_subscriber.trigger_runner.js.subscribe.called
        assert m_trigger_subscriber.subscriptions == []


@patch("runner.trigger.subscriber.getattr")
async def test_process_message_ok(m_getattr, m_msg, m_trigger_subscriber):
    request_id = "test_request_id"
    expected_response_msg = KaiNatsMessage(
        request_id=request_id,
        from_node="test_process_id",
        message_type=MessageType.OK,
        payload=Any(),
    )
    m_msg.data = expected_response_msg.SerializeToString()
    m_trigger_subscriber._new_request_msg = Mock(return_value=expected_response_msg)
    m_trigger_subscriber._process_runner_error = AsyncMock()
    m_trigger_subscriber.trigger_runner.sdk.metadata.get_process = Mock(return_value="test_process_id")
    m_handler = AsyncMock()
    m_getattr.return_value = m_handler

    await m_trigger_subscriber._process_message(m_msg)

    assert m_trigger_subscriber._new_request_msg.called
    assert not m_trigger_subscriber._process_runner_error.called
    assert m_trigger_subscriber.trigger_runner.sdk.request_msg == expected_response_msg
    assert m_getattr.called
    assert m_handler.called
    assert m_msg.ack.called
    assert m_trigger_subscriber.trigger_runner.messages_metric.record.called


async def test_process_message_not_valid_protobuf_ko(m_msg, m_trigger_subscriber):
    request_id = "test_request_id"
    expected_response_msg = KaiNatsMessage(
        request_id=request_id,
        from_node="test_process_id",
        message_type=MessageType.OK,
        payload=Any(),
    )
    m_msg.data = expected_response_msg.SerializeToString()
    m_trigger_subscriber._new_request_msg = Mock(side_effect=NewRequestMsgError(Exception("New request message error")))
    m_trigger_subscriber._process_runner_error = AsyncMock()

    await m_trigger_subscriber._process_message(m_msg)

    assert m_trigger_subscriber._new_request_msg.called
    assert m_trigger_subscriber._process_runner_error.called
    assert not m_msg.ack.called


@patch("runner.trigger.subscriber.getattr", return_value=None)
async def test_process_message_undefined_handler_ko(m_getattr, m_msg, m_trigger_subscriber):
    request_id = "test_request_id"
    expected_response_msg = KaiNatsMessage(
        request_id=request_id,
        from_node="test_process_id",
        message_type=MessageType.OK,
        payload=Any(),
    )
    m_msg.data = expected_response_msg.SerializeToString()
    m_trigger_subscriber._new_request_msg = Mock(return_value=expected_response_msg)
    m_trigger_subscriber._process_runner_error = AsyncMock()
    m_trigger_subscriber.trigger_runner.sdk.metadata.get_process = Mock(return_value="test_process_id")

    await m_trigger_subscriber._process_message(m_msg)

    assert m_trigger_subscriber._new_request_msg.called
    m_trigger_subscriber.trigger_runner.sdk.metadata.get_process.called
    assert m_getattr.called
    assert m_trigger_subscriber._process_runner_error.called
    assert not m_msg.ack.called


@patch("runner.trigger.subscriber.getattr")
async def test_process_message_handler_ko(m_getattr, m_msg, m_trigger_subscriber):
    request_id = "test_request_id"
    expected_response_msg = KaiNatsMessage(
        request_id=request_id,
        from_node="test_process_id",
        message_type=MessageType.OK,
        payload=Any(),
    )
    m_msg.data = expected_response_msg.SerializeToString()
    m_trigger_subscriber._new_request_msg = Mock(return_value=expected_response_msg)
    m_trigger_subscriber._process_runner_error = AsyncMock()
    m_trigger_subscriber.trigger_runner.sdk.metadata.get_process = Mock(return_value="test_process_id")
    m_handler = AsyncMock(side_effect=Exception("Handler error"))
    m_getattr.return_value = m_handler

    await m_trigger_subscriber._process_message(m_msg)

    assert m_trigger_subscriber._new_request_msg.called
    m_trigger_subscriber.trigger_runner.sdk.metadata.get_process.called
    assert m_getattr.called
    assert m_trigger_subscriber._process_runner_error.called
    assert not m_msg.ack.called


@patch("runner.trigger.subscriber.getattr")
async def test_process_message_ack_ko_ok(m_getattr, m_msg, m_trigger_subscriber):
    request_id = "test_request_id"
    m_msg.ack.side_effect = Exception("Ack error")
    expected_response_msg = KaiNatsMessage(
        request_id=request_id,
        from_node="test_process_id",
        message_type=MessageType.OK,
        payload=Any(),
    )
    m_msg.data = expected_response_msg.SerializeToString()
    m_trigger_subscriber._new_request_msg = Mock(return_value=expected_response_msg)
    m_trigger_subscriber._process_runner_error = AsyncMock()
    m_trigger_subscriber.trigger_runner.sdk.metadata.get_process = Mock(return_value="test_process_id")
    m_handler = AsyncMock()
    m_getattr.return_value = m_handler

    await m_trigger_subscriber._process_message(m_msg)

    assert m_trigger_subscriber._new_request_msg.called
    m_trigger_subscriber.trigger_runner.sdk.metadata.get_process.called
    assert m_getattr.called
    assert m_handler.called
    assert m_msg.ack.called


async def test_process_runner_error_ok(m_msg, m_trigger_subscriber):
    m_msg.data = b"wrong bytes test"
    m_trigger_subscriber.trigger_runner.sdk.messaging.send_error = AsyncMock()

    await m_trigger_subscriber._process_runner_error(m_msg, Exception("process runner error"))

    assert m_msg.ack.called
    assert m_trigger_subscriber.trigger_runner.sdk.messaging.send_error.called
    assert m_trigger_subscriber.trigger_runner.sdk.messaging.send_error.call_args == call("process runner error")


async def test_process_runner_error_ack_ko_ok(m_msg, m_trigger_subscriber):
    m_msg.data = b"wrong bytes"
    m_msg.ack.side_effect = Exception("Ack error")
    m_trigger_subscriber.trigger_runner.sdk.messaging.send_error = AsyncMock()

    await m_trigger_subscriber._process_runner_error(m_msg, Exception("process runner ack error"))

    assert m_trigger_subscriber.trigger_runner.sdk.messaging.send_error.called
    assert m_trigger_subscriber.trigger_runner.sdk.messaging.send_error.call_args == call("process runner ack error")


def test_new_request_msg_ok(m_trigger_subscriber):
    request_id = "test_request_id"
    expected_response_msg = KaiNatsMessage(
        request_id=request_id,
        from_node="test_process_id",
        message_type=MessageType.OK,
        payload=Any(),
    )
    data = expected_response_msg.SerializeToString()

    result = m_trigger_subscriber._new_request_msg(data)

    assert result == expected_response_msg


def test_new_request_msg_compressed_ok(m_trigger_subscriber):
    request_id = "test_request_id"
    expected_response_msg = KaiNatsMessage(
        request_id=request_id,
        from_node="test_process_id",
        message_type=MessageType.OK,
        payload=Any(),
    )
    data = expected_response_msg.SerializeToString()
    data = compress(data)

    result = m_trigger_subscriber._new_request_msg(data)

    assert result == expected_response_msg


@patch("runner.trigger.subscriber.uncompress", side_effect=Exception("Uncompress error"))
def test_new_request_msg_compressed_ko(_, m_trigger_subscriber):
    request_id = "test_request_id"
    expected_response_msg = KaiNatsMessage(
        request_id=request_id,
        from_node="test_process_id",
        message_type=MessageType.OK,
        payload=Any(),
    )
    data = expected_response_msg.SerializeToString()
    data = compress(data)

    with pytest.raises(NewRequestMsgError):
        m_trigger_subscriber._new_request_msg(data)


class MockKaiNatsMessage:
    def __init__(self) -> None:
        self.data = None
        self.ParseFromString = Mock()  # NOSONAR


@patch("runner.trigger.subscriber.KaiNatsMessage", return_value=MockKaiNatsMessage())
def test_new_request_msg_not_valid_protobuf_ko(m_request_message, m_trigger_subscriber):
    m_request_message.return_value.ParseFromString.side_effect = Exception("ParseFromString error")

    with pytest.raises(NewRequestMsgError):
        m_trigger_subscriber._new_request_msg(b"wrong bytes")
