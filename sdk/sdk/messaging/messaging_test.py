from unittest.mock import call, patch

import pytest
from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message
from mock import AsyncMock, Mock
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.js.client import JetStreamContext
from vyper import v

from sdk.kai_nats_msg_pb2 import KaiNatsMessage, MessageType
from sdk.messaging.exceptions import FailedToGetMaxMessageSizeError, MessageTooLargeError, NewRequestMsgError
from sdk.messaging.messaging import Messaging, _message_type_converter
from sdk.messaging.messaging_utils import compress, is_compressed

NATS_OUTPUT = "subscription.output"
TEST_CHANNEL = "subscription.test"
ANY_BYTE = b"any"


@pytest.fixture(scope="function")
def m_messaging() -> Messaging:
    v.set("nats.output", NATS_OUTPUT)
    v.set("metadata.process_name", "test_process_id")
    nc = AsyncMock(spec=NatsClient)
    js = Mock(spec=JetStreamContext)
    request_msg = Mock(spec=KaiNatsMessage)
    request_msg.request_id = "test_request_id"

    messaging = Messaging(nc=nc, js=js)
    messaging.request_msg = request_msg

    return messaging


def test_ok():
    nc = NatsClient()
    js = nc.jetstream()
    request_msg = KaiNatsMessage()

    messaging = Messaging(nc=nc, js=js)
    messaging.request_msg = request_msg

    assert messaging.js is not None
    assert messaging.nc is not None
    assert messaging.request_msg is not None
    assert messaging.logger is not None
    assert messaging.messaging_utils is not None


async def test_send_output(m_messaging):
    m_messaging._publish_msg = AsyncMock()
    response = Mock(spec=Message)

    await m_messaging.send_output(response=response, chan=TEST_CHANNEL)

    assert m_messaging._publish_msg.called
    assert m_messaging._publish_msg.call_args == call(
        msg=response, msg_type=MessageType.OK, chan=TEST_CHANNEL, request_id="test_request_id"
    )


async def test_send_output_with_request_id(m_messaging):
    m_messaging._publish_msg = AsyncMock()
    response = Mock(spec=Message)
    request_id = "test_request_id"

    await m_messaging.send_output_with_request_id(response=response, request_id=request_id, chan=TEST_CHANNEL)

    assert m_messaging._publish_msg.called
    assert m_messaging._publish_msg.call_args == call(
        msg=response, msg_type=MessageType.OK, request_id=request_id, chan=TEST_CHANNEL
    )


async def test_send_any(m_messaging):
    m_messaging._publish_any = AsyncMock()
    response = Mock(spec=Any)

    await m_messaging.send_any(response=response, chan=TEST_CHANNEL)

    assert m_messaging._publish_any.called
    assert m_messaging._publish_any.call_args == call(
        payload=response, msg_type=MessageType.OK, chan=TEST_CHANNEL, request_id="test_request_id"
    )


async def test_send_any_with_request_id(m_messaging):
    m_messaging._publish_any = AsyncMock()
    response = Mock(spec=Any)
    request_id = "test_request_id"

    await m_messaging.send_any_with_request_id(response=response, request_id=request_id, chan=TEST_CHANNEL)

    assert m_messaging._publish_any.called
    assert m_messaging._publish_any.call_args == call(
        payload=response, msg_type=MessageType.OK, request_id=request_id, chan=TEST_CHANNEL
    )


async def test_send_error(m_messaging):
    m_messaging._publish_error = AsyncMock()

    await m_messaging.send_error(error="test_error")

    assert m_messaging._publish_error.called
    assert m_messaging._publish_error.call_args == call(err_msg="test_error", chan=None, request_id="test_request_id")


async def test_send_error_with_channel(m_messaging):
    m_messaging._publish_error = AsyncMock()

    await m_messaging.send_error(error="test_error", chan=TEST_CHANNEL)

    assert m_messaging._publish_error.called
    assert m_messaging._publish_error.call_args == call(
        err_msg="test_error", chan=TEST_CHANNEL, request_id="test_request_id"
    )


def test_get_error_message(m_messaging):
    m_messaging.request_msg.message_type = MessageType.ERROR
    m_messaging.request_msg.error = "test_error"

    message = m_messaging.get_error_message()

    assert message == "test_error"


@pytest.mark.parametrize(
    "message_type, function, expected_result",
    [
        (MessageType.OK, "is_message_ok", True),
        (MessageType.ERROR, "is_message_error", True),
    ],
)
def test_is_message_ok(m_messaging, message_type, function, expected_result):
    m_messaging.request_msg.message_type = message_type

    is_message = getattr(m_messaging, function)()

    assert is_message == expected_result


@patch.object(Any, "Pack", return_value=Mock(spec=Any))
async def test__publish_msg_ok(_, m_messaging):
    request_id = "test_request_id"
    msg = Mock(spec=Message)
    expected_response_msg = KaiNatsMessage(
        request_id=request_id,
        from_node="test_process_id",
        message_type=MessageType.OK,
        payload=Any(),
    )
    m_messaging._publish_response = AsyncMock()

    await m_messaging._publish_msg(msg=msg, msg_type=MessageType.OK, request_id=request_id, chan=TEST_CHANNEL)

    assert m_messaging._publish_response.called
    assert m_messaging._publish_response.call_args == call(expected_response_msg, TEST_CHANNEL)


@patch.object(Any, "Pack", side_effect=Exception)
async def test__publish_msg_packing_message_ko(_, m_messaging):
    message = Mock(spec=Any)
    m_messaging._new_response_msg = Mock(spec=KaiNatsMessage)
    m_messaging._publish_response = AsyncMock()

    await m_messaging._publish_msg(msg=message, msg_type=MessageType.OK)

    assert not m_messaging._new_response_msg.called
    assert not m_messaging._publish_response.called


async def test__publish_any_ok(m_messaging):
    request_id = "test_request_id"
    payload = Any()
    expected_response_msg = KaiNatsMessage(
        request_id=request_id,
        payload=payload,
        from_node="test_process_id",
        message_type=MessageType.OK,
    )
    m_messaging._publish_response = AsyncMock()

    await m_messaging._publish_any(payload=payload, msg_type=MessageType.OK, request_id=request_id, chan=TEST_CHANNEL)

    assert m_messaging._publish_response.called
    assert m_messaging._publish_response.call_args == call(expected_response_msg, TEST_CHANNEL)


async def test__publish_error_ok(m_messaging):
    m_messaging._publish_response = AsyncMock()

    await m_messaging._publish_error(request_id="test_request_id", err_msg="test_error")

    assert m_messaging._publish_response.called
    assert m_messaging._publish_response.call_args == call(
        KaiNatsMessage(
            request_id="test_request_id",
            error="test_error",
            from_node="test_process_id",
            message_type=MessageType.ERROR,
        ),
        None,
    )


def test__new_response_msg_ok(m_messaging):
    request_id = "test_request_id"
    payload = Any()

    response = m_messaging._new_response_msg(payload, request_id, msg_type=MessageType.OK)

    assert response == KaiNatsMessage(
        request_id=request_id,
        payload=payload,
        from_node="test_process_id",
        message_type=MessageType.OK,
    )


async def test__publish_response_ok(m_messaging):
    message = KaiNatsMessage()
    bytes_message = message.SerializeToString()
    m_messaging._prepare_output_message = AsyncMock(return_value=bytes_message)

    await m_messaging._publish_response(message)

    assert m_messaging.js.publish.called
    assert m_messaging.js.publish.call_args == call(NATS_OUTPUT, bytes_message)


async def test__publish_response_with_channel_ok(m_messaging):
    message = KaiNatsMessage()
    bytes_message = message.SerializeToString()
    m_messaging._prepare_output_message = AsyncMock(return_value=bytes_message)

    await m_messaging._publish_response(message, chan=TEST_CHANNEL)

    assert m_messaging.js.publish.called
    assert m_messaging.js.publish.call_args == call(f"{NATS_OUTPUT}.{TEST_CHANNEL}", bytes_message)


@patch.object(KaiNatsMessage, "SerializeToString", side_effect=Exception)
async def test__publish_response_serializing_message_ko(_, m_messaging):
    message = KaiNatsMessage()

    await m_messaging._publish_response(message)

    assert not m_messaging.js.publish.called


async def test__publish_response_preparing_output_message_ko(m_messaging):
    message = KaiNatsMessage()
    m_messaging._prepare_output_message = AsyncMock(side_effect=MessageTooLargeError("0", "0"))

    await m_messaging._publish_response(message)

    assert not m_messaging.js.publish.called


async def test__publish_response_publishing_message_ko(m_messaging):
    message = KaiNatsMessage()
    m_messaging._prepare_output_message = AsyncMock(return_value=b"any")
    m_messaging.js.publish = AsyncMock(side_effect=Exception)

    await m_messaging._publish_response(message)

    assert m_messaging.js.publish.called


def test__get_output_subject_ok(m_messaging):
    channel = "test_channel"

    output_subject = m_messaging._get_output_subject()
    output_subject_with_channel = m_messaging._get_output_subject(channel)

    assert output_subject == NATS_OUTPUT
    assert output_subject_with_channel == f"{NATS_OUTPUT}.{channel}"


async def test__prepare_output_message_ok(m_messaging):
    m_messaging.messaging_utils.get_max_message_size = AsyncMock(return_value=100)

    result = await m_messaging._prepare_output_message(ANY_BYTE)

    assert result == ANY_BYTE


async def test__prepare_output_message_compressed_ok(m_messaging):
    m_messaging.messaging_utils.get_max_message_size = AsyncMock(return_value=30)

    result = await m_messaging._prepare_output_message(b"any" * 30)

    assert is_compressed(result)
    assert result == compress(b"any" * 30)


async def test__prepare_output_message_getting_max_message_size_ko(m_messaging):
    m_messaging.messaging_utils.get_max_message_size = AsyncMock(side_effect=FailedToGetMaxMessageSizeError)

    with pytest.raises(FailedToGetMaxMessageSizeError):
        await m_messaging._prepare_output_message(ANY_BYTE)


async def test__prepare_output_message_too_large_ko(m_messaging):
    m_messaging.messaging_utils.get_max_message_size = AsyncMock(return_value=0)

    with pytest.raises(MessageTooLargeError):
        await m_messaging._prepare_output_message(ANY_BYTE)


def test_message_type_converter_ok():
    assert _message_type_converter(MessageType.OK) == "ok"
    assert _message_type_converter(MessageType.ERROR) == "error"
    assert _message_type_converter(MessageType.UNDEFINED) == "undefined"


def test_get_request_id_ok(m_messaging):
    proto_message = KaiNatsMessage(request_id="test_request_id")
    message_data = proto_message.SerializeToString()
    nats_message = Mock(spec=Msg)
    nats_message.data = message_data

    response = m_messaging.get_request_id(nats_message)

    assert response[0] == "test_request_id"
    assert response[1] == None


def test_get_request_id_ko(m_messaging):
    nats_message = Mock(spec=Msg)
    nats_message.data = b"invalid_data"

    response = m_messaging.get_request_id(nats_message)

    assert response[0] == ""
    assert isinstance(response[1], NewRequestMsgError)


@patch("sdk.messaging.messaging_utils.is_compressed", return_value=True)
@patch("sdk.messaging.messaging_utils.uncompress", return_value=Exception)
def test_get_request_id_uncompress_ko(_, uncompress_mock, m_messaging):
    nats_message = Mock(spec=Msg)
    nats_message.data = b"compressed"

    response = m_messaging.get_request_id(nats_message)

    assert response[0] == ""
    assert isinstance(response[1], NewRequestMsgError)
