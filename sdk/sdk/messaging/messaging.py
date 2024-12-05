from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

import loguru
from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message
from loguru import logger
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.js.client import JetStreamContext
from vyper import v

from sdk.kai_nats_msg_pb2 import KaiNatsMessage, MessageType
from sdk.messaging.exceptions import FailedToGetMaxMessageSizeError, MessageTooLargeError, NewRequestMsgError
from sdk.messaging.messaging_utils import (
    MessagingUtils,
    MessagingUtilsABC,
    compress,
    is_compressed,
    size_in_mb,
    uncompress,
)


@dataclass
class MessagingABC(ABC):
    @abstractmethod
    async def send_output(self, response: Message, chan: Optional[str]) -> None:
        pass

    @abstractmethod
    async def send_output_with_request_id(self, response: Message, request_id: str, chan: Optional[str]) -> None:
        pass

    @abstractmethod
    async def send_any(self, response: Any, chan: Optional[str]) -> None:
        pass

    @abstractmethod
    async def send_any_with_request_id(self, response: Any, request_id: str, chan: Optional[str]) -> None:
        pass

    @abstractmethod
    async def send_error(self, error: str, chan: Optional[str]) -> None:
        pass

    @abstractmethod
    def get_error_message(self) -> str:
        pass

    @abstractmethod
    def is_message_ok(self) -> bool:
        pass

    @abstractmethod
    def is_message_error(self) -> bool:
        pass

    @abstractmethod
    def get_request_id(self, msg: Msg) -> (str, Exception):
        pass


@dataclass
class Messaging(MessagingABC):
    js: JetStreamContext
    nc: NatsClient
    request_msg: KaiNatsMessage = field(init=False, default=None)
    messaging_utils: MessagingUtilsABC = field(init=False)
    logger: loguru.Logger = field(init=False)

    def __post_init__(self) -> None:
        origin = logger._core.extra["origin"]
        self.logger = logger.bind(context=f"{origin}.[MESSAGING]")
        self.messaging_utils = MessagingUtils(js=self.js, nc=self.nc)

    async def send_output(self, response: Message, chan: Optional[str] = None) -> None:
        request_id = self.request_msg.request_id if self.request_msg else None
        await self._publish_msg(msg=response, msg_type=MessageType.OK, chan=chan, request_id=request_id)

    async def send_output_with_request_id(self, response: Message, request_id: str, chan: Optional[str] = None) -> None:
        await self._publish_msg(msg=response, msg_type=MessageType.OK, request_id=request_id, chan=chan)

    async def send_any(self, response: Any, chan: Optional[str] = None) -> None:
        request_id = self.request_msg.request_id if self.request_msg else None
        await self._publish_any(payload=response, msg_type=MessageType.OK, chan=chan, request_id=request_id)

    async def send_any_with_request_id(self, response: Any, request_id: str, chan: Optional[str] = None) -> None:
        await self._publish_any(payload=response, msg_type=MessageType.OK, request_id=request_id, chan=chan)

    async def send_error(self, error: str, chan: Optional[str] = None) -> None:
        request_id = self.request_msg.request_id if self.request_msg else None
        await self._publish_error(err_msg=error, chan=chan, request_id=request_id)

    def get_error_message(self) -> str:
        return self.request_msg.error if self.is_message_error() else ""

    def is_message_ok(self) -> bool:
        return self.request_msg.message_type == MessageType.OK

    def is_message_error(self) -> bool:
        return self.request_msg.message_type == MessageType.ERROR

    async def _publish_msg(
        self, msg: Message, msg_type: MessageType.V, request_id: Optional[str] = None, chan: Optional[str] = None
    ) -> None:
        try:
            payload = Any()
            payload.Pack(msg)
        except Exception as e:
            self.logger.debug(f"failed packing message: {e}")
            return

        if not request_id:
            request_id = str(uuid.uuid4())

        response_msg = self._new_response_msg(payload, request_id, msg_type)
        await self._publish_response(response_msg, chan)

    async def _publish_any(
        self, payload: Any, msg_type: MessageType.V, request_id: Optional[str] = None, chan: Optional[str] = None
    ) -> None:
        if not request_id:
            request_id = str(uuid.uuid4())

        response_msg = self._new_response_msg(payload, request_id, msg_type)
        await self._publish_response(response_msg, chan)

    async def _publish_error(self, err_msg: str, request_id: str, chan: Optional[str] = None) -> None:
        response_msg = KaiNatsMessage(
            request_id=request_id,
            error=err_msg,
            from_node=v.get_string("metadata.process_name"),
            message_type=MessageType.ERROR,
        )
        await self._publish_response(response_msg, chan)

    def _new_response_msg(self, payload: Any, request_id: str, msg_type: MessageType.V) -> KaiNatsMessage:
        self.logger.info(
            f"preparing response message of type {_message_type_converter(msg_type)} and request_id {request_id}..."
        )
        return KaiNatsMessage(
            request_id=request_id,
            payload=payload,
            from_node=v.get_string("metadata.process_name"),
            message_type=msg_type,
        )

    def get_request_id(self, msg: Msg) -> (str, Exception):
        request_msg = KaiNatsMessage()

        data = msg.data
        if is_compressed(data):
            try:
                data = uncompress(data)
            except Exception as e:
                error = NewRequestMsgError(error=e)
                self.logger.error(f"{error}")
                return "", error

        try:
            request_msg.ParseFromString(data)  # deserialize from bytes
        except Exception as e:
            error = NewRequestMsgError(error=e)
            self.logger.error(f"{error}")
            return "", error

        return request_msg.request_id, None if getattr(request_msg, "request_id", None) else "", NewRequestMsgError()

    async def _publish_response(self, response_msg: KaiNatsMessage, chan: Optional[str] = None) -> None:
        output_subject = self._get_output_subject(chan)

        try:
            output_msg = response_msg.SerializeToString()  # serialize to bytes
        except Exception as e:
            self.logger.debug(f"failed serializing response message: {e} with request_id {response_msg.request_id}")
            return

        try:
            output_msg = await self._prepare_output_message(output_msg)
        except (FailedToGetMaxMessageSizeError, MessageTooLargeError) as e:
            self.logger.debug(f"failed preparing output message: {e} with request_id {response_msg.request_id}")
            return

        self.logger.info(f"publishing response to subject {output_subject} with request_id {response_msg.request_id}")

        try:
            await self.js.publish(output_subject, output_msg)
        except Exception as e:
            self.logger.debug(f"failed publishing response: {e} with request_id {response_msg.request_id}")
            return

    def _get_output_subject(self, chan: Optional[str] = None) -> str:
        output_subject = v.get_string("nats.output")
        return f"{output_subject}.{chan}" if chan else output_subject

    async def _prepare_output_message(self, msg: bytes) -> bytes:
        max_size = await self.messaging_utils.get_max_message_size()
        if len(msg) <= max_size:
            return msg

        self.logger.info("message exceeds maximum size allowed! compressing data...")
        out_msg = compress(msg)

        len_out_msg = len(out_msg)
        if len_out_msg > max_size:
            self.logger.warning(f"compressed message size: {len_out_msg} exceeds maximum allowed size: {max_size}")
            raise MessageTooLargeError(size_in_mb(len_out_msg), size_in_mb(max_size))

        self.logger.info(
            f"message compressed! original message size: {len(msg)} - compressed message size: {len_out_msg}"
        )

        return out_msg


def _message_type_converter(msg_type: MessageType.V) -> str:
    if msg_type == MessageType.ERROR:
        return "error"
    elif msg_type == MessageType.OK:
        return "ok"
    else:
        return "undefined"
