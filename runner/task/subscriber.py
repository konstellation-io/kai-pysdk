from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

import loguru
from loguru import logger
from nats.aio.msg import Msg
from nats.js.api import ConsumerConfig, DeliverPolicy
from nats.js.client import JetStreamContext
from vyper import v

from runner.common.common import Handler
from sdk.messaging.messaging_utils import is_compressed, uncompress

if TYPE_CHECKING:
    from runner.task.task_runner import TaskRunner

import time

from opentelemetry.util.types import Attributes

from runner.task.exceptions import HandlerError, NewRequestMsgError, NotValidProtobuf
from sdk.kai_nats_msg_pb2 import KaiNatsMessage
from sdk.metadata.metadata import Metadata


@dataclass
class TaskSubscriber:
    task_runner: "TaskRunner"
    logger: loguru.Logger = field(init=False)
    subscriptions: list[JetStreamContext.PushSubscription] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        origin = logger._core.extra["origin"]
        self.logger = self.task_runner.logger.bind(context=f"{origin}.[SUBSCRIBER]")

    async def start(self) -> None:
        input_subjects = v.get("nats.inputs")
        process = self.task_runner.sdk.metadata.get_process().replace(".", "-").replace(" ", "-")

        ack_wait_time = timedelta(hours=v.get_int("runner.subscriber.ack_wait_time"))
        if isinstance(input_subjects, str):
            input_subjects = input_subjects.replace(" ", "").split(",")
        if isinstance(input_subjects, list):
            for subject in input_subjects:
                subject_ = subject.replace(".", "-")
                consumer_name = f"{subject_}-{process}"

                self.logger.info(f"subscribing to {subject} from queue group {consumer_name}")
                try:
                    sub = await self.task_runner.js.subscribe(
                        subject=subject,
                        queue=consumer_name,
                        durable=consumer_name,
                        cb=self._process_message,
                        config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW, ack_wait=ack_wait_time.total_seconds()),
                        manual_ack=True,
                    )
                except Exception as e:
                    self.logger.error(f"error subscribing to the NATS subject {subject}: {e}")
                    await self.task_runner._shutdown_handler(asyncio.get_event_loop())

                self.subscriptions.append(sub)
                self.logger.info(f"listening to {subject} from queue group {consumer_name}")

            if len(self.subscriptions) > 0:
                self.logger.info("runner successfully subscribed")
        else:
            self.logger.error("input subjects undefined")
            await self.task_runner._shutdown_handler(asyncio.get_event_loop())

    def get_attributes(self, request_id: str) -> Attributes:
        return {
            "product": Metadata.get_product(),
            "version": Metadata.get_version(),
            "workflow": Metadata.get_workflow(),
            "process": Metadata.get_process(),
            "request_id": request_id,
        }

    async def _process_message(self, msg: Msg) -> None:
        try:
            request_msg = self._new_request_msg(msg.data)
            self.task_runner.sdk.set_request_msg(request_msg)
        except Exception as e:
            await self._process_runner_error(msg, NotValidProtobuf(msg.subject, error=e))
            return

        with self.logger.contextualize(metadata={"request_id": request_msg.request_id}):
            start = time.time() * 1000
            self.logger.info("new message received")
            self.logger.info(f"processing message with request_id {request_msg.request_id} and subject {msg.subject}")

            from_node = request_msg.from_node
            handler = self._get_response_handler(from_node.lower())
            to_node = self.task_runner.sdk.metadata.get_process()

            if handler is None:
                end = time.time() * 1000
                elapsed = end - start
                self.logger.info(f"{Metadata.get_process()} execution time: {elapsed} ms")
                self.task_runner.messages_metric.record(elapsed, attributes=self.get_attributes(request_msg.request_id))
                await self._process_runner_error(msg, Exception(f"no handler defined for {from_node}"))
                return

            try:
                if self.task_runner.preprocessor is not None:
                    await self.task_runner.preprocessor(self.task_runner.sdk, request_msg.payload)
            except Exception as e:
                end = time.time() * 1000
                elapsed = end - start
                self.logger.info(f"{Metadata.get_process()} execution time: {elapsed} ms")
                self.task_runner.messages_metric.record(elapsed, attributes=self.get_attributes(request_msg.request_id))
                await self._process_runner_error(
                    msg, HandlerError(from_node, to_node, error=e, type="handler preprocessor")
                )
                return

            try:
                await handler(self.task_runner.sdk, request_msg.payload)
            except Exception as e:
                end = time.time() * 1000
                elapsed = end - start
                self.logger.info(f"{Metadata.get_process()} execution time: {elapsed} ms")
                self.task_runner.messages_metric.record(elapsed, attributes=self.get_attributes(request_msg.request_id))
                await self._process_runner_error(msg, HandlerError(from_node, to_node, error=e))
                return

            try:
                if self.task_runner.postprocessor is not None:
                    await self.task_runner.postprocessor(self.task_runner.sdk, request_msg.payload)
            except Exception as e:
                end = time.time() * 1000
                elapsed = end - start
                self.logger.info(f"{Metadata.get_process()} execution time: {elapsed} ms")
                self.task_runner.messages_metric.record(elapsed, attributes=self.get_attributes(request_msg.request_id))
                await self._process_runner_error(
                    msg, HandlerError(from_node, to_node, error=e, type="handler postprocessor")
                )
                return

            try:
                await msg.ack()
            except Exception as e:
                end = time.time() * 1000
                elapsed = end - start
                self.logger.info(f"{Metadata.get_process()} execution time: {elapsed} ms")
                self.task_runner.messages_metric.record(elapsed, attributes=self.get_attributes(request_msg.request_id))
                self.logger.error(f"error acknowledging message: {e}")

            end = time.time() * 1000
            elapsed = end - start
            self.logger.info(f"{Metadata.get_process()} execution time: {elapsed} ms")
            self.task_runner.messages_metric.record(elapsed, attributes=self.get_attributes(request_msg.request_id))

    async def _process_runner_error(self, msg: Msg, error: Exception) -> None:
        error_msg = str(error)
        self.logger.info(f"publishing error message {error_msg}")

        try:
            await msg.ack()
        except Exception as e:
            self.logger.error(f"error acknowledging message: {e}")

        await self.task_runner.sdk.messaging.send_error(error_msg)

    def _new_request_msg(self, data: bytes) -> KaiNatsMessage:
        request_msg = KaiNatsMessage()

        if is_compressed(data):
            try:
                data = uncompress(data)
            except Exception as e:
                error = NewRequestMsgError(error=e)
                self.logger.error(f"{error}")
                raise error

        try:
            request_msg.ParseFromString(data)  # deserialize from bytes
        except Exception as e:
            error = NewRequestMsgError(error=e)
            self.logger.error(f"{error}")
            raise error

        return request_msg

    def _get_response_handler(self, subject: str) -> Handler | None:
        if subject in self.task_runner.response_handlers:
            return self.task_runner.response_handlers[subject]

        return (
            self.task_runner.response_handlers["default"] if "default" in self.task_runner.response_handlers else None
        )
