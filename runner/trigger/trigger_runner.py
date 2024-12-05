from __future__ import annotations

import asyncio
import signal
import sys
import threading
from asyncio import Queue
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

import loguru
from google.protobuf import any_pb2
from loguru import logger
from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext
from opentelemetry.metrics._internal.instrument import Histogram

from runner.common.common import Finalizer, Initializer
from runner.trigger.exceptions import FailedToInitializeMetricsError, UndefinedRunnerFunctionError
from runner.trigger.helpers import compose_finalizer, compose_initializer, compose_runner, get_response_handler
from runner.trigger.subscriber import TriggerSubscriber
from sdk.kai_sdk import KaiSDK

ResponseHandler = Callable[[KaiSDK, any_pb2.Any], Awaitable[None]]


@dataclass
class TriggerRunner:
    sdk: KaiSDK = field(init=False)
    nc: NatsClient
    js: JetStreamContext
    logger: loguru.Logger = logger.bind(context="[TRIGGER]")
    response_handler: ResponseHandler = field(init=False, default=None)
    response_channels: dict[str, Queue] = field(init=False, default_factory=dict)
    initializer: Optional[Initializer] = None
    runner: RunnerFunc = field(init=False)
    subscriber: TriggerSubscriber = field(init=False)
    finalizer: Optional[Finalizer] = None
    tasks: list[threading.Thread] = field(init=False, default_factory=list)
    messages_metric: Histogram = field(init=False)

    def __post_init__(self) -> None:
        logger.configure(extra={"context": "", "metadata": {}, "origin": "[TRIGGER]"})
        self.sdk = KaiSDK(nc=self.nc, js=self.js, logger=logger)
        self.subscriber = TriggerSubscriber(self)
        self._init_metrics()

    def _init_metrics(self) -> None:
        try:
            self.messages_metric = self.sdk.measurements.get_metrics_client().create_histogram(
                name="runner-process-message-metric",
                unit="ms",
                description="How long it takes to process a message and times called.",
            )
        except Exception as e:
            self.logger.error(f"error initializing metrics: {e}")
            raise FailedToInitializeMetricsError(e)

    def with_initializer(self, initializer: Initializer) -> TriggerRunner:
        self.initializer = compose_initializer(initializer)
        return self

    def with_runner(self, runner: RunnerFunc) -> TriggerRunner:
        self.runner = compose_runner(runner)
        return self

    def with_finalizer(self, finalizer: Finalizer) -> TriggerRunner:
        self.finalizer = compose_finalizer(finalizer)
        return self

    async def get_response_channel(self, request_id: str) -> str | None:
        if request_id not in self.response_channels:
            self.response_channels[request_id] = Queue(maxsize=1)
        response = await self.response_channels[request_id].get()
        return response

    async def _shutdown_handler(
        self,
        loop: asyncio.AbstractEventLoop,
        signal: Optional[signal.Signals] = None,
    ) -> None:
        if signal:
            self.logger.info(f"received exit signal {signal.name}...")
        self.logger.info("shutting down runner...")
        self.logger.info("closing opened channels...")
        for request_id, channel in self.response_channels.items():
            await channel.put(None)
            self.logger.info(f"channel closed for request id {request_id}")

        self.logger.info("shutting down subscriber")
        for sub in self.subscriber.subscriptions:
            self.logger.info(f"unsubscribing from subject {sub.subject}")

            try:
                await sub.unsubscribe()
            except Exception as e:
                self.logger.error(f"error unsubscribing from the NATS subject {sub.subject}: {e}")
                sys.exit(1)

        if signal:
            await self.finalizer(self.sdk)

        self.logger.info("successfully shutdown trigger runner")

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

        [task.cancel() for task in tasks]

        self.logger.info(f"cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks, return_exceptions=True)

        if not self.nc.is_closed:
            self.logger.info("closing nats connection")
            await self.nc.close()

        loop.stop()
        if not signal:
            sys.exit(1)

    async def run(self) -> None:
        if getattr(self, "runner", None) is None:
            raise UndefinedRunnerFunctionError

        if not self.initializer:
            self.initializer = compose_initializer()

        self.response_handler = get_response_handler(self.response_channels)

        if not self.finalizer:
            self.finalizer = compose_finalizer()

        loop = asyncio.get_event_loop()
        signals = (signal.SIGINT, signal.SIGTERM)
        for s in signals:
            loop.add_signal_handler(
                s,
                lambda s=s: asyncio.create_task(self._shutdown_handler(loop, signal=s)),
            )

        try:
            await self.initializer(self.sdk)
            await self.subscriber.start()
            asyncio.create_task(self.runner(self, self.sdk))
        except Exception as e:
            self.logger.error(f"error starting subscriber: {e}")
            await self._shutdown_handler(loop)


RunnerFunc = Callable[[TriggerRunner, KaiSDK], Awaitable[None]]
