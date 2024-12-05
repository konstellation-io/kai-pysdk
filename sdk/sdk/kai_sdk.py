from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass, field
from typing import Optional

import loguru
from loguru import logger
from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext

from sdk.centralized_config.centralized_config import CentralizedConfig, CentralizedConfigABC
from sdk.ephemeral_storage.ephemeral_storage import EphemeralStorage, EphemeralStorageABC
from sdk.kai_nats_msg_pb2 import KaiNatsMessage
from sdk.measurements.measurements import Measurements, MeasurementsABC
from sdk.messaging.messaging import Messaging, MessagingABC
from sdk.metadata.metadata import Metadata, MetadataABC
from sdk.model_registry.model_registry import ModelRegistry, ModelRegistryABC
from sdk.persistent_storage.persistent_storage import PersistentStorage, PersistentStorageABC
from sdk.predictions.store import Predictions, PredictionsABC

LOGGER_FORMAT = (
    "<green>{time:YYYY-MM-DDTHH:mm:ss.SSS}Z</green> "
    "<cyan>{level}</cyan> {extra[context]} <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
    "<level>{message}</level> <level>{extra[metadata]}</level>"
)
DEBUG_ORIGIN = "[SDK]"


@dataclass
class Storage:
    persistent: PersistentStorageABC
    ephemeral: EphemeralStorageABC = field(default=None)


@dataclass
class KaiSDK:
    nc: NatsClient
    js: JetStreamContext
    logger: Optional[loguru.Logger] = None
    request_msg: KaiNatsMessage = field(init=False, default=None)
    metadata: MetadataABC = field(init=False)
    messaging: MessagingABC = field(init=False)
    centralized_config: CentralizedConfigABC = field(init=False)
    model_registry: ModelRegistryABC = field(init=False)
    measurements: MeasurementsABC = field(init=False)
    storage: Storage = field(init=False)
    predictions: PredictionsABC = field(init=False)

    def __post_init__(self) -> None:
        if not self.logger:
            self._initialize_logger()
        else:
            origin = logger._core.extra["origin"]
            self.logger = self.logger.bind(context=f"{origin}.[SDK]")

        self.centralized_config = CentralizedConfig(js=self.js)
        self.messaging = Messaging(nc=self.nc, js=self.js)
        self.metadata = Metadata()

        try:
            self.measurements = Measurements()
        except Exception as e:
            assert self.logger is not None
            self.logger.error(f"error initializing measurements: {e}")
            sys.exit(1)

        try:
            self.storage = Storage(PersistentStorage(), EphemeralStorage(js=self.js))
        except Exception as e:
            assert self.logger is not None
            self.logger.error(f"error initializing storage: {e}")
            sys.exit(1)

        self.predictions = Predictions()

        try:
            self.model_registry = ModelRegistry()
        except Exception as e:
            assert self.logger is not None
            self.logger.error(f"error initializing model registry: {e}")
            sys.exit(1)

    async def initialize(self) -> None:
        try:
            await self.storage.ephemeral.initialize()
        except Exception as e:
            assert self.logger is not None
            origin = logger._core.extra["origin"]
            if origin != DEBUG_ORIGIN:
                self.logger.error(f"error initializing object store: {e}")
                raise e
            else:
                self.logger.error(f"error initializing object store: {e}")
                asyncio.get_event_loop().stop()
                sys.exit(1)

        try:
            await self.centralized_config.initialize()
        except Exception as e:
            assert self.logger is not None
            origin = logger._core.extra["origin"]
            if origin != DEBUG_ORIGIN:
                self.logger.error(f"error initializing centralized configuration: {e}")
                raise e
            else:
                self.logger.error(f"error initializing centralized configuration: {e}")
                asyncio.get_event_loop().stop()
                sys.exit(1)

    def get_request_id(self) -> str | None:
        request_msg = getattr(self, "request_msg", None)
        return self.request_msg.request_id if request_msg else None

    def _initialize_logger(self) -> None:
        logger.remove()  # Remove the pre-configured handler
        logger.add(
            sys.stdout,
            colorize=True,
            format=LOGGER_FORMAT,
            backtrace=True,
            diagnose=True,
            level="TRACE",
        )
        logger.configure(extra={"context": "", "metadata": {}, "origin": DEBUG_ORIGIN})

        self.logger = logger.bind(context=DEBUG_ORIGIN)
        self.logger.warning("no logger provided, default sdk logger initialized")

    def set_request_msg(self, request_msg: KaiNatsMessage) -> None:
        self.request_msg = request_msg
        assert isinstance(self.messaging, Messaging)
        self.messaging.request_msg = request_msg
        self.predictions.request_id = request_msg.request_id
        origin = logger._core.extra["origin"]
        logger.configure(extra={"metadata": {"request_id": request_msg.request_id}, "origin": origin})
