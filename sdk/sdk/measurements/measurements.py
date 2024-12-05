from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import loguru
from grpc import Compression
from loguru import logger
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Meter, get_meter, set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from vyper import v

from sdk.measurements.exceptions import FailedToInitializeMeasurementsError
from sdk.metadata.metadata import Metadata


@dataclass
class MeasurementsABC(ABC):
    @abstractmethod
    def get_metrics_client(self) -> Meter:
        pass


@dataclass
class Measurements(MeasurementsABC):
    logger: loguru.Logger = field(init=False)
    metricsClient: Meter = field(init=False)

    def __post_init__(self):
        origin = logger._core.extra["origin"]
        self.logger = logger.bind(context=f"{origin}.[MEASUREMENTS]")
        try:
            resource = Resource(
                {
                    "service.product": Metadata.get_product(),
                    "service.workflow": Metadata.get_workflow(),
                    "service.process": Metadata.get_process(),
                    "service.version": Metadata.get_version(),
                }
            )
            endpoint = v.get_string("measurements.endpoint")
            insecure = v.get_bool("measurements.insecure")
            timeout = v.get_int("measurements.timeout")
            self._setup_metrics(resource, endpoint, insecure, timeout)
        except Exception as e:
            self.logger.error(f"failed to initialize measurements: {e}")
            raise FailedToInitializeMeasurementsError(e)

        self.logger.info("successfully initialized measurements")

    def get_metrics_client(self) -> Meter:
        return self.metricsClient

    def _setup_metrics(self, resource: Resource, endpoint: str, insecure: bool, timeout: int):
        reader = PeriodicExportingMetricReader(
            export_interval_millis=v.get_int("measurements.metrics_interval") * 1000,
            exporter=OTLPMetricExporter(
                endpoint=endpoint,
                insecure=insecure,
                timeout=timeout,
                compression=Compression(Compression.Gzip),
            ),
        )
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        set_meter_provider(provider)
        self.metricsClient = get_meter(__name__)
