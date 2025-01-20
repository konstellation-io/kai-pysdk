from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import timedelta
from functools import reduce

import loguru
from loguru import logger
from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext
from vyper import v

from kaisdk.runner.common.common import LogEncoding, LogLevel
from kaisdk.runner.exceptions import FailedLoadingConfigError, JetStreamConnectionError, NATSConnectionError
from kaisdk.runner.task.task_runner import TaskRunner
from kaisdk.runner.trigger.trigger_runner import TriggerRunner

LOGGER_FORMAT = (
    "<green>{time:YYYY-MM-DDTHH:mm:ss.SSS}Z</green> "
    "<cyan>{level}</cyan> {extra[context]} <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
    "<level>{message}</level> <level>{extra[metadata]}</level>"
)

MANDATORY_CONFIG_KEYS = [
    "metadata.product_id",
    "metadata.version_tag",
    "metadata.workflow_name",
    "metadata.workflow_type",
    "metadata.process_name",
    "metadata.process_type",
    "nats.url",
    "nats.stream",
    "nats.output",
    "centralized_configuration.global.bucket",
    "centralized_configuration.product.bucket",
    "centralized_configuration.workflow.bucket",
    "centralized_configuration.process.bucket",
    "minio.endpoint",
    "minio.client_user",  # generated user for the bucket
    "minio.client_password",  # generated user's password for the bucket
    "minio.ssl",  # Enable or disable SSL
    "minio.bucket",  # Bucket to be used
    "auth.endpoint",  # keycloak endpoint
    "auth.client",  # Client to be used to authenticate
    "auth.client_secret",  # Client's secret to be used
    "auth.realm",  # Realm
    "predictions.endpoint",
    "predictions.username",
    "predictions.password",
    "predictions.index",
    "measurements.endpoint",
    "measurements.insecure",
    "measurements.timeout",
    "measurements.metrics_interval",
]


def custom_sink(encoding: str, path: str):
    def configure_sink(message: loguru.Message):
        result = message

        if encoding == "json":
            record = message.record

            filepath = record["file"].path
            if filepath:
                domain = filepath.split("/")
                if len(domain) > 1:
                    filepath = f"{domain[-2]}/{domain[-1]}"
                else:
                    filepath = domain[0]
                filepath = filepath + ":" + str(record["line"])

            metadata = {}
            if record["extra"]["metadata"]:
                for key, value in record["extra"]["metadata"].items():
                    metadata[key] = value

            serialized_log = {
                "level": record["level"].name,
                "ts": record["time"].timestamp(),
                "logger": record["extra"]["context"],
                "caller": filepath,
                "msg": record["message"],
                **metadata,
            }
            result = json.dumps(serialized_log)

        if path == "stdout":
            print(result, file=sys.stdout)
        elif path == "stderr":
            print(result, file=sys.stderr)
        else:
            with open(path, "a") as file:
                print(result, file=file)

    return configure_sink


@dataclass
class LoggerOptions:
    level: LogLevel = LogLevel.WARNING
    encoding: LogEncoding = LogEncoding.JSON
    output_paths: list[str] = field(default_factory=lambda: ["stdout"])
    error_output_paths: list[str] = field(default_factory=lambda: ["stderr"])


@dataclass
class NatsOptions:
    ack_wait_time: timedelta = timedelta(hours=22)


@dataclass
class RunnerOptions:
    logger_options: LoggerOptions = field(default_factory=LoggerOptions)
    nats_options: NatsOptions = field(default_factory=NatsOptions)

    def to_vyper(self) -> None:
        v.set("runner.logger.level", self.logger_options.level)
        v.set("runner.logger.encoding", self.logger_options.encoding)
        v.set("runner.logger.output_paths", self.logger_options.output_paths)
        v.set("runner.logger.error_output_paths", self.logger_options.error_output_paths)
        v.set("runner.subscriber.ack_wait_time", self.nats_options.ack_wait_time.total_seconds())


@dataclass
class Runner:
    nc: NatsClient = NatsClient()
    js: JetStreamContext = field(init=False)
    logger: loguru.Logger = field(init=False)
    user_options: RunnerOptions = field(default_factory=RunnerOptions)

    def __post_init__(self) -> None:
        self.initialize_config()
        self.initialize_logger()

    async def initialize(self) -> Runner:
        try:
            self.js = self.nc.jetstream()
        except Exception as e:
            self.logger.error(f"error connecting to jetstream: {e}")
            raise JetStreamConnectionError(e)

        try:
            await self.nc.connect(v.get_string("nats.url"))
        except Exception as e:
            self.logger.error(f"error connecting to nats: {e}")
            raise NATSConnectionError(e)

        return self

    def _validate_config(self, keys: dict[str]) -> None:
        for key in MANDATORY_CONFIG_KEYS:
            try:
                _ = reduce(lambda d, k: d[k], key.split("."), keys)
            except Exception:
                raise FailedLoadingConfigError(Exception(f"missing mandatory configuration key: {key}"))

    def initialize_config(self) -> None:
        v.set_env_prefix("KAI")
        v.automatic_env()

        v.set_config_name("app")
        v.set_config_type("yaml")
        v.add_config_path(".")

        if v.is_set("APP_CONFIG_PATH"):
            v.add_config_path(v.get_string("APP_CONFIG_PATH"))

        error = None
        try:
            v.merge_in_config()
        except Exception as e:
            error = e

        if len(v.all_keys()) == 0:
            raise FailedLoadingConfigError(error)

        self.user_options.to_vyper()

        self._validate_config(v.all_settings())

        v.set_default("minio.internal_folder", ".kai")
        v.set_default("model_registry.folder_name", ".models")

    def initialize_logger(self) -> None:
        encoding = v.get_string("runner.logger.encoding")
        output_paths = v.get("runner.logger.output_paths")
        error_output_paths = v.get("runner.logger.error_output_paths")
        level = v.get_string("runner.logger.level")

        logger.remove()  # Remove the pre-configured handler
        for output_path in output_paths:
            logger.add(
                custom_sink(encoding, output_path),
                colorize=True,
                format=LOGGER_FORMAT,
                backtrace=False,
                diagnose=False,
                level=level,
                filter=lambda level: level["level"].name != "ERROR" and level["level"].name != "CRITICAL",
            )

        for error_output_path in error_output_paths:
            logger.add(
                custom_sink(encoding, error_output_path),
                colorize=True,
                format=LOGGER_FORMAT,
                backtrace=True,
                diagnose=True,
                level="ERROR",
            )

        logger.configure(extra={"context": "", "metadata": {}, "origin": "[RUNNER]"})

        self.logger = logger.bind(context="[RUNNER]")
        self.logger.info("logger initialized")

    def trigger_runner(self) -> TriggerRunner:
        return TriggerRunner(self.nc, self.js)

    def task_runner(self) -> TaskRunner:
        return TaskRunner(self.nc, self.js)

    def with_logger_level(self, level: LogLevel):
        self.user_options.logger_options.level = level
        return self

    def with_logger_encoding(self, encoding: LogEncoding):
        self.user_options.logger_options.encoding = encoding
        return self

    def with_logger_output_paths(self, output_paths: list[str]):
        self.user_options.logger_options.output_paths = output_paths
        return self

    def with_logger_error_output_paths(self, error_output_paths: list[str]):
        self.user_options.logger_options.error_output_paths = error_output_paths
        return self

    def with_nats_ack_wait_time(self, ack_wait_time: timedelta):
        self.user_options.nats_options.ack_wait_time = ack_wait_time
        return self
