from __future__ import annotations

import ast
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime

import loguru
from loguru import logger
from redis import Redis
from redis.commands.json.path import Path
from vyper import v

from sdk.metadata.metadata import Metadata
from sdk.predictions.exceptions import (
    EmptyIdError,
    FailedToDeletePredictionError,
    FailedToFindPredictionsError,
    FailedToGetPredictionError,
    FailedToInitializePredictionsStoreError,
    FailedToSavePredictionError,
    FailedToUpdatePredictionError,
    MalformedEndpointError,
    MissingRequiredFilterFieldError,
    NotFoundError,
)
from sdk.predictions.types import Filter, Payload, Prediction, UpdatePayloadFunc


@dataclass
class PredictionsABC(ABC):
    @abstractmethod
    def save(self, id: str, payload: Payload) -> None:
        pass

    @abstractmethod
    def get(self, id: str) -> Prediction:
        pass

    @abstractmethod
    def find(self, filter: Filter) -> list[Prediction]:
        pass

    @abstractmethod
    def update(self, id: str, update_payload: UpdatePayloadFunc) -> None:
        pass

    @abstractmethod
    def delete(self, id: str) -> None:
        pass


@dataclass
class Predictions(PredictionsABC):
    logger: loguru.Logger = field(init=False)
    request_id: str = ""
    client: Redis = field(init=False)

    def __post_init__(self):
        origin = logger._core.extra["origin"]
        self.logger = logger.bind(context=f"{origin}.[PREDICTIONS STORE]")
        try:
            try:
                endpoint = v.get_string("predictions.endpoint")
                endpoint_ = endpoint.split(":")
                host = endpoint_[0]
                port = int(endpoint_[1])
            except Exception as e:
                self.logger.error(f"malformed endpoint: {e}")
                raise MalformedEndpointError(v.get_string("predictions.endpoint"), e)

            self.client = Redis(
                host=host,
                port=port,
                username=v.get_string("predictions.username"),
                password=v.get_string("predictions.password"),
            )
        except Exception as e:
            self.logger.error(f"failed to initialize predictions store: {e}")
            raise FailedToInitializePredictionsStoreError(e)

        self.logger.info("successfully initialized predictions store")

    def save(self, id: str, payload: Payload) -> None:
        try:
            if id == "" or id is None:
                self.logger.error(f"{EmptyIdError()}")
                raise EmptyIdError()

            creation_timestamp = datetime.now().timestamp() * 1000  # milliseconds
            key = self._get_key_with_product_prefix(id)
            prediction = Prediction(
                creation_date=int(creation_timestamp),
                last_modified=int(creation_timestamp),
                payload=payload,
                metadata={
                    "product": Metadata.get_product(),
                    "version": Metadata.get_version(),
                    "workflow": Metadata.get_workflow(),
                    "workflow_type": Metadata.get_workflow_type(),
                    "process": Metadata.get_process(),
                    "request_id": self.request_id,
                },
            )
            self.client.json().set(name=key, path=Path.root_path(), obj=asdict(prediction))
        except Exception as e:
            self.logger.error(f"failed to save prediction with {id} to the predictions store: {e}")
            raise FailedToSavePredictionError(id, e)

        self.logger.info(f"successfully saved prediction with {id} to the predictions store")

    def get(self, id: str) -> Prediction:
        try:
            if id == "" or id is None:
                self.logger.error(f"{EmptyIdError()}")
                raise EmptyIdError()

            key = self._get_key_with_product_prefix(id)
            prediction = self.client.json().get(key)

            if prediction is None:
                self.logger.error(f"prediction {id} not found in the predictions store")
                raise NotFoundError(id)

            prediction = Prediction(**prediction)
        except Exception as e:
            self.logger.error(f"failed to get prediction {id} from the predictions store: {e}")
            raise FailedToGetPredictionError(id, e)

        self.logger.info(f"successfully found prediction {id} from the predictions store")
        return prediction

    def find(self, filter: Filter) -> list[Prediction]:
        try:
            self._validate_filter(filter)
            index = v.get_string("predictions.index")
            predictions = self.client.ft(index).search(query=self._build_query(filter))
            result = [Prediction(**ast.literal_eval(prediction["json"])) for prediction in predictions.docs]
            self.logger.info(f"successfully found predictions from the predictions store matching the filter {filter}")
        except Exception as e:
            self.logger.error(
                f"failed to find predictions from the predictions store matching the filter {filter}: {e}"
            )
            raise FailedToFindPredictionsError(filter, e)

        return result

    def update(self, id: str, update_payload: UpdatePayloadFunc) -> None:
        try:
            if id == "" or id is None:
                self.logger.error(f"{EmptyIdError()}")
                raise EmptyIdError()

            key = self._get_key_with_product_prefix(id)
            prediction = self.client.json().get(key)

            if prediction is None:
                self.logger.error(f"prediction {id} not found in the predictions store")
                raise NotFoundError(id)

            payload = prediction["payload"]
            new_payload = update_payload(payload)
            if new_payload is None:
                self.logger.error(f"update function returned None for prediction {id}")
                raise FailedToUpdatePredictionError(id)

            last_modified = int(datetime.now().timestamp() * 1000)  # milliseconds

            updated_prediction = Prediction(
                creation_date=prediction["creation_date"],
                last_modified=last_modified,
                payload=new_payload,
                metadata=prediction["metadata"],
            )

            self.client.json().set(name=key, path=Path.root_path(), obj=asdict(updated_prediction))
        except Exception as e:
            self.logger.error(f"failed to update prediction with {id} to the predictions store: {e}")
            raise FailedToUpdatePredictionError(id, e)

        self.logger.info(f"successfully updated prediction with {id} to the predictions store")

    def delete(self, id: str) -> None:
        try:
            if id == "" or id is None:
                self.logger.error(f"{EmptyIdError()}")
                raise EmptyIdError()

            key = self._get_key_with_product_prefix(id)
            result = self.client.json().delete(key)

            if result == 0:
                self.logger.error(f"prediction {id} not found in the predictions store")
                raise NotFoundError(id)
        except Exception as e:
            self.logger.error(f"failed to delete prediction {id} from the predictions store: {e}")
            raise FailedToDeletePredictionError(id, e)

        self.logger.info(f"successfully deleted prediction {id} from the predictions store")

    def _validate_filter(self, filter: Filter) -> None:
        if filter.version is None:
            filter.version = Metadata.get_version()

        if filter.creation_date is None:
            self.logger.error("filter creation_date is required")
            raise MissingRequiredFilterFieldError("creation_date")

        if filter.creation_date.start_date is None:
            self.logger.error("filter creation_date start_date is required")
            raise MissingRequiredFilterFieldError("creation_date.start_date")

        if filter.creation_date.end_date is None:
            self.logger.error("filter creation_date end_date is required")
            raise MissingRequiredFilterFieldError("creation_date.end_date")

    @staticmethod
    def _build_query(filter: Filter) -> str:
        query = "@product:{%s} @creation_date:[%s %s] @version:{%s}" % (
            Metadata.get_product(),
            int(filter.creation_date.start_date.timestamp() * 1000),  # milliseconds
            int(filter.creation_date.end_date.timestamp() * 1000),  # milliseconds
            filter.version,
        )

        if filter.workflow:
            query = query + " @workflow:{%s}" % filter.workflow

        if filter.workflow_type:
            query = query + " @workflow_type:{%s}" % filter.workflow_type

        if filter.process:
            query = query + " @process:{%s}" % filter.process

        if filter.request_id:
            query = query + " @request_id:{%s}" % filter.request_id

        query = query.replace("-", "\\-")
        query = query.replace(".", "\\.")

        return query

    @staticmethod
    def _get_key_with_product_prefix(key: str) -> str:
        return f"{Metadata.get_product()}:{key}"
