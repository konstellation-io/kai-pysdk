from dataclasses import asdict
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from redis import Redis
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
from sdk.predictions.store import Predictions
from sdk.predictions.types import Filter, Prediction, TimestampRange

PREDICTIONS_ENDPOINT_KEY = "predictions.endpoint"
PREDICTIONS_USERNAME_KEY = "predictions.username"
PREDICTIONS_PASSWORD_KEY = "predictions.password"
PREDICTIONS_INDEX_KEY = "predictions.index"
PREDICTIONS_ENDPOINT = "localhost:6379"
PREDICTIONS_USERNAME = "test_username"
PREDICTIONS_PASSWORD = "test_password"
PREDICTIONS_INDEX_KEY = "test_index"


@pytest.fixture
def m_redis():
    return Mock(spec=Redis)


@pytest.fixture
def m_store(m_redis):
    store = Predictions()
    store.client = m_redis

    return store


@pytest.fixture
def m_prediction():
    return Prediction(
        creation_date=int(datetime.now().timestamp()),
        last_modified=int(datetime.now().timestamp()),
        payload={"test": "test"},
        metadata={
            "version": "test_version",
            "workflow": "test_workflow",
            "workflow_type": "test_workflow_type",
            "process": "test_process",
            "request_id": "test_request_id",
        },
    )


@pytest.fixture
def m_filter():
    return Filter(
        version="test_version",
        workflow="test_workflow",
        workflow_type="test_workflow_type",
        process="test_process",
        request_id="test_request_id",
        creation_date=TimestampRange(start_date=datetime.now(), end_date=datetime.now()),
    )


@patch.object(Redis, "__init__", return_value=None)
def test_ok(m_redis_init):
    v.set(PREDICTIONS_ENDPOINT_KEY, PREDICTIONS_ENDPOINT)
    v.set(PREDICTIONS_USERNAME_KEY, PREDICTIONS_USERNAME)
    v.set(PREDICTIONS_PASSWORD_KEY, PREDICTIONS_PASSWORD)
    v.set(PREDICTIONS_INDEX_KEY, PREDICTIONS_INDEX_KEY)

    store = Predictions()

    m_redis_init.assert_called_once_with(
        host="localhost", port=6379, username="test_username", password="test_password"
    )
    assert store.client is not None


def test_malformed_endpoint_ko():
    v.set(PREDICTIONS_ENDPOINT_KEY, "localhost")
    v.set(PREDICTIONS_USERNAME_KEY, PREDICTIONS_USERNAME)
    v.set(PREDICTIONS_PASSWORD_KEY, PREDICTIONS_PASSWORD)
    v.set(PREDICTIONS_INDEX_KEY, PREDICTIONS_INDEX_KEY)

    with pytest.raises(FailedToInitializePredictionsStoreError) as error:
        Predictions()

        assert error == MalformedEndpointError("localhost", "localhost")


@patch.object(Redis, "__init__", side_effect=Exception)
def test_initialization_ko(_):
    v.set(PREDICTIONS_ENDPOINT_KEY, PREDICTIONS_ENDPOINT)
    v.set(PREDICTIONS_USERNAME_KEY, PREDICTIONS_USERNAME)
    v.set(PREDICTIONS_PASSWORD_KEY, PREDICTIONS_PASSWORD)
    v.set(PREDICTIONS_INDEX_KEY, PREDICTIONS_INDEX_KEY)

    with pytest.raises(FailedToInitializePredictionsStoreError):
        Predictions()


def test_save_ko(m_store):
    m_store.client.json.return_value.set.side_effect = Exception

    with pytest.raises(FailedToSavePredictionError):
        m_store.save("test_id", {"test": "test"})


def test_save_wrong_id(m_store):
    with pytest.raises(FailedToSavePredictionError) as error:
        m_store.save("", lambda x: x)

        assert error == FailedToSavePredictionError("", EmptyIdError)


def test_get_ko(m_store):
    m_store.client.json.return_value.get.side_effect = Exception

    with pytest.raises(FailedToGetPredictionError):
        m_store.get("test_id")


def test_get_not_found(m_store):
    m_store.client.json.return_value.get.side_effect = None

    with pytest.raises(FailedToGetPredictionError) as error:
        m_store.get("test_id")

        assert error == FailedToGetPredictionError("test_id", NotFoundError)


def test_get_wrong_id_ko(m_store):
    with pytest.raises(FailedToGetPredictionError) as error:
        m_store.get("")

        assert error == FailedToGetPredictionError("", EmptyIdError)


def test_find_validate_filter_ko(m_store, m_filter):
    m_store._validate_filter = Mock(side_effect=MissingRequiredFilterFieldError(""))

    with pytest.raises(FailedToFindPredictionsError) as error:
        m_store.find(m_filter)

        assert error == FailedToFindPredictionsError(m_filter, MissingRequiredFilterFieldError)

    assert m_store._validate_filter.called
    assert not m_store.client.ft.return_value.search.called


def test_find_missing_required_filter_field_ko(m_store):
    with pytest.raises(FailedToFindPredictionsError) as error:
        m_store.find({})

        assert error == FailedToFindPredictionsError({}, MissingRequiredFilterFieldError)


def test_update_ko(m_store):
    m_store.client.json.return_value.get.side_effect = Exception

    with pytest.raises(FailedToUpdatePredictionError):
        m_store.update("test_id", lambda x: x)


def test_update_prediction_not_found(m_store):
    m_store.client.json.return_value.get.return_value = None

    with pytest.raises(FailedToUpdatePredictionError) as error:
        m_store.update("test_id", lambda x: x)

        assert error == FailedToUpdatePredictionError("test_id", NotFoundError)


def test_update_wrong_id(m_store):
    with pytest.raises(FailedToUpdatePredictionError) as error:
        m_store.update("", lambda x: x)

        assert error == FailedToUpdatePredictionError("", EmptyIdError)


def test_update_failed_to_save_prediction(m_store, m_prediction):
    m_store.client.json.return_value.get.return_value = asdict(m_prediction)
    m_store.client.json.return_value.set.side_effect = Exception

    with pytest.raises(FailedToUpdatePredictionError):
        m_store.update("test_id", lambda x: x)


def test_delete_ko(m_store):
    m_store.client.json.return_value.delete.side_effect = Exception

    with pytest.raises(FailedToDeletePredictionError):
        m_store.delete("test_id")


def test_delete_not_found(m_store):
    m_store.client.json.return_value.delete.return_value = 0

    with pytest.raises(FailedToDeletePredictionError) as error:
        m_store.delete("test_id")

        assert error == FailedToDeletePredictionError("test_id", NotFoundError)


def test_delete_wrong_id(m_store):
    with pytest.raises(FailedToDeletePredictionError) as error:
        m_store.delete("")

        assert error == FailedToDeletePredictionError("", EmptyIdError)


def test_validate_filter_ok(m_store):
    filter_ = Filter(
        creation_date=TimestampRange(start_date=datetime.now(), end_date=datetime.now()),
    )
    m_store._validate_filter(filter_)

    assert filter_.version == Metadata.get_version()


def test_validate_filter_missing_required_filter_field_ko(m_store):
    with pytest.raises(MissingRequiredFilterFieldError):
        m_store._validate_filter(
            Filter(
                creation_date=TimestampRange(start_date=None, end_date=datetime.now()),
            )
        )


def test_build_query_ok(m_store):
    time_ = datetime.now()
    expected_time = int(time_.timestamp() * 1000)
    result = m_store._build_query(
        Filter(
            version="test",
            workflow="test",
            workflow_type="test",
            process="test",
            request_id="test",
            creation_date=TimestampRange(start_date=time_, end_date=time_),
        )
    )

    assert (
        result
        == "@product:{%s} @creation_date:[%s %s] @version:{%s} @workflow:{%s} @workflow_type:{%s} @process:{%s} @request_id:{%s}"
        % (Metadata.get_product(), expected_time, expected_time, "test", "test", "test", "test", "test")
    )


def test_build_query_optional_fields_ok(m_store):
    v.set("metadata.version", "test_version")
    time_ = datetime.now()
    expected_time = int(time_.timestamp() * 1000)
    result = m_store._build_query(
        Filter(
            creation_date=TimestampRange(start_date=time_, end_date=time_),
        )
    )

    assert result == "@product:{%s} @creation_date:[%s %s] @version:{%s}" % (
        Metadata.get_product(),
        expected_time,
        expected_time,
        None,  # version is not set by default here
    )
