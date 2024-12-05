import io
from unittest.mock import Mock, patch

import pytest
import urllib3
from minio import Minio
from vyper import v

from sdk.auth.authentication import Authentication
from sdk.model_registry.exceptions import (
    FailedToDeleteModelError,
    FailedToGetModelError,
    FailedToInitializeModelRegistryError,
    FailedToSaveModelError,
    InvalidVersionError,
    ModelAlreadyExistsError,
    ModelNotFoundError,
)
from sdk.model_registry.model_registry import Model, ModelInfo, ModelRegistry, ModelRegistryABC

METADATA = {
    "process": "test_process",
    "product": "konstellation",
    "version": "v0.0.1",
    "workflow": "test_workflow",
    "Model_version": "0.0.1",
    "Model_description": "test_description",
    "Model_format": "Pycharm",
}
EXPECTED_MODEL_INFO = ModelInfo(
    name="test-key",
    version=METADATA["Model_version"],
    description=METADATA["Model_description"],
    format=METADATA["Model_format"],
)

EXPECTED_MODEL = Model(
    name="test-key",
    version=METADATA["Model_version"],
    description=METADATA["Model_description"],
    format=METADATA["Model_format"],
    model=b"test-payload",
)


@pytest.fixture(scope="function")
@patch.object(Minio, "__new__", return_value=Mock(spec=Minio))
def m_model_registry(minio_mock: Mock) -> ModelRegistryABC:
    model_registry = ModelRegistry()
    model_registry.minio_client = minio_mock
    model_registry.minio_bucket_name = "test-minio-bucket"
    model_registry.model_folder_name = ".kai/.model"

    return model_registry


@pytest.fixture(scope="function")
def m_model() -> urllib3.BaseHTTPResponse:
    object_ = Mock(spec=urllib3.BaseHTTPResponse)
    object_.close.return_value = None
    object_.data = b"test-payload"
    object_.headers = {  # for object response from get method
        "content-length": 12,
        "content-type": "application/octet-stream",
        "x-amz-meta-product": METADATA["product"],
        "x-amz-meta-version": METADATA["version"],
        "x-amz-meta-workflow": METADATA["workflow"],
        "x-amz-meta-process": METADATA["process"],
        "x-amz-meta-model_version": METADATA["Model_version"],
        "x-amz-meta-model_description": METADATA["Model_description"],
        "x-amz-meta-model_format": METADATA["Model_format"],
        "x-amz-version-id": "test-version",
    }
    object_.is_dir = False
    object_.metadata = object_.headers  # for stat_object in list methods
    object_.http_headers = object_.headers  # for object response from save method
    object_.object_name = "test-key"
    object_.version_id = "test-version"

    return object_


@patch.object(Authentication, "__new__", return_value=Mock(spec=Authentication))
@patch.object(Minio, "__new__", return_value=Mock(spec=Minio))
def test_ok(_, __):
    v.set("minio.endpoint", "test-endpoint")
    v.set("minio.client_user", "test-access-key")
    v.set("minio.client_password", "test-secret-key")
    v.set("minio.ssl", False)
    v.set("minio.bucket", "test-minio-bucket")
    v.set("minio.internal_folder", ".kai")
    v.set("metadata.product_id", METADATA["product"])
    v.set("metadata.workflow_name", METADATA["workflow"])
    v.set("metadata.process_name", METADATA["process"])
    v.set("metadata.version_tag", METADATA["version"])
    v.set("model_registry.folder_name", ".model")

    model_registry = ModelRegistry()

    assert model_registry.minio_client is not None
    assert model_registry.minio_bucket_name == "test-minio-bucket"
    assert model_registry.model_folder_name == ".kai/.model"


def test_ko(m_model_registry):
    with pytest.raises(FailedToInitializeModelRegistryError):
        ModelRegistry()

        assert m_model_registry.minio_client is None


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=False)
def test_register_model_ok(_, m_model_registry, m_model):
    v.set("metadata.product_id", METADATA["product"])
    v.set("metadata.workflow_name", METADATA["workflow"])
    v.set("metadata.process_name", METADATA["process"])
    v.set("metadata.version_tag", METADATA["version"])
    m_model_registry.minio_client.put_object.return_value = m_model
    name = "test-key"
    model = io.BytesIO(b"test-payload")

    m_model_registry.register_model(
        model, name, METADATA["Model_version"], METADATA["Model_format"], METADATA["Model_description"]
    )

    m_model_registry.minio_client.put_object.assert_called_once()


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=False)
def test_register_model_without_description_ok(_, m_model_registry, m_model):
    v.set("metadata.product_id", METADATA["product"])
    v.set("metadata.workflow_name", METADATA["workflow"])
    v.set("metadata.process_name", METADATA["process"])
    v.set("metadata.version_tag", METADATA["version"])
    m_model_registry.minio_client.put_object.return_value = m_model
    name = "test-key"
    model = io.BytesIO(b"test-payload")

    m_model_registry.register_model(model, name, METADATA["Model_version"], METADATA["Model_format"])

    m_model_registry.minio_client.put_object.assert_called_once()
    m_model_registry.minio_client.put_object.assert_called_with(
        bucket_name="test-minio-bucket",
        object_name=".kai/.model/test-key",
        data=model,
        length=12,
        metadata={
            "product": METADATA["product"],
            "version": METADATA["version"],
            "workflow": METADATA["workflow"],
            "process": METADATA["process"],
            "Model_version": METADATA["Model_version"],
            "Model_description": "",
            "Model_format": METADATA["Model_format"],
        },
    )


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=False)
def test_register_model_ko(_, m_model_registry):
    m_model_registry.minio_client.put_object.side_effect = Exception
    name = "test-key"
    model = io.BytesIO(b"test-payload")

    with pytest.raises(FailedToSaveModelError):
        m_model_registry.register_model(
            model, name, METADATA["Model_version"], METADATA["Model_description"], METADATA["Model_format"]
        )

    m_model_registry.minio_client.put_object.assert_called_once()


@patch("sdk.model_registry.model_registry.ModelRegistry.get_model", return_value=EXPECTED_MODEL)
def test_register_model_already_exists_ko(_, m_model_registry, m_model):
    name = "test-key"
    model = io.BytesIO(b"test-payload")

    with pytest.raises(ModelAlreadyExistsError):
        m_model_registry.register_model(
            model, name, METADATA["Model_version"], METADATA["Model_description"], METADATA["Model_format"]
        )


def test_register_model_invalid_version_ko(m_model_registry):
    m_model_registry.minio_client.put_object.side_effect = Exception
    name = "test-key"
    model = io.BytesIO(b"test-payload")

    with pytest.raises(InvalidVersionError):
        m_model_registry.register_model(
            model, name, "invalid version v1", METADATA["Model_description"], METADATA["Model_format"]
        )

    m_model_registry.minio_client.put_object.assert_not_called()


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=True)
def test_get_model_no_version_ok(_, m_model_registry, m_model):
    m_model_registry.minio_client.get_object.return_value = m_model
    m_model_registry.minio_client.stat_object.return_value = m_model

    response = m_model_registry.get_model("test-key")

    m_model_registry.minio_client.get_object.assert_called_once()
    assert response == EXPECTED_MODEL


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=True)
@patch("sdk.model_registry.model_registry.ModelRegistry._get_model_version_from_list")
def test_get_model_with_version_ok(get_model_from_list_mock, _, m_model_registry, m_model):
    get_model_from_list_mock.return_value = m_model, m_model

    response = m_model_registry.get_model("test-key", METADATA["Model_version"])

    m_model_registry.minio_client.get_object.assert_not_called()
    get_model_from_list_mock.assert_called_once()
    assert response == EXPECTED_MODEL


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=False)
def test_get_model_not_found_ko(_, m_model_registry):
    with pytest.raises(FailedToGetModelError):
        m_model_registry.get_model("test-key")

    m_model_registry.minio_client.get_object.assert_not_called()


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=True)
@patch("sdk.model_registry.model_registry.ModelRegistry._get_model_version_from_list")
def test_get_model_with_version_not_found_ko(get_model_from_list_mock, _, m_model_registry):
    get_model_from_list_mock.side_effect = ModuleNotFoundError("test-key", METADATA["Model_version"])

    with pytest.raises(FailedToGetModelError):
        m_model_registry.get_model("test-key", METADATA["Model_version"])

    m_model_registry.minio_client.get_object.assert_not_called()
    get_model_from_list_mock.assert_called_once()


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=True)
def test_get_model_ko(_, m_model_registry):
    m_model_registry.minio_client.get_object.side_effect = Exception

    with pytest.raises(FailedToGetModelError):
        m_model_registry.get_model("test-key")

    m_model_registry.minio_client.get_object.assert_called_once()


def test_get_model_invalid_version_ko(m_model_registry):
    m_model_registry.minio_client.get_object.side_effect = Exception

    with pytest.raises(InvalidVersionError):
        m_model_registry.get_model("test-key", "v1.0.0")

    m_model_registry.minio_client.get_object.assert_not_called()


def test_list_models_ok(m_model_registry, m_model):
    m_model_registry.minio_client.list_objects.return_value = [m_model]
    m_model_registry.minio_client.stat_object.return_value = m_model

    response = m_model_registry.list_models()

    m_model_registry.minio_client.list_objects.assert_called_once()
    assert len(response) == 1
    assert response == [EXPECTED_MODEL_INFO]


def test_list_models_ko(m_model_registry):
    m_model_registry.minio_client.list_objects.side_effect = Exception

    objects = m_model_registry.list_models()

    m_model_registry.minio_client.list_objects.assert_called_once()
    assert objects == []


def test_list_model_versions_ok(m_model_registry, m_model):
    m_model_registry.minio_client.list_objects.return_value = [m_model]
    m_model_registry.minio_client.stat_object.return_value = m_model

    objects = m_model_registry.list_model_versions("test-key")

    m_model_registry.minio_client.list_objects.assert_called_once()
    assert objects == [EXPECTED_MODEL_INFO]


def test_list_model_versions_ko(m_model_registry):
    m_model_registry.minio_client.list_objects.side_effect = Exception

    objects = m_model_registry.list_model_versions("test-key")

    m_model_registry.minio_client.list_objects.assert_called_once()
    assert objects == []


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=True)
def test_delete_model_ok(_, m_model_registry):
    m_model_registry.minio_client.remove_object.return_value = None

    m_model_registry.delete_model("test-key")

    m_model_registry.minio_client.remove_object.assert_called_once()


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=False)
def test_delete_model_not_found_ok(_, m_model_registry):
    m_model_registry.minio_client.remove_object.return_value = None

    m_model_registry.delete_model("test-key")

    m_model_registry.minio_client.remove_object.assert_not_called()


@patch("sdk.model_registry.model_registry.ModelRegistry._object_exist", return_value=True)
def test_delete_model_ko(_, m_model_registry):
    m_model_registry.minio_client.remove_object.side_effect = Exception

    with pytest.raises(FailedToDeleteModelError):
        m_model_registry.delete_model("test-key")

    m_model_registry.minio_client.remove_object.assert_called_once()


def test__object_exist_ok(m_model_registry):
    m_model_registry.minio_client.stat_object.return_value = None

    exist = m_model_registry._object_exist("test-key")

    m_model_registry.minio_client.stat_object.assert_called_once()
    assert exist


def test__object_exist_ko(m_model_registry):
    m_model_registry.minio_client.stat_object.side_effect = Exception

    with pytest.raises(Exception):
        m_model_registry._object_exist("test-key")

        m_model_registry.minio_client.stat_object.assert_called_once()


def test__get_model_version_from_list_ok(m_model_registry, m_model):
    m_model_registry.minio_client.list_objects.return_value = [m_model]
    m_model_registry.minio_client.get_object.return_value = m_model
    m_model_registry.minio_client.stat_object.return_value = m_model

    object_, stats = m_model_registry._get_model_version_from_list("test-key", METADATA["Model_version"])

    m_model_registry.minio_client.list_objects.assert_called_once()
    assert object_ == m_model
    assert stats == m_model


def test__get_model_version_from_list_ko(m_model_registry, m_model):
    m_model_registry.minio_client.list_objects.return_value = []

    with pytest.raises(ModelNotFoundError):
        m_model_registry._get_model_version_from_list("test-key", "v1.0.0")

    m_model_registry.minio_client.list_objects.assert_called_once()
