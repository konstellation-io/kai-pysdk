import io
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import urllib3
from minio import Minio
from vyper import v

from sdk.auth.authentication import Authentication
from sdk.persistent_storage.exceptions import (
    FailedToDeleteFileError,
    FailedToGetFileError,
    FailedToInitializePersistentStorageError,
    FailedToSaveFileError,
)
from sdk.persistent_storage.persistent_storage import Object, ObjectInfo, PersistentStorage, PersistentStorageABC

TTL_DAYS = 30
EXPIRE_DATETIME = datetime.strptime("Sat, 18 Nov 2023 00:00:00 GMT", "%a, %d %b %Y %H:%M:%S %Z")
EXPECTED_OBJECT_INFO = ObjectInfo(
    key="test-key",
    version="test-version",
    expires=EXPIRE_DATETIME,
)

EXPECTED_OBJECT = Object(
    key="test-key",
    version="test-version",
    expires=EXPIRE_DATETIME,
    data=b"test-payload",
)


@pytest.fixture(scope="function")
@patch.object(Minio, "__new__", return_value=Mock(spec=Minio))
def m_persistent_storage(minio_mock: Mock) -> PersistentStorageABC:
    persistent_storage = PersistentStorage()
    persistent_storage.minio_client = minio_mock
    persistent_storage.minio_bucket_name = "test-minio-bucket"

    return persistent_storage


@pytest.fixture(scope="function")
def m_object() -> urllib3.BaseHTTPResponse:
    object_ = Mock(spec=urllib3.BaseHTTPResponse)
    object_.close.return_value = None
    object_.data = b"test-payload"
    object_.headers = {  # for object response from get method
        "content-length": 12,
        "content-type": "application/octet-stream",
        "x-amz-expiration": 'expiry-date="Sat, 18 Nov 2023 00:00:00 GMT", rule-id="ttl-test.txt"',
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

    persistent_storage = PersistentStorage()

    assert persistent_storage.minio_client is not None
    assert persistent_storage.minio_bucket_name == "test-minio-bucket"


def test_ko():
    with pytest.raises(FailedToInitializePersistentStorageError):
        persistent_storage = PersistentStorage()

        assert persistent_storage.minio_client is None


def test_save_ok(m_persistent_storage, m_object):
    v.set("minio.internal_folder", ".kai")

    m_persistent_storage.minio_client.put_object.return_value = m_object
    payload = io.BytesIO(b"test-payload")

    response = m_persistent_storage.save("test-key", payload, TTL_DAYS)

    m_persistent_storage.minio_client.get_bucket_lifecycle.assert_called_once()
    m_persistent_storage.minio_client.set_bucket_lifecycle.assert_called_once()
    m_persistent_storage.minio_client.put_object.assert_called_once()
    assert response == EXPECTED_OBJECT_INFO


def test_save_internal_folder_error_ko(m_persistent_storage, m_object):
    v.set("minio.internal_folder", ".kai")

    m_persistent_storage.minio_client.put_object.return_value = m_object
    payload = io.BytesIO(b"test-payload")

    response = m_persistent_storage.save(".kai/test-key", payload, TTL_DAYS)

    m_persistent_storage.minio_client.get_bucket_lifecycle.assert_not_called()
    m_persistent_storage.minio_client.set_bucket_lifecycle.assert_not_called()
    m_persistent_storage.minio_client.put_object.assert_not_called()
    assert response is None


def test_save_no_ttl_ok(m_persistent_storage, m_object):
    v.set("minio.internal_folder", ".kai")

    m_persistent_storage.minio_client.put_object.return_value = m_object
    payload = io.BytesIO(b"test-payload")

    response = m_persistent_storage.save("test-key", payload)

    m_persistent_storage.minio_client.get_bucket_lifecycle.assert_not_called()
    m_persistent_storage.minio_client.set_bucket_lifecycle.assert_not_called()
    m_persistent_storage.minio_client.put_object.assert_called_once_with(
        "test-minio-bucket",
        "test-key",
        payload,
        payload.getbuffer().nbytes,
    )

    assert response == EXPECTED_OBJECT_INFO


def test_save_ko(m_persistent_storage):
    m_persistent_storage.minio_client.put_object.side_effect = Exception
    payload = io.BytesIO(b"test-payload")

    with pytest.raises(FailedToSaveFileError):
        m_persistent_storage.save("test-key", payload, TTL_DAYS)

    m_persistent_storage.minio_client.get_bucket_lifecycle.assert_called_once()
    m_persistent_storage.minio_client.set_bucket_lifecycle.assert_called_once()
    m_persistent_storage.minio_client.put_object.assert_called_once()


@patch("sdk.persistent_storage.persistent_storage.PersistentStorage._object_exist", return_value=True)
def test_get_ok(_, m_persistent_storage, m_object):
    m_persistent_storage.minio_client.get_object.return_value = m_object

    payload = m_persistent_storage.get("test-key", "test-version")

    m_persistent_storage.minio_client.get_object.assert_called_once()
    assert payload == EXPECTED_OBJECT


@patch("sdk.persistent_storage.persistent_storage.PersistentStorage._object_exist", return_value=False)
def test_get_not_found_ok(_, m_persistent_storage):
    payload = m_persistent_storage.get("test-key", "test-version")

    m_persistent_storage.minio_client.get_object.assert_not_called()
    assert payload is None


@patch("sdk.persistent_storage.persistent_storage.PersistentStorage._object_exist", return_value=True)
def test_get_ko(_, m_persistent_storage):
    m_persistent_storage.minio_client.get_object.side_effect = Exception

    with pytest.raises(FailedToGetFileError):
        m_persistent_storage.get("test-key", "test-version")

    m_persistent_storage.minio_client.get_object.assert_called_once()


def test_get_internal_folder_error_ko(m_persistent_storage, m_object):
    m_persistent_storage.minio_client.get_object.return_value = m_object

    payload = m_persistent_storage.get(".kai/test-key", "test-version")

    m_persistent_storage.minio_client.get_object.assert_not_called()
    assert payload is None


@patch("sdk.persistent_storage.persistent_storage.PersistentStorage._object_exist", return_value=True)
def test_delete_ok(_, m_persistent_storage):
    m_persistent_storage.minio_client.remove_object.return_value = None

    m_persistent_storage.delete("test-key")

    m_persistent_storage.minio_client.remove_object.assert_called_once()


@patch("sdk.persistent_storage.persistent_storage.PersistentStorage._object_exist", return_value=True)
def test_delete_internal_folder_error_ko(_, m_persistent_storage):
    m_persistent_storage.minio_client.remove_object.return_value = None

    response = m_persistent_storage.delete(".kai/test-key")

    assert response is False


@patch("sdk.persistent_storage.persistent_storage.PersistentStorage._object_exist", return_value=False)
def test_delete_not_found_ok(_, m_persistent_storage):
    m_persistent_storage.minio_client.remove_object.return_value = None

    m_persistent_storage.delete("test-key")

    m_persistent_storage.minio_client.remove_object.assert_not_called()


@patch("sdk.persistent_storage.persistent_storage.PersistentStorage._object_exist", return_value=True)
def test_delete_ko(_, m_persistent_storage):
    m_persistent_storage.minio_client.remove_object.side_effect = Exception

    with pytest.raises(FailedToDeleteFileError):
        m_persistent_storage.delete("test-key")

    m_persistent_storage.minio_client.remove_object.assert_called_once()


def test_list_ok(m_persistent_storage, m_object):
    m_persistent_storage.minio_client.list_objects.return_value = [m_object]
    m_persistent_storage.minio_client.stat_object.side_effect = [m_object]

    objects = m_persistent_storage.list()

    m_persistent_storage.minio_client.list_objects.assert_called_once()
    assert objects == [EXPECTED_OBJECT_INFO]


def test_list_ko(m_persistent_storage):
    m_persistent_storage.minio_client.list_objects.side_effect = Exception

    objects = m_persistent_storage.list()

    m_persistent_storage.minio_client.list_objects.assert_called_once()
    assert objects == []


def test_list_versions_ok(m_persistent_storage, m_object):
    m_persistent_storage.minio_client.list_objects.return_value = [m_object]
    m_persistent_storage.minio_client.stat_object.side_effect = [m_object]

    objects = m_persistent_storage.list_versions("test-key")

    m_persistent_storage.minio_client.list_objects.assert_called_once()
    assert objects == [EXPECTED_OBJECT_INFO]


def test_list_versions_ko(m_persistent_storage):
    m_persistent_storage.minio_client.list_objects.side_effect = Exception

    objects = m_persistent_storage.list_versions("test-key")

    m_persistent_storage.minio_client.list_objects.assert_called_once()
    assert objects == []


def test__object_exist_ok(m_persistent_storage):
    m_persistent_storage.minio_client.stat_object.return_value = None

    exist = m_persistent_storage._object_exist("test-key", "test-version")

    m_persistent_storage.minio_client.stat_object.assert_called_once()
    assert exist


def test__object_exist_ko(m_persistent_storage):
    m_persistent_storage.minio_client.stat_object.side_effect = Exception

    with pytest.raises(Exception):
        m_persistent_storage._object_exist("test-key", "test-version")

        m_persistent_storage.minio_client.stat_object.assert_called_once()


def test__get_expiry_date_ok(m_persistent_storage, m_object):
    expiry_date = m_persistent_storage._get_expiry_date(m_object.metadata.get("x-amz-expiration"))

    assert expiry_date == EXPIRE_DATETIME


def test__get_expiry_date_empty_ok(m_persistent_storage, m_object):
    m_object.metadata = {}
    expiry_date = m_persistent_storage._get_expiry_date(m_object.metadata.get("x-amz-expiration"))

    assert expiry_date is None


def test__get_expiry_date_no_match_ok(m_persistent_storage, m_object):
    m_object.metadata = {"x-amz-expiration": 'expiry="Sat, 18 Nov 2023 00:00:00 GMT"'}
    expiry_date = m_persistent_storage._get_expiry_date(m_object.metadata.get("x-amz-expiration"))

    assert expiry_date is None
