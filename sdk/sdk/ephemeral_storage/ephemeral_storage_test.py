from unittest.mock import Mock, call

import pytest
from mock import AsyncMock
from nats.aio.client import Client as NatsClient
from nats.js.api import ObjectInfo
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError, ObjectNotFoundError
from nats.js.object_store import ObjectStore as NatsObjectStore
from vyper import v

from sdk.ephemeral_storage.ephemeral_storage import EphemeralStorage, EphemeralStorageABC
from sdk.ephemeral_storage.exceptions import (
    FailedToCompileRegexpError,
    FailedToDeleteFileError,
    FailedToGetFileError,
    FailedToInitializeEphemeralStorageError,
    FailedToListFilesError,
    FailedToPurgeFilesError,
    FailedToSaveFileError,
    UndefinedEphemeralStorageError,
)

KEY_140 = "object:140"
KEY_141 = "object:141"
KEY_142 = "object:142"
LIST_KEYS = [KEY_140, KEY_141, KEY_142]


@pytest.fixture(scope="function")
def m_objects() -> list[ObjectInfo]:
    objects = []
    for key in LIST_KEYS:
        object_info = ObjectInfo(
            name=key,
            deleted=False,
            bucket="test_bucket",
            nuid="test_nuid",
        )
        objects.append(object_info)

    return objects


@pytest.fixture(scope="function")
def m_objects_results(m_objects: list[ObjectInfo]) -> list[NatsObjectStore.ObjectResult]:
    objects_results = []
    for obj in m_objects:
        object_result = NatsObjectStore.ObjectResult(
            info=obj,
            data=b"any",
        )
        objects_results.append(object_result)

    return objects_results


@pytest.fixture(scope="function")
def m_ephemeral_storage() -> EphemeralStorageABC:
    js = Mock(spec=JetStreamContext)

    ephemeral_storage = EphemeralStorage(js=js, ephemeral_storage_name="test_object_store")
    ephemeral_storage.object_store = AsyncMock(spec=NatsObjectStore)

    return ephemeral_storage


def test_ok():
    nc = NatsClient()
    js = nc.jetstream()
    name = "test_object_store"

    v.set("nats.object_store", name)

    ephemeral_storage = EphemeralStorage(js=js, ephemeral_storage_name=name)

    assert ephemeral_storage.js is not None
    assert ephemeral_storage.ephemeral_storage_name == name
    assert ephemeral_storage.object_store is None


async def test_initialize_ok(m_ephemeral_storage):
    m_ephemeral_storage.object_store = None
    name = "test_object_store"
    fake_object_store = AsyncMock(spec=NatsObjectStore)
    m_ephemeral_storage.js.object_store.return_value = fake_object_store

    m_ephemeral_storage.ephemeral_storage_name = name

    await m_ephemeral_storage.initialize()

    assert m_ephemeral_storage.object_store == fake_object_store


async def test_initialize_undefined_object_store_name(m_ephemeral_storage):
    m_ephemeral_storage.ephemeral_storage_name = None
    m_ephemeral_storage.object_store = None

    await m_ephemeral_storage.initialize()

    assert m_ephemeral_storage.object_store is None


async def test_initialize_ko(m_ephemeral_storage):
    m_ephemeral_storage.object_store = None
    m_ephemeral_storage.js.object_store.side_effect = Exception
    name = "test_object_store"
    m_ephemeral_storage.ephemeral_storage_name = name

    with pytest.raises(FailedToInitializeEphemeralStorageError):
        await m_ephemeral_storage.initialize()


async def test_list_ok(m_ephemeral_storage, m_objects):
    m_ephemeral_storage.object_store.list.return_value = m_objects

    result = await m_ephemeral_storage.list()

    assert m_ephemeral_storage.object_store.list.called
    assert result == [obj.name for obj in m_objects]


async def test_list_regex_ok(m_ephemeral_storage, m_objects):
    m_ephemeral_storage.object_store.list.return_value = m_objects
    expected = [m_objects[0].name, m_objects[1].name]

    result = await m_ephemeral_storage.list(r"(object:140|object:141)")

    assert m_ephemeral_storage.object_store.list.called
    assert result == expected


async def test_list_regex_ko(m_ephemeral_storage, m_objects):
    m_ephemeral_storage.object_store.list.return_value = m_objects

    with pytest.raises(FailedToCompileRegexpError):
        await m_ephemeral_storage.list(1)


async def test_list_undefined_ko(m_ephemeral_storage):
    m_ephemeral_storage.ephemeral_storage_name = None
    m_ephemeral_storage.object_store = None

    with pytest.raises(UndefinedEphemeralStorageError):
        await m_ephemeral_storage.list()


async def test_list_not_found(m_ephemeral_storage):
    m_ephemeral_storage.object_store.list.side_effect = NotFoundError

    result = await m_ephemeral_storage.list()

    assert m_ephemeral_storage.object_store.list.called
    assert result == []


async def test_list_failed_ko(m_ephemeral_storage):
    m_ephemeral_storage.object_store.list.side_effect = Exception
    with pytest.raises(FailedToListFilesError):
        await m_ephemeral_storage.list()


async def test_get_ok(m_ephemeral_storage):
    expected = AsyncMock(spec=NatsObjectStore.ObjectResult)
    expected.data = b"any"
    m_ephemeral_storage.object_store.get.return_value = expected

    result = await m_ephemeral_storage.get("test-key")

    assert m_ephemeral_storage.object_store.get.called
    assert m_ephemeral_storage.object_store.get.call_args == call("test-key")
    assert result == (expected.data, True)


async def test_get_undefined_ko(m_ephemeral_storage):
    m_ephemeral_storage.ephemeral_storage_name = None
    m_ephemeral_storage.object_store = None

    with pytest.raises(UndefinedEphemeralStorageError):
        await m_ephemeral_storage.get("key-1")


async def test_get_not_found(m_ephemeral_storage):
    m_ephemeral_storage.object_store.get.side_effect = ObjectNotFoundError

    result = await m_ephemeral_storage.get("test-key")

    assert m_ephemeral_storage.object_store.get.called
    assert result == (None, False)


async def test_get_failed_ko(m_ephemeral_storage):
    m_ephemeral_storage.object_store.get.side_effect = Exception

    with pytest.raises(FailedToGetFileError):
        await m_ephemeral_storage.get("test-key")


async def test_save_ok(m_ephemeral_storage):
    m_ephemeral_storage.object_store.get.side_effect = ObjectNotFoundError
    result = await m_ephemeral_storage.save("test-key", b"any")

    assert m_ephemeral_storage.object_store.put.called
    assert m_ephemeral_storage.object_store.put.call_args == call("test-key", b"any")
    assert result is None


async def test_save_overwrite_ok(m_ephemeral_storage, m_objects):
    m_ephemeral_storage.object_store.get.return_value = m_objects[0]
    await m_ephemeral_storage.save(KEY_140, b"any", overwrite=True)

    assert m_ephemeral_storage.object_store.put.called
    assert m_ephemeral_storage.object_store.put.call_args == call(KEY_140, b"any")


async def test_save_undefined_ko(m_ephemeral_storage):
    m_ephemeral_storage.ephemeral_storage_name = None
    m_ephemeral_storage.object_store = None

    with pytest.raises(UndefinedEphemeralStorageError):
        await m_ephemeral_storage.save("key-1", b"any2")


async def test_save_missing_payload_ko(m_ephemeral_storage):
    with pytest.raises(Exception):
        await m_ephemeral_storage.save("key-1")


async def test_save_failed_ko(m_ephemeral_storage):
    m_ephemeral_storage.object_store.put.side_effect = Exception

    with pytest.raises(FailedToSaveFileError):
        await m_ephemeral_storage.save("test-key", b"prueba")


async def test_save_failed_object_already_exists_ko(m_ephemeral_storage):
    m_ephemeral_storage.object_store.get.side_effect = b"object"

    with pytest.raises(FailedToSaveFileError) as e:
        await m_ephemeral_storage.save("test-key", b"prueba")

        e.message == FailedToSaveFileError(key="test-key", error="file already exists")


async def test_delete_ok(m_ephemeral_storage, m_objects, m_objects_results):
    m_ephemeral_storage.object_store.list.return_value = m_objects
    deleted_object = m_objects_results[0]
    deleted_object.info.deleted = True
    m_ephemeral_storage.object_store.delete.return_value = deleted_object

    result = await m_ephemeral_storage.delete(KEY_140)

    assert result


async def test_delete_undefined_ko(m_ephemeral_storage):
    m_ephemeral_storage.ephemeral_storage_name = None
    m_ephemeral_storage.object_store = None

    with pytest.raises(UndefinedEphemeralStorageError):
        await m_ephemeral_storage.delete("key-1")


async def test_delete_not_found_ko(m_ephemeral_storage):
    m_ephemeral_storage.object_store.delete.side_effect = ObjectNotFoundError

    result = await m_ephemeral_storage.delete("key-1")

    assert not result


async def test_delete_failed_ko(m_ephemeral_storage):
    m_ephemeral_storage.object_store.delete.side_effect = Exception

    with pytest.raises(FailedToDeleteFileError):
        await m_ephemeral_storage.delete("test-key")


async def test_purge_ok(m_ephemeral_storage, m_objects, m_objects_results):
    m_ephemeral_storage.object_store.list.return_value = [obj for obj in m_objects]
    for obj in m_objects_results:
        obj.info.deleted = True
    m_ephemeral_storage.object_store.delete.side_effect = m_objects_results

    result = await m_ephemeral_storage.purge()

    assert result is None
    assert m_ephemeral_storage.object_store.delete.call_count == 3
    assert m_ephemeral_storage.object_store.delete.call_args_list == [
        call(KEY_140),
        call(KEY_141),
        call(KEY_142),
    ]


async def test_purge_one_already_deleted_ok(m_ephemeral_storage, m_objects, m_objects_results):
    m_ephemeral_storage.object_store.list.return_value = [obj for obj in m_objects]
    for obj in m_objects_results[1:]:
        obj.info.deleted = True

    m_ephemeral_storage.object_store.delete.side_effect = m_objects_results

    result = await m_ephemeral_storage.purge()

    assert result is None
    assert m_ephemeral_storage.object_store.delete.call_count == 3
    assert m_ephemeral_storage.object_store.delete.call_args_list == [
        call(KEY_140),
        call(KEY_141),
        call(KEY_142),
    ]


async def test_purge_regex_ok(m_ephemeral_storage, m_objects, m_objects_results):
    m_ephemeral_storage.object_store.list.return_value = [m_objects[0], m_objects[2]]
    for obj in m_objects_results:
        obj.info.deleted = True
    m_ephemeral_storage.object_store.delete.side_effect = [m_objects_results[0], m_objects_results[2]]

    result = await m_ephemeral_storage.purge(r"(object:140|object:142)")

    assert result is None
    assert m_ephemeral_storage.object_store.delete.call_count == 2
    assert m_ephemeral_storage.object_store.delete.call_args_list == [call(KEY_140), call(KEY_142)]


async def test_purge_undefined_ko(m_ephemeral_storage):
    m_ephemeral_storage.ephemeral_storage_name = None
    m_ephemeral_storage.object_store = None

    with pytest.raises(UndefinedEphemeralStorageError):
        await m_ephemeral_storage.purge()


async def test_purge_regex_ko(m_ephemeral_storage):
    with pytest.raises(FailedToCompileRegexpError):
        await m_ephemeral_storage.purge(1)


async def test_purge_failed_ko(m_ephemeral_storage, m_objects):
    m_ephemeral_storage.object_store.list.return_value = [m_objects[0], m_objects[2]]
    m_ephemeral_storage.object_store.delete.side_effect = Exception

    with pytest.raises(FailedToPurgeFilesError):
        await m_ephemeral_storage.purge()
