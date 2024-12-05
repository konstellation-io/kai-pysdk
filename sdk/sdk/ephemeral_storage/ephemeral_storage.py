from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

import loguru
from loguru import logger
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError, ObjectNotFoundError
from nats.js.object_store import ObjectStore as NatsObjectStore
from vyper import v

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

UNDEFINED_OBJECT_STORE = "undefined ephemeral storage"


@dataclass
class EphemeralStorageABC(ABC):
    @abstractmethod
    async def initialize(self) -> None:
        pass

    @abstractmethod
    async def list(self, regexp: Optional[str]) -> list[str]:
        pass

    @abstractmethod
    async def get(self, key: str) -> tuple[Optional[bytes], bool]:
        pass

    @abstractmethod
    async def save(self, key: str, payload: bytes, overwrite: Optional[bool]) -> None:
        pass

    @abstractmethod
    async def delete(self, key: str) -> bool:
        pass

    @abstractmethod
    async def purge(self, regexp: Optional[str]) -> None:
        pass


@dataclass
class EphemeralStorage(EphemeralStorageABC):
    js: JetStreamContext
    ephemeral_storage_name: Optional[str] = None
    object_store: Optional[NatsObjectStore] = None
    logger: loguru.Logger = field(init=False)

    def __post_init__(self) -> None:
        self.ephemeral_storage_name = v.get_string("nats.object_store")
        origin = logger._core.extra["origin"]
        if self.ephemeral_storage_name:
            self.logger = logger.bind(context=f"{origin}.[EPHEMERAL STORAGE: {self.ephemeral_storage_name}]")
        else:
            self.logger = logger.bind(context=f"{origin}.[EPHEMERAL STORAGE]")

    async def initialize(self) -> None:
        if self.ephemeral_storage_name:
            self.logger.info(f"initializing ephemeral storage {self.ephemeral_storage_name}...")
            self.object_store = await self._init_object_store()
        else:
            self.logger.info("undefined ephemeral storage [skipped]")

    async def _init_object_store(self) -> NatsObjectStore:
        try:
            assert isinstance(self.ephemeral_storage_name, str)
            object_store = await self.js.object_store(self.ephemeral_storage_name)
            self.logger.debug(f"ephemeral storage {self.ephemeral_storage_name} successfully initialized")
            return object_store
        except Exception as e:
            self.logger.warning(f"failed to initialize ephemeral storage '{self.ephemeral_storage_name}': {e}")
            raise FailedToInitializeEphemeralStorageError(error=e)

    async def list(self, regexp: Optional[str] = None) -> list[str]:
        if not self.object_store:
            self.logger.warning(UNDEFINED_OBJECT_STORE)
            raise UndefinedEphemeralStorageError

        try:
            objects = await self.object_store.list(ignore_deletes=True)
        except NotFoundError as e:
            self.logger.debug(f"no files found in ephemeral storage {self.ephemeral_storage_name}: {e}")
            return []
        except Exception as e:
            self.logger.warning(f"failed to list files from ephemeral storage {self.ephemeral_storage_name}: {e}")
            raise FailedToListFilesError(error=e)

        pattern = None
        if regexp:
            try:
                pattern = re.compile(regexp)
            except Exception as e:
                self.logger.warning(f"failed to compile regexp {regexp}: {e}")
                raise FailedToCompileRegexpError(error=e)

        response = []
        for obj in objects:
            obj_name = obj.name
            if not pattern or pattern.match(obj_name):
                response.append(obj_name)

        self.logger.info(f"files successfully listed from ephemeral storage {self.ephemeral_storage_name}")
        return response

    async def get(self, key: str) -> tuple[Optional[bytes], bool]:
        if not self.object_store:
            self.logger.warning(UNDEFINED_OBJECT_STORE)
            raise UndefinedEphemeralStorageError

        try:
            response = await self.object_store.get(key)
            self.logger.info(f"file {key} successfully retrieved from ephemeral storage {self.ephemeral_storage_name}")
            return response.data, True
        except ObjectNotFoundError as e:
            self.logger.debug(f"file {key} not found in ephemeral storage {self.ephemeral_storage_name}: {e}")
            return None, False
        except Exception as e:
            self.logger.warning(f"failed to get file {key} from ephemeral storage {self.ephemeral_storage_name}: {e}")
            raise FailedToGetFileError(key=key, error=e)

    async def save(self, key: str, payload: bytes, overwrite: Optional[bool] = False) -> None:
        if not self.object_store:
            self.logger.warning(UNDEFINED_OBJECT_STORE)
            raise UndefinedEphemeralStorageError

        if not overwrite:
            try:
                await self.object_store.get(key)
                self.logger.warning(f"file {key} already exists in ephemeral storage {self.ephemeral_storage_name}")
                raise FailedToSaveFileError(key=key, error="file already exists")
            except ObjectNotFoundError:
                pass

        try:
            await self.object_store.put(key, payload)
            self.logger.info(f"file {key} successfully saved in ephemeral storage {self.ephemeral_storage_name}")
        except Exception as e:
            self.logger.warning(f"failed to save file {key} in ephemeral storage {self.ephemeral_storage_name}: {e}")
            raise FailedToSaveFileError(key=key, error=e)

    async def delete(self, key: str) -> bool:
        if not self.object_store:
            self.logger.warning(UNDEFINED_OBJECT_STORE)
            raise UndefinedEphemeralStorageError

        try:
            await self.object_store.delete(key)
            # NATS python has a bug not returning anything when deleting an object
            # return info_.info.deleted if info_.info.deleted else False
        except ObjectNotFoundError as e:
            self.logger.debug(f"file {key} not found in ephemeral storage {self.ephemeral_storage_name}: {e}")
            return False
        except Exception as e:
            self.logger.warning(
                f"failed to delete file {key} from ephemeral storage {self.ephemeral_storage_name}: {e}"
            )
            raise FailedToDeleteFileError(key=key, error=e)
        else:
            return True

    async def purge(self, regexp: Optional[str] = None) -> None:
        if not self.object_store:
            self.logger.warning(UNDEFINED_OBJECT_STORE)
            raise UndefinedEphemeralStorageError

        pattern = None
        if regexp:
            try:
                pattern = re.compile(regexp)
            except Exception as e:
                self.logger.warning(f"failed to compile regexp {regexp}: {e}")
                raise FailedToCompileRegexpError(error=e)

        object_names = await self.list()
        deleted = 0
        for name in object_names:
            if not pattern or pattern.match(name):
                self.logger.info(f"deleting file {name} from ephemeral storage {self.ephemeral_storage_name}...")

                try:
                    info_ = await self.object_store.delete(name)
                    if info_.info.deleted:
                        deleted += 1
                        self.logger.info(
                            f"file {name} successfully deleted from ephemeral storage {self.ephemeral_storage_name}"
                        )
                except Exception as e:
                    self.logger.warning(
                        f"failed to delete file {name} from ephemeral storage {self.ephemeral_storage_name}: {e}"
                    )
                    raise FailedToPurgeFilesError(error=e)

        self.logger.info(f"{deleted} files successfully purged from ephemeral storage {self.ephemeral_storage_name}")
