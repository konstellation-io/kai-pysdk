from typing import Optional


class UndefinedEphemeralStorageError(Exception):
    def __init__(self):
        super().__init__("undefined ephemeral storage")


class FailedToInitializeEphemeralStorageError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed to initialize ephemeral storage"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToCompileRegexpError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed to compile regexp"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToListFilesError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed to list objects from the ephemeral storage"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToGetFileError(Exception):
    def __init__(self, key: str, error: Optional[Exception] = None):
        message = f"failed to get file {key} from the ephemeral storage"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToSaveFileError(Exception):
    def __init__(self, key: str, error: Optional[Exception] = None):
        message = f"failed to save file {key} to the ephemeral storage"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToDeleteFileError(Exception):
    def __init__(self, key: str, error: Optional[Exception] = None):
        message = f"failed to delete file {key} from the ephemeral storage"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToPurgeFilesError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed to purge objects from the ephemeral storage"
        super().__init__(f"{message}: {error}" if error else message)
