from typing import Optional


class FailedToInitializePersistentStorageError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed to initialize persistent storage"
        super().__init__(f"{message}: {error}" if error else message)


class MissingBucketError(Exception):
    def __init__(self, bucket: str):
        super().__init__(f"bucket: {bucket} not found in the persistent storage")


class FailedToListFilesError(Exception):
    def __init__(self, bucket: str, error: Optional[Exception] = None):
        message = f"failed to list files from the persistent storage bucket {bucket}"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToGetFileError(Exception):
    def __init__(self, key: str, version: str, bucket: str, error: Optional[Exception] = None):
        message = f"failed to get file {key} with version {version} from the persistent storage bucket {bucket}"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToSaveFileError(Exception):
    def __init__(self, key: str, bucket: str, error: Optional[Exception] = None):
        message = f"failed to save file {key} to the persistent storage bucket {bucket}"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToDeleteFileError(Exception):
    def __init__(self, key: str, version: str, bucket: str, error: Optional[Exception] = None):
        message = f"failed to delete file {key} with version {version} from the persistent storage bucket {bucket}"
        super().__init__(f"{message}: {error}" if error else message)
