from typing import Optional


class FailedToInitializeConfigError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed to initialize configuration"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToGetConfigError(Exception):
    def __init__(self, key: str, scope: str, error: Optional[Exception] = None):
        message = f"failed to get configuration given key {key} and scope {scope}"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToSetConfigError(Exception):
    def __init__(self, key: str, scope: str, error: Optional[Exception] = None):
        message = f"failed to set configuration given key {key} and scope {scope}"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToDeleteConfigError(Exception):
    def __init__(self, key: str, scope: str, error: Optional[Exception] = None):
        message = f"failed to delete configuration given key {key} and scope {scope}"
        super().__init__(f"{message}: {error}" if error else message)
