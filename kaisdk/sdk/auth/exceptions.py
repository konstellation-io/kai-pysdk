from typing import Optional


class FailedToGetTokenError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed to get token"
        super().__init__(f"{message}: {error}" if error else message)
