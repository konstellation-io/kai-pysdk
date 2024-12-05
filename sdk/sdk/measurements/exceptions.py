from typing import Optional


class FailedToInitializeMeasurementsError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed to initialize measurements"
        super().__init__(f"{message}: {error}" if error else message)
