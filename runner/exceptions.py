from typing import Optional


class FailedLoadingConfigError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "configuration could not be loaded"
        super().__init__(f"{message}: {error}" if error else message)


class NATSConnectionError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed connecting to nats"
        super().__init__(f"{message}: {error}" if error else message)


class JetStreamConnectionError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed connecting to jetstream"
        super().__init__(f"{message}: {error}" if error else message)
