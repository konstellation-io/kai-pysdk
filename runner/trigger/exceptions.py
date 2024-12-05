from typing import Optional


class UndefinedRunnerFunctionError(Exception):
    def __init__(self):
        message = "undefined runner function"
        super().__init__(message)


class NotValidProtobuf(Exception):
    def __init__(self, subject: str, error: Optional[Exception] = None):
        message = f"error parsing message data coming from subject {subject} because is not a valid protobuf"
        super().__init__(f"{message}: {error}" if error else message)


class NewRequestMsgError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "error creating new request message"
        super().__init__(f"{message}: {error}" if error else message)


class UndefinedResponseHandlerError(Exception):
    def __init__(self, subject: str):
        message = f"undefined response handler from subject {subject}"
        super().__init__(message)


class HandlerError(Exception):
    def __init__(self, node_from: str, node_to: str, error: Optional[Exception] = None):
        message = f"error in node {node_from} executing handler for node {node_to}"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToInitializeMetricsError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "error initializing metrics"
        super().__init__(f"{message}: {error}" if error else message)
