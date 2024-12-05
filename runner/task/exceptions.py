from typing import Optional


class UndefinedDefaultHandlerFunctionError(Exception):
    def __init__(self):
        super().__init__("Undefined default handler")


class NotValidProtobuf(Exception):
    def __init__(self, subject: str, error: Optional[Exception] = None):
        message = f"error parsing message data coming from subject {subject} because is not a valid protobuf"
        super().__init__(f"{message}: {error}" if error else message)


class NewRequestMsgError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "error creating new request message"
        super().__init__(f"{message}: {error}" if error else message)


class HandlerError(Exception):
    def __init__(self, node_from: str, node_to: str, error: Optional[Exception] = None, type: str = "handler"):
        message = f"error in node {node_from} executing {type} for node {node_to}"
        super().__init__(f"{message}: {error}" if error else message)


class FailedToInitializeMetricsError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "error initializing metrics"
        super().__init__(f"{message}: {error}" if error else message)
