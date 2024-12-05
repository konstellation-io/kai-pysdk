from typing import Optional


class FailedToGetMaxMessageSizeError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "failed to get max message size"
        super().__init__(f"{message}: {error}" if error else message)


class MessageTooLargeError(Exception):
    def __init__(self, message_size: str, max_message_size: str):
        super().__init__(f"message size {message_size} is larger than max message size {max_message_size}")


class NewRequestMsgError(Exception):
    def __init__(self, error: Optional[Exception] = None):
        message = "error creating new request message"
        super().__init__(f"{message}: {error}" if error else message)
