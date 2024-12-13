from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

Payload = dict[str, Any]
UpdatePayloadFunc = Callable[[Payload], Payload]


@dataclass
class Prediction:
    creation_date: int
    last_modified: int
    payload: Payload
    metadata: dict[str, str]


@dataclass
class TimestampRange:
    start_date: datetime
    end_date: datetime


@dataclass
class Filter:
    creation_date: TimestampRange
    version: Optional[str] = None
    workflow: Optional[str] = None
    workflow_type: Optional[str] = None
    process: Optional[str] = None
    request_id: Optional[str] = None
