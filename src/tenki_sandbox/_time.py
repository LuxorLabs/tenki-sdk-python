from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp


def seconds(value: float | int | object | None) -> Optional[float]:
    if value is None:
        return None
    total_seconds = getattr(value, "total_seconds", None)
    if callable(total_seconds):
        return float(total_seconds())
    return float(value)


def duration(value: float | int | object | None) -> Optional[Duration]:
    secs = seconds(value)
    if secs is None:
        return None
    out = Duration()
    out.FromSeconds(int(secs))
    nanos = int((secs - int(secs)) * 1_000_000_000)
    if nanos:
        out.nanos = nanos
    return out


def duration_ms(value: float | int | object | None) -> int:
    secs = seconds(value)
    if secs is None:
        return 0
    return max(0, int(secs * 1000))


def timestamp(value: datetime | None) -> Optional[Timestamp]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    out = Timestamp()
    out.FromDatetime(value)
    return out


def datetime_from_proto(value: Timestamp | None) -> datetime | None:
    if value is None:
        return None
    return value.ToDatetime(tzinfo=timezone.utc)

