"""Scalar types for the IRI Facility API"""

import datetime
import enum
from typing import Annotated

from pydantic import BeforeValidator, WithJsonSchema


# -----------------------------------------------------------------------
# StrictHTTPBool: a strict boolean type
def _validate_strict_bool(value) -> bool:
    """Validate the input value as a strict boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v == "true":
            return True
        if v == "false":
            return False
        raise ValueError("Invalid boolean value. Expected 'true' or 'false'.")
    raise ValueError("Invalid boolean value. Expected true/false or 'true'/'false'.")


StrictHTTPBool = Annotated[
    bool,
    BeforeValidator(_validate_strict_bool),
    WithJsonSchema(
        {
            "type": "boolean",
            "description": "Strict boolean. Only true/false allowed (bool or string).",
            "example": True,
        }
    ),
]


# -----------------------------------------------------------------------
# StrictDateTime: a strict ISO8601 datetime type
def _normalize_datetime(dt: datetime.datetime) -> datetime.datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def _validate_strict_datetime(value) -> datetime.datetime:
    """Validate the input value as a strict ISO8601 datetime."""
    if isinstance(value, datetime.datetime):
        return _normalize_datetime(value)
    if not isinstance(value, str):
        raise ValueError("Invalid datetime value. Expected ISO8601 datetime string.")
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        dt = datetime.datetime.fromisoformat(v)
    except Exception as ex:
        raise ValueError("Invalid datetime format. Expected ISO8601 string.") from ex

    return _normalize_datetime(dt)


StrictDateTime = Annotated[
    datetime.datetime,
    BeforeValidator(_validate_strict_datetime),
    WithJsonSchema(
        {
            "type": "string",
            "format": "date-time",
            "description": "Strict ISO8601 datetime. Only valid ISO8601 datetime strings are accepted.",
            "example": "2026-02-21T12:00:00Z",
        }
    ),
]


# -----------------------------------------------------------------------
# AllocationUnit: an enum for allocation units
class AllocationUnit(enum.Enum):
    """Units for allocation"""

    node_hours = "node_hours"
    bytes = "bytes"
    inodes = "inodes"
