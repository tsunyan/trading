"""Strict primitives shared by private JSON adapters; never echo wire values."""

import re
from datetime import timedelta
from decimal import Decimal

from pydantic import AwareDatetime, TypeAdapter

_TIME = TypeAdapter(AwareDatetime)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def decimal_string(value):
    if not isinstance(value, str) or not re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", value):
        raise ValueError("invalid_numeric_field")
    return Decimal(value)


def positive_id(value):
    if type(value) is not int or value <= 0:
        raise ValueError("invalid_identity")
    return value


def timestamp_string(value):
    if not isinstance(value, str) or not re.match(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T", value):
        raise ValueError("invalid_timestamp")
    return _TIME.validate_python(value)


def clock_skew(value):
    if type(value) is not int or not 0 <= value <= 1000:
        raise ValueError("invalid_clock_skew_ms")
    return timedelta(milliseconds=value)
