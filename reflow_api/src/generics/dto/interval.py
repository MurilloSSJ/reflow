from typing import Annotated, Any
from dateutil import relativedelta
from datetime import timedelta
from src.generics.models.interval import Interval
from pydantic import PlainSerializer, PlainValidator, ValidationInfo


def serialize_interval(value: Interval) -> Interval:
    interval = Interval()
    return interval.process_bind_param(value, None)


def validate_interval(value: Interval | Any, _info: ValidationInfo) -> Any:
    if (
        isinstance(value, Interval)
        or isinstance(value, timedelta)
        or isinstance(value, relativedelta.relativedelta)
    ):
        return value
    interval = Interval()
    try:
        return interval.process_result_value(value, None)
    except ValueError as e:
        # Interval may be provided in string format (cron),
        # so it must be returned as valid value.
        if isinstance(value, str):
            return value
        raise e


PydanticInterval = Annotated[
    Interval,
    PlainValidator(validate_interval),
    PlainSerializer(serialize_interval, return_type=Interval),
]
