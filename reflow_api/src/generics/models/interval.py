import json
from datetime import datetime
from dateutil import relativedelta
from sqlalchemy.types import Text, TypeDecorator


class Interval(TypeDecorator):
    """Base class representing a time interval."""

    impl = Text

    cache_ok = True

    attr_keys = {
        datetime.timedelta: ("days", "seconds", "microseconds"),
        relativedelta.relativedelta: (
            "years",
            "months",
            "days",
            "leapdays",
            "hours",
            "minutes",
            "seconds",
            "microseconds",
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "microsecond",
        ),
    }

    def process_bind_param(self, value, dialect):
        if isinstance(value, tuple(self.attr_keys)):
            attrs = {key: getattr(value, key) for key in self.attr_keys[type(value)]}
            return json.dumps({"type": type(value).__name__, "attrs": attrs})
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if not value:
            return value
        data = json.loads(value)
        if isinstance(data, dict):
            type_map = {key.__name__: key for key in self.attr_keys}
            return type_map[data["type"]](**data["attrs"])
        return data
