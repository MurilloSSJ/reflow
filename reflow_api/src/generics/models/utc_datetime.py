from datetime import datetime
import pendulum
from sqlalchemy import TIMESTAMP
from sqlalchemy.types import TypeDecorator


class UtcDateTime(TypeDecorator):
    """
    Similar to :class:`~sqlalchemy.types.TIMESTAMP` with ``timezone=True`` option, with some differences.

    - Never silently take naive :class:`~datetime.datetime`, instead it
      always raise :exc:`ValueError` unless time zone aware value.
    - :class:`~datetime.datetime` value's :attr:`~datetime.datetime.tzinfo`
      is always converted to UTC.
    - Unlike SQLAlchemy's built-in :class:`~sqlalchemy.types.TIMESTAMP`,
      it never return naive :class:`~datetime.datetime`, but time zone
      aware value, even with SQLite or MySQL.
    - Always returns TIMESTAMP in UTC.
    """

    impl = TIMESTAMP(timezone=True)

    cache_ok = True

    def process_bind_param(self, value):
        if not isinstance(value, datetime):
            if value is None:
                return None
            raise TypeError(f"expected datetime.datetime, not {value!r}")
        elif value.tzinfo is None:
            raise ValueError("naive datetime is disallowed")
        return value.astimezone(pendulum.UTC)

    def process_result_value(self, value):
        """
        Process DateTimes from the DB making sure to always return UTC.

        Not using timezone.convert_to_utc as that converts to configured TIMEZONE
        while the DB might be running with some other setting. We assume UTC
        datetimes in the database.
        """
        if value is not None:
            if value.tzinfo is None:
                value = value.replace(tzinfo=pendulum.UTC)
            else:
                value = value.astimezone(pendulum.UTC)

        return value
