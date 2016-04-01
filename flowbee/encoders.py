"""Helper classes for parsers.

This is taken directly from Django Rest Framework:
https://github.com/tomchristie/django-rest-framework/blob/master/rest_framework/utils/encoders.py

With the following modifications:

- Removed QuerySet Serialization and Import
- Removed Promise and Import
- Removed force_text import
- Removed django.utils.six, replaced with six as dependency
- Altered if timezone and timezone.is_aware(obj) check to -> timezone_is_aware(obj)
- Removed rest_framework.compat import total_seconds
"""
from __future__ import unicode_literals

import datetime
import decimal
import json
import uuid
import six


def timezone_is_aware(value):
    """Determine if a given datetime.datetime is aware.

    The concept is defined in Python's docs:
    http://docs.python.org/library/datetime.html#datetime.tzinfo

    Assuming value.tzinfo is either None or a proper datetime.tzinfo,
    value.utcoffset() implements the appropriate logic.

    .. note::

        Daken from Django's source
    """
    return value.utcoffset() is not None


def total_seconds(timedelta):
    """Return total seconds from timedelta

    Python 2.6 compatible
    Taken from https://github.com/tomchristie/django-rest-framework/blob/master/rest_framework/compat.py
    """
    # TimeDelta.total_seconds() is only available in Python 2.7
    if hasattr(timedelta, 'total_seconds'):
        return timedelta.total_seconds()
    else:
        return (timedelta.days * 86400.0) + float(timedelta.seconds) + (timedelta.microseconds / 1000000.0)


class JSONEncoder(json.JSONEncoder):
    """JSONEncoder subclass that knows how to encode date/time/timedelta,
    decimal types, generators and other basic python objects.
    """

    def default(self, obj):
        # For Date Time string spec, see ECMA 262
        # http://ecma-international.org/ecma-262/5.1/#sec-15.9.1.15
        if isinstance(obj, datetime.datetime):
            representation = obj.isoformat()
            if obj.microsecond:
                representation = representation[:23] + representation[26:]
            if representation.endswith('+00:00'):
                representation = representation[:-6] + 'Z'
            return representation
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, datetime.time):
            if timezone_is_aware(obj):
                raise ValueError("JSON can't represent timezone-aware times.")
            representation = obj.isoformat()
            if obj.microsecond:
                representation = representation[:12]
            return representation
        elif isinstance(obj, datetime.timedelta):
            return six.text_type(total_seconds(obj))
        elif isinstance(obj, decimal.Decimal):
            # Serializers will coerce decimals to strings by default.
            return float(obj)
        elif isinstance(obj, uuid.UUID):
            return six.text_type(obj)
        elif hasattr(obj, 'tolist'):
            # Numpy arrays and array scalars.
            return obj.tolist()
        elif hasattr(obj, '__getitem__'):
            try:
                return dict(obj)
            except:
                pass
        elif hasattr(obj, '__iter__'):
            return tuple(item for item in obj)
        return super(JSONEncoder, self).default(obj)
