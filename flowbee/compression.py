from __future__ import absolute_import
import logging
import gzip
import json
import base64
from cStringIO import StringIO


log = logging.getLogger(__name__)


def compress_b64_json(value):
    result = compress_b64(json.dumps(value))
    return result


def compress_b64(value):
    result = base64.b64encode(compress(value))
    return result


def compress(value):
    buffer = StringIO()
    with gzip.GzipFile(fileobj=buffer, mode="w") as f:
        f.write(value)
    return buffer.getvalue()


def decompress_b64_json(value):
    payload = json.loads(decompress_b64(value))
    return payload


def decompress_b64(value):
    bytes = base64.b64decode(value)
    return decompress(bytes)


def decompress(value):
    buffer = StringIO(value)
    gz_file = gzip.GzipFile(fileobj=buffer)
    data = gz_file.read()
    gz_file.close()

    return data
