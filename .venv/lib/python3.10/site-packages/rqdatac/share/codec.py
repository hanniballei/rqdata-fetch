# -*- coding: utf-8 -*-
import struct
import sys
from datetime import datetime, timedelta, date
from decimal import Decimal
from functools import partial
from zlib import compress as zlib_compress, decompress as zlib_decompress

import msgpack

from .protocol import HEADER_FORMAT, COMPRESSION_METHOD, SERIALIZATION_TYPE


def raise_(e):
    raise e


_EPOCH = datetime(1970, 1, 1)
_EPOCH_DATE = _EPOCH.date()


def _object_hook(obj):
    if "__$dt__" in obj:
        return _EPOCH + timedelta(seconds=obj["__$dt__"])
    if "__$dl__" in obj:
        return Decimal(obj["__$dl__"])
    if "__$da__" in obj:
        return _EPOCH_DATE + timedelta(days=obj["__$da__"])
    return obj


def _default(obj):
    if isinstance(obj, datetime):
        return {"__$dt__": int((obj - _EPOCH).total_seconds())}
    if isinstance(obj, date):
        return {"__$da__": (obj - _EPOCH_DATE).days}
    if isinstance(obj, Decimal):
        return {"__$dl__": obj.to_eng_string()}
    return obj


use_bin_type = False if sys.version_info[0] == 2 else True
try:
    msgpack.loads(b'\xa4test', raw=True)
    msgpack_dumps = partial(msgpack.dumps, default=_default, use_bin_type=use_bin_type)
    msgpack_loads = partial(msgpack.loads, object_hook=_object_hook, raw=False,
                            max_str_len=2147483647,  # 2**32-1
                            max_bin_len=2147483647,
                            max_array_len=2147483647,
                            max_map_len=2147483647,
                            max_ext_len=2147483647
                            )
except TypeError:
    msgpack_dumps = partial(msgpack.dumps, default=_default, use_bin_type=use_bin_type)
    msgpack_loads = partial(msgpack.loads, object_hook=_object_hook, encoding="utf8")


try:
    import snappy

    snappy_decompress = snappy.decompress
    snappy_compress = snappy.compress
except (ImportError, AttributeError):
    snappy = None
    snappy_compress = snappy_decompress = lambda *_: raise_(
        RuntimeError(
            "compression not support {}".format(COMPRESSION_METHOD[COMPRESSION_METHOD.SNAPPY])
        )
    )

try:
    import brotli

    brotli_compress = partial(brotli.compress, quality=1)
    brotli_decompress = brotli.decompress
except ImportError:
    brotli = None
    brotli_compress = brotli_decompress = lambda *_: raise_(
        RuntimeError(
            "compression not support {}".format(COMPRESSION_METHOD[COMPRESSION_METHOD.BROTLI])
        )
    )

try:
    import zstandard as zstd
    import threading
    
    class LocalZstd:
        def __init__(self):
            self._local = threading.local()

        def compress(self, data):
            if getattr(self._local, "compressor", None):
                return self._local.compressor.compress(data)

            self._local.compressor = zstd.ZstdCompressor(1)
            return self._local.compressor.compress(data)

        def decompress(self, data):
            if getattr(self._local, "decompressor", None):
                return self._local.decompressor.decompress(data)
            self._local.decompressor = zstd.ZstdDecompressor()
            return self._local.decompressor.decompress(data)

    local_zstd = LocalZstd()
    zstd_compress = local_zstd.compress
    zstd_decompress = local_zstd.decompress
except ImportError:
    zstd = None
    zstd_compress = zstd_decompress = lambda *_: raise_(
        RuntimeError(
            "compression not support {}".format(COMPRESSION_METHOD[COMPRESSION_METHOD.ZSTD])
        )
    )


try:
    import rapidjson as json
except ImportError:
    import json
_json_dump = partial(json.dumps, default=_default)


def json_dumps(*args, **kwargs):
    return json.dumps(*args, default=_default, **kwargs).encode('utf-8')


json_loads = partial(json.loads, object_hook=_object_hook)


SERIALIZER = {
    SERIALIZATION_TYPE.RAW: lambda x: x,
    SERIALIZATION_TYPE.JSON: json_dumps,
    SERIALIZATION_TYPE.MSGPACK: msgpack_dumps,
}

DESERIALIZER = {
    SERIALIZATION_TYPE.RAW: lambda x: x,
    SERIALIZATION_TYPE.JSON: json_loads,
    SERIALIZATION_TYPE.MSGPACK: msgpack_loads,
}

COMPRESSION = {
    COMPRESSION_METHOD.NONE: lambda x: x,
    COMPRESSION_METHOD.SNAPPY: snappy_compress,
    COMPRESSION_METHOD.ZLIB: zlib_compress,
    COMPRESSION_METHOD.BROTLI: brotli_compress,
    COMPRESSION_METHOD.ZSTD: zstd_compress,
}

DECOMPRESSION = {
    COMPRESSION_METHOD.NONE: lambda x: x,
    COMPRESSION_METHOD.SNAPPY: snappy_decompress,
    COMPRESSION_METHOD.ZLIB: zlib_decompress,
    COMPRESSION_METHOD.BROTLI: brotli_decompress,
    COMPRESSION_METHOD.ZSTD: zstd_decompress,
}


_header_struct = struct.Struct(HEADER_FORMAT)
pack_header = _header_struct.pack
unpack_header = _header_struct.unpack


def pack_one(data, mt, st, cm, force_compress=False):
    # type: (...,int, int, int) -> bytes
    data = SERIALIZER[st](data)
    if force_compress or len(data) > 1024:
        data = COMPRESSION[cm](data)
    else:
        cm = COMPRESSION_METHOD.NONE
    return pack_header(mt, st, cm, len(data)) + data


def unpack_one(data, st, cm):
    return DESERIALIZER[st](DECOMPRESSION[cm](data))
