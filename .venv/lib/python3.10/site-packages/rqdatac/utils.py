# -*- coding: utf-8 -*-
import datetime
import socket

import pandas as pd
import numpy as np

from dateutil.parser import parse as parse_datetime
from dateutil.relativedelta import relativedelta

from six import string_types, integer_types, PY2, binary_type


def iterable(it):
    return hasattr(it, "__next__") or hasattr(it, "__iter__")


pd_version = pd.__version__
if pd_version >= "0.25":
    is_panel_removed = True
else:
    is_panel_removed = False

_str_like = string_types + (bytes,)


if PY2:
    connection_error = socket.error
    timeout_error = socket.timeout
else:
    timeout_error = TimeoutError
    connection_error = ConnectionError


def listify(it):
    if isinstance(it, _str_like):
        return [it]
    elif iterable(it):
        return list(it)
    else:
        return [it]


def to_datetime(dt):
    if isinstance(dt, datetime.datetime):
        return dt
    elif isinstance(dt, datetime.date):
        return datetime.datetime(dt.year, dt.month, dt.day)
    elif isinstance(dt, string_types):
        return parse_datetime(dt, ignoretz=True)
    elif isinstance(dt, integer_types):
        return int_to_datetime(dt)
    elif hasattr(dt, "to_pydatetime"):
        return dt.to_pydatetime()
    elif hasattr(dt, "dtype") and dt.dtype.char == "M":
        return parse_datetime(str(dt))
    raise ValueError("expect a datetime like object, got %r(%r)" % (type(dt), dt))


def to_date(dt):
    if isinstance(dt, datetime.datetime):
        return dt.date()
    elif isinstance(dt, datetime.date):
        return dt
    elif isinstance(dt, string_types):
        return parse_datetime(dt).date()
    elif isinstance(dt, integer_types):
        return int8_to_date(dt)
    elif hasattr(dt, "to_pydatetime"):
        return dt.to_pydatetime()
    elif hasattr(dt, "dtype") and dt.dtype.char == "M":
        return parse_datetime(str(dt)).date()
    raise ValueError("expect a datetime like object, got %r(%r)" % (type(dt), dt))


def to_datetime_str(dt):
    if not isinstance(dt, datetime.datetime):
        dt = to_datetime(dt)
    return "%04d-%02d-%02d %02d:%02d:%02d" % (
        dt.year,
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
    )


def to_date_str(dt):
    if not isinstance(dt, datetime.date):
        dt = to_date(dt)
    return "%04d-%02d-%02d" % (dt.year, dt.month, dt.day)


def delay_today(years=0, months=0, days=0, leapdays=0, weeks=0):
    # type: (int, int, int, int, int) -> datetime.datetime
    return to_datetime(
        datetime.date.today()
        + relativedelta(years=years, months=months, days=days, leapdays=leapdays, weeks=weeks)
    )


def int_to_datetime(dt):
    # type: (int) -> datetime.datetime
    if 9999999 < dt < 99999999:  # 8位日期
        return int8_to_datetime(dt)
    if 9999999999999 < dt < 99999999999999:  # 14位日期时间
        return int14_to_datetime(dt)
    if 9999999999999999 < dt < 99999999999999999:  # 17位日期时间
        return int17_to_datetime(dt)
    raise ValueError("a datetime int should be 8, 14 or 17 length int, now is {}".format(dt))


def int8_to_datetime(dt):
    # type: (int) -> datetime.datetime
    year, dt = dt // 10000, dt % 10000
    month, day = dt // 100, dt % 100
    return datetime.datetime(year, month, day)


_int8_vectorize = np.vectorize(lambda y, m, d: datetime.datetime(y, m, d))


def int8_to_datetime_v(dtarr):
    if not isinstance(dtarr, np.ndarray):
        dtarr = np.array(dtarr)
    years, dt = dtarr // 10000, dtarr % 10000
    months, days = dt // 100, dt % 100
    return _int8_vectorize(years, months, days)


def int9_to_time(tm):
    hour, tm = tm // 10000000, tm % 10000000
    minute, tm = tm // 100000, tm % 100000
    second, ms = tm // 1000, tm % 1000
    return datetime.time(hour, minute, second, ms * 1000)


def int14_to_datetime(dt):
    # type: (int) -> datetime.datetime
    year, dt = dt // 10000000000, dt % 10000000000
    month, dt = dt // 100000000, dt % 100000000
    day, dt = dt // 1000000, dt % 1000000
    hour, dt = dt // 10000, dt % 10000
    minute, second = dt // 100, dt % 100
    return datetime.datetime(year, month, day, hour, minute, second)


_int14_vectorize = np.vectorize(lambda y, m, d, h, mm, s: datetime.datetime(y, m, d, h, mm, s))


def int14_to_datetime_v(dtarr):
    if not isinstance(dtarr, np.ndarray):
        dtarr = np.array(dtarr)
    years, dt = dtarr // 10000000000, dtarr % 10000000000
    months = dt // 100000000
    dt %= 100000000
    days = dt // 1000000
    dt %= 1000000
    hours = dt // 10000
    dt %= 10000
    minutes, seconds = dt // 100, dt % 100
    return _int14_vectorize(years, months, days, hours, minutes, seconds)


def int17_to_datetime(dt):
    # type: (int) -> datetime.datetime
    year, dt = dt // 10000000000000, dt % 10000000000000
    month, dt = dt // 100000000000, dt % 100000000000
    day, dt = dt // 1000000000, dt % 1000000000
    hour, dt = dt // 10000000, dt % 10000000
    minute, dt = dt // 100000, dt % 100000
    second, ms = dt // 1000, dt % 1000
    return datetime.datetime(year, month, day, hour, minute, second, ms * 1000)


_int17_vectorize = np.vectorize(lambda y, m, d, h, mm, s, ms: datetime.datetime(y, m, d, h, mm, s, ms))


def int17_to_datetime_v(dtarr):
    if not isinstance(dtarr, np.ndarray):
        dtarr = np.array(dtarr)
    years, dt = dtarr // 10000000000000, dtarr % 10000000000000
    months = dt // 100000000000
    dt %= 100000000000
    days = dt // 1000000000
    dt %= 1000000000
    hours = dt // 10000000
    dt %= 10000000
    minutes = dt // 100000
    dt %= 100000
    seconds, ms = dt // 1000, dt % 1000
    return _int17_vectorize(years, months, days, hours, minutes, seconds, ms*1000)


def int8_to_date(dt):
    # type: (int) -> datetime.date
    year, dt = dt // 10000, dt % 10000
    month, day = dt // 100, dt % 100
    return datetime.date(year, month, day)


def date_to_int8(dt):
    return dt.year * 10000 + dt.month * 100 + dt.day


def datetime_to_int14(dt):
    return (
        dt.year * 10000000000
        + dt.month * 100000000
        + dt.day * 1000000
        + dt.hour * 10000
        + dt.minute * 100
        + dt.second
    )


def datetime_to_int17(dt):
    return (
        dt.year * 10000000000000
        + dt.month * 100000000000
        + dt.day * 1000000000
        + dt.hour * 10000000
        + dt.minute * 100000
        + dt.second * 1000
        + int(dt.microsecond / 1000)  # ms have six digits
    )


def to_date_int(ds):
    # type: (...) -> int
    if isinstance(ds, int):
        return ds
    elif not isinstance(ds, (datetime.date, datetime.datetime)):
        ds = to_date(ds)
    year, month, day = ds.year, ds.month, ds.day
    return year * 10000 + month * 100 + day


def today_int():
    today = datetime.date.today()
    return today.year * 10000 + today.month * 100 + today.day


def _int_to_time(s):
    if s < 10000:
        return datetime.time(s // 100, s % 100)
    if s < 10000000:
        return datetime.time(s // 10000, (s % 10000) // 100, s % 100)
    return datetime.time(s // 10000000000, (s % 10000000000) // 100000000, (s % 100000000) // 1000000,  s % 1000000)


def to_time(s):
    """convert object to datetime.time something like hh:mm:ss.* or hh:mm:ss or hh:mm"""
    if isinstance(s, datetime.time):
        return s
    if isinstance(s, integer_types):
        return _int_to_time(s)
    if isinstance(s, string_types):
        return parse_datetime(s).time()
    if isinstance(s, datetime.datetime):
        return s.time()
    raise TypeError('unknown type: {}'.format(s))


def safe_string_equal(s1, s2):
    if PY2:
        if isinstance(s1, binary_type):
            s1 = s1.decode("utf8")
        if isinstance(s2, binary_type):
            s2 = s2.decode("utf8")
    return s1 == s2


def pf_fill_nan(pf, order_book_ids):
    pf = pf.transpose(2, 0, 1)
    for order_book_id in order_book_ids:
        if order_book_id not in pf:
            pf[order_book_id] = np.NAN
    return pf.transpose(1, 2, 0)


def get_tick_value(tick, field, default=0):
    key_map = {"a": "ask", "b": "bid"}
    if field.startswith("a") or field.startswith("b"):
        key = key_map[field[0]]
        if field.endswith("v"):
            key += "_vol"
        t = tick.get(key)
        if t:
            return t[int(field[1]) - 1]
        else:
            return default
    else:
        return tick.get(field, default)


def convert_bar_to_multi_df(data, dt_name, fields, convert_dt, default=np.nan, return_slice_map=False):
    line_no = 0
    dt_set = set()
    obid_level = []
    obid_slice_map = {}
    for obid, d in data:
        dts = d[dt_name]
        dts_len = len(dts)
        if dts_len == 0:
            continue
        obid_slice_map[obid] = slice(line_no, line_no + dts_len, None)
        dt_set.update(dts)
        line_no += dts_len

        obid_level.append(obid)

    if line_no == 0:
        return (None, obid_slice_map) if return_slice_map else None

    obid_idx_map = {o: i for i, o in enumerate(obid_level)}
    obid_label = np.empty(line_no, dtype=object)
    dt_label = np.empty(line_no, dtype=object)
    arr = np.full((line_no, len(fields)), default)
    r_map_fields = {f: i for i, f in enumerate(fields)}

    dt_arr_sorted = np.array(sorted(dt_set))
    dt_level = convert_dt(dt_arr_sorted)

    for obid, d in data:
        dts = d[dt_name]
        if len(dts) == 0:
            continue
        slice_ = obid_slice_map[obid]
        for f, value in d.items():
            if f == dt_name:
                dt_label[slice_] = dt_arr_sorted.searchsorted(dts, side='left')
            else:
                arr[slice_, r_map_fields[f]] = value
        obid_label[slice_] = [obid_idx_map[obid]] * len(dts)
    try:
        # func 'is_datetime_with_singletz_array'  is the most time consuming part in multi_index constructing
        # it is useless for our situation. skip it.
        func_is_singletz = getattr(pd._libs.lib, 'is_datetime_with_singletz_array')
        setattr(pd._libs.lib, 'is_datetime_with_singletz_array', lambda *args: True)
    except AttributeError:
        func_is_singletz = None

    multi_idx = pd.MultiIndex([obid_level, dt_level], [obid_label, dt_label],
                              names=('order_book_id', dt_name))

    if func_is_singletz is not None:
        # recovery
        setattr(pd._libs.lib, 'is_datetime_with_singletz_array', func_is_singletz)

    df = pd.DataFrame(data=arr, index=multi_idx, columns=fields)
    if return_slice_map:
        return df, obid_slice_map
    return df
