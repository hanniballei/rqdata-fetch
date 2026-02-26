# -*- coding: utf-8 -*-
import bisect
import datetime
from rqdatac.client import get_client
from rqdatac.utils import int8_to_date, int_to_datetime
from rqdatac.validators import ensure_date_int
from rqdatac.decorators import export_as_api, ttl_cache, compatible_with_parm
from rqdatac.rqdatah_helper import rqdatah_serialize, http_conv_list_to_csv


@ttl_cache(24 * 3600)
def _get_all_trading_dates(market):
    return get_client().execute("get_all_trading_dates", market=market)


def _map_expect_type(ty, fmt, dates):
    if ty == "int":
        return dates
    if ty == "datetime":
        return [int_to_datetime(dt) for dt in dates]
    if ty == "date":
        return [int8_to_date(dt) for dt in dates]
    if ty == "str":
        return [int_to_datetime(dt).strftime(fmt) for dt in dates]
    raise TypeError(ty)


def _expect_type(ty, fmt, date):
    return _map_expect_type(ty, fmt, [date])[0]


def get_trading_dates_in_type(start_date, end_date, expect_type="datetime", fmt=None, market="cn"):
    """获取两个日期之间的交易日列表

    :param start_date: 开始日期
    :param end_date: 结束日期
    :param expect_type:  (Default value = "datetime")
    :param fmt:  (Default value = None)
    :param market:  (Default value = "cn")

    """
    start_date = ensure_date_int(start_date)
    end_date = ensure_date_int(end_date)
    dates = _get_all_trading_dates(market)
    start_pos = bisect.bisect_left(dates, start_date)
    end_pos = bisect.bisect_right(dates, end_date)
    return _map_expect_type(expect_type, fmt, dates[start_pos:end_pos])


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
@rqdatah_serialize(converter=http_conv_list_to_csv, name='trading_date')
def get_trading_dates(start_date, end_date, market="cn"):
    """
    获取某个国家市场的交易日列表（起止日期加入判断）。

    Parameters
    ----------
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        开始日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        结束日期
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    list[datetime.date]
        交易日期列表

    Examples
    --------
    >>> get_trading_dates(start_date='20160505', end_date='20160505')
    [datetime.date(2016, 5, 5)]

    """
    start_date = ensure_date_int(start_date)
    end_date = ensure_date_int(end_date)
    dates = _get_all_trading_dates(market)
    start_pos = bisect.bisect_left(dates, start_date)
    end_pos = bisect.bisect_right(dates, end_date)
    return [int8_to_date(i) for i in dates[start_pos:end_pos]]


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
@rqdatah_serialize(converter=http_conv_list_to_csv, name='trading_date')
def get_next_trading_date(date, n=1, market="cn"):
    """返回指定日期之后的第 n 个交易日

    默认获取指定日期的国内市场的下一交易日。

    Parameters
    ----------
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        指定日期
    n : int, optional
        n 代表未来第 n 个交易日。默认为 1，即下一个交易日
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    datetime.date
        交易日期

    Examples
    --------
    >>> get_next_trading_date(date='2016-05-01',n=1)
    [datetime.date(2016, 5, 3)]

    """
    if n < 1:
        raise ValueError("n: except a positive value, got {}".format(n))
    date = ensure_date_int(date)
    dates = _get_all_trading_dates(market)
    pos = bisect.bisect_right(dates, date)
    if pos + n - 1 < len(dates):
        return int8_to_date(dates[pos + n - 1])
    return int8_to_date(dates[-1])


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
@rqdatah_serialize(converter=http_conv_list_to_csv, name='trading_date')
def get_previous_trading_date(date, n=1, market="cn"):
    """返回指定日期之前的第 n 个交易日

    默认获取指定日期的上一交易日。

    Parameters
    ----------
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        指定日期
    n : int, optional
        n 代表往前第 n 个交易日。默认为 1，即前一个交易日
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    datetime.date
        交易日期

    Examples
    --------
    >>> get_previous_trading_date('20160502',n=1)
    [datetime.date(2016, 4, 29)]

    """
    if n < 1:
        raise ValueError("n: except a positive value, got {}".format(n))
    date = ensure_date_int(date)
    dates = _get_all_trading_dates(market)
    pos = bisect.bisect_left(dates, date)
    if pos > n:
        return int8_to_date(dates[pos - n])
    return int8_to_date(dates[0])


@export_as_api
def get_latest_trading_date(market="cn"):
    """
    获取最近一个交易日（若当天为交易日，则返回当天；若当天为节假日，则返回上一个交易日）

    Parameters
    ----------
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    datetime.date
        交易日期

    Examples
    --------
    >>> get_latest_trading_date()
    datetime.date(2019, 11, 22)

    """
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    return get_previous_trading_date(tomorrow, market=market)


@export_as_api
def get_future_latest_trading_date(market="cn"):
    """
    获取当天的最近一个期货交易日（从夜盘的集合竞价开始算起，作为新的交易日；若当天为节假日，则返回下一个交易日）

    Parameters
    ----------
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    datetime.date
        交易日期

    Examples
    --------
    >>> get_future_latest_trading_date()
    datetime.date(2023, 6, 21)

    """
    now = datetime.datetime.now()
    latest_trading_date = get_latest_trading_date(market)
    if now < datetime.datetime.combine(latest_trading_date, datetime.time(20, 55)):
        return latest_trading_date
    return get_next_trading_date(latest_trading_date, market=market)


@export_as_api
def trading_date_offset(date, offset, market="cn"):
    """获取当前日期之前（或之后）的一个交易日

    :param date: 日期
    :param offset: 日期间隔
    :param market:  (Default value = "cn")

    """
    date = ensure_date_int(date)
    dates = _get_all_trading_dates(market)
    if offset < 0:
        pos = bisect.bisect_left(dates, date) + offset
    else:
        pos = bisect.bisect_right(dates, date) + offset

    return int8_to_date(dates[pos])


@export_as_api
def is_trading_date(date, market="cn"):
    """判断日期是否为交易日
    :param date: 日期 如20190401
    :param market:  (Default value = "cn")
    :returns: bool
    """
    date = ensure_date_int(date)
    dates = _get_all_trading_dates(market)
    return date in dates


def get_prev_weekday(date):
    """获取上一个工作日，周二到周四返回昨天，周一返回上周五
    :param date : date_int 如 如20190411
    :returns: date_int 如20190413
    """
    date = int8_to_date(date)
    weekday = date.weekday()
    if weekday == 0:
        days = 3
    else:
        days = 1
    return date - datetime.timedelta(days=days)


@export_as_api
def has_night_trading(date, market="cn"):
    """判断交易日是否有夜盘
    :param date: 日期 如20190401
    :param market:  (Default value = "cn")
    :returns: bool
    """
    date = ensure_date_int(date)
    if not is_trading_date(date, market):
        return False
    return is_trading_date(get_prev_weekday(date))


@ttl_cache(3)
def current_trading_date(market='cn'):
    return get_client().execute('current_trading_date', market=market)
