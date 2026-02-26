# -*- coding: utf-8 -*-
import datetime

import pandas as pd
import numpy as np

from rqdatac.services.calendar import get_previous_trading_date
from rqdatac.validators import (
    ensure_string_in,
    ensure_order_book_id,
    ensure_order_book_ids,
    ensure_date_range,
    check_items_in_container,
    ensure_list_of_string
)
from rqdatac.utils import (
    int8_to_datetime_v,
    int14_to_datetime_v,
    int17_to_datetime_v,
    int17_to_datetime,
    today_int,
    date_to_int8,
    convert_bar_to_multi_df,
)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api
from rqdatac.share.errors import MarketNotSupportError, PermissionDenied

DAYBAR_FIELDS = [
    "close", "volume", "total_turnover"
]
TICKBAR_FIELDS = [
    "datetime", "close", "volume", "total_turnover", "bid_vol", "ask_vol"
]


def get_auction_info_daybar(order_book_ids, start_date, end_date, fields, duration=1, market="cn"):
    data = get_client().execute(
        "get_auction_info_daybar", order_book_ids, start_date, end_date, fields, duration, market
    )
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    res = convert_bar_to_multi_df(data, 'date', fields, int8_to_datetime_v)
    return res


def get_today_auction_info_minbar(order_book_ids, date, fields, duration, market="cn"):
    data = get_client().execute("get_today_auction_info_minbar", order_book_ids, date, fields, duration, market)
    return convert_bar_to_multi_df(data, "datetime", fields, int14_to_datetime_v)


def get_auction_info_minbar(order_book_ids, start_date, end_date, fields, duration, market):
    data = get_client().execute(
        "get_auction_info_minbar", order_book_ids, start_date, end_date, fields, duration, market
    )
    if data:
        data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
        df = convert_bar_to_multi_df(data, 'datetime', fields, int14_to_datetime_v)
    else:
        df = None

    today = today_int()
    if df is None:
        history_latest_date = date_to_int8(get_previous_trading_date(today, market=market))
    else:
        history_latest_date = date_to_int8(df.index.get_level_values(1).max())

    if history_latest_date >= end_date or start_date > today or history_latest_date >= today:
        return df
    try:
        live_df = get_today_auction_info_minbar(order_book_ids, today, fields, duration, market)
    except (MarketNotSupportError, PermissionDenied):
        live_df = None
    if live_df is None:
        return df
    if df is None:
        return live_df
    df = pd.concat([df, live_df])
    df.sort_index(inplace=True)
    return df


def get_today_auction_info_tick(order_book_id, date, fields, market="cn"):
    data = get_client().execute("get_today_auction_info_tick", order_book_id, date, market)
    df = pd.DataFrame(data[0])
    if df.empty:
        return None
    df = df[fields]
    df.datetime = df.datetime.apply(int17_to_datetime)
    df.set_index("datetime", inplace=True)
    return df


def get_auction_info_tickbar(order_book_id, start_date, end_date, fields, market):
    order_book_id = ensure_order_book_id(order_book_id)
    start_date, end_date = ensure_date_range(start_date, end_date, datetime.timedelta(days=3))
    data = get_client().execute(
        "get_auction_info_tickbar", order_book_id, start_date, end_date, fields, market
    )
    today = today_int()
    if data:
        data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
        df_list = []
        for obid, d in data:
            df = pd.DataFrame(d)
            df_list.append(df)

        df = pd.concat(df_list)  # type: pd.DataFrame
        df["datetime"] = int17_to_datetime_v(df["datetime"].values)
        history_latest_date = date_to_int8(df.iloc[-1]["datetime"])
        df.set_index("datetime", inplace=True)
    else:
        df = None
        history_latest_date = date_to_int8(get_previous_trading_date(today, market=market))

    if history_latest_date >= end_date or start_date > today or history_latest_date >= today:
        return df
    try:
        live_df = get_today_auction_info_tick(order_book_id, today, fields, market=market)
    except (MarketNotSupportError, PermissionDenied):
        live_df = None
    if live_df is None:
        return df
    if df is None:
        return live_df
    return pd.concat([df, live_df])


@export_as_api
def get_ksh_auction_info(order_book_ids, start_date=None, end_date=None, frequency="1d", market="cn"):
    import warnings

    msg = "'get_ksh_auction_info' is deprecated, and will be removed on 2021-01-29, " \
          "use 'get_auction_info' instead."
    warnings.warn(msg, stacklevel=2)
    return get_auction_info(order_book_ids, start_date, end_date, frequency, market)


@export_as_api
def get_auction_info(order_book_ids, start_date=None, end_date=None, frequency="1d", fields=None, market="cn"):
    """
    获取科创板、创业板等股票合约盘后固定价格交易信息，可获取历史和实时

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可输入 order_book_id, order_book_id list，获取 tick 数据时，只支持单个合约
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期
    frequency : str, optional
        数据的频率。 现在支持日/分钟/tick 级别的数据，默认为'1d'。只支持'1d','1m','tick',不支持'5d'等频率
    fields : str | list[str], optional
        字段名称
    market : str, optional
        默认是中国市场('cn')

    Returns
    -------
    pandas.DataFrame
        包含盘后数据的 DataFrame

    Examples
    --------
    获取合约列表盘后日线数据

    >>> get_auction_info(['688012.XSHG','688011.XSHG'],'20190722','20190722','1d')
                           close volume total_turnover
    order_book_id date
    688012.XSHG 2019-07-22 81.03 112858.0 9144883.74
    688011.XSHG 2019-07-22 70.17 19350.0 1357789.50

    获取单一合约盘后分钟数据

    >>> get_auction_info('688012.XSHG','20190722','20190722','1m')
                       close volume total_turnover
    order_book_id datetime
    688012.XSHG 2019-07-22 15:06:00 81.03 1400.0 113442.00
                2019-07-22 15:07:00 81.03 600.0 48618.00
    ...
                2019-07-22 15:29:00 81.03 3241.0 262618.23
                2019-07-22 15:30:00 81.03 1400.0 113442.00

    """
    ensure_string_in(frequency, ("1d", "1m", "tick"), "frequency")
    if frequency == "tick":
        if fields is not None:
            ensure_list_of_string(fields, "fields")
            check_items_in_container(fields, set(TICKBAR_FIELDS), "fields")
        else:
            fields = TICKBAR_FIELDS
        return get_auction_info_tickbar(order_book_ids, start_date, end_date, fields, market)

    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields is not None:
        ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, set(DAYBAR_FIELDS), "fields")
    else:
        fields = DAYBAR_FIELDS
    if frequency == "1d":
        return get_auction_info_daybar(order_book_ids, start_date, end_date, fields, 1, market)

    return get_auction_info_minbar(order_book_ids, start_date, end_date, fields, 1, market)