# -*- coding: utf-8 -*-
import datetime
import warnings
import math

import pandas as pd
from dateutil.relativedelta import relativedelta

from rqdatac.utils import to_datetime, is_panel_removed
from rqdatac.validators import (
    ensure_date_range,
    ensure_date_or_today_int,
    ensure_list_of_string,
    check_items_in_container,
    ensure_order,
    ensure_order_book_id,
    ensure_order_book_ids,
    ensure_dates_base_on_listed_date,
    ensure_string,
    ensure_date_int,
    raise_for_no_panel,
    check_quarter,
    quarter_string_to_date,
)
from rqdatac.services.basic import instruments
from rqdatac.services.calendar import (
    get_trading_dates,
    get_previous_trading_date,
    get_trading_dates_in_type,
    is_trading_date,
)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api, compatible_with_parm, may_trim_bjse
from rqdatac.hk_decorators import support_hk_order_book_id
from rqdatac.rqdatah_helper import rqdatah_serialize, http_conv_list_to_csv


@export_as_api
def current_stock_connect_quota(
        connect=None, fields=None
):
    """
    获取沪深港通每日额度数据

    Parameters
    ----------
    connect : str | list[str], optional
        默认返回全部 connect
        1、输入输入'hk_to_sh'返回沪股通的额度信息。
        2、输入'hk_to_sz'返回深股通的额度信息。
        3、输入'sh_to_hk'返回港股通（上海）的额度信息。
        4、输入'sz_to_hk'返回港股通（深圳）的额度信息
    fields : str | list[str], optional
        默认为所有字段。见下方列表

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - quota_balance : float, 余额
        - quota_balance_ratio : float, 占比
        - buy_turnover : float, 买方金额
        - sell_turnover : float, 卖方金额

    Examples
    --------
    >>> current_stock_connect_quota()
      buy_turnover sell_turnover quota_balance quota_balance_ratio
    datetime connect
    2020-05-26 16:10:00 sh_to_hk 5.463000e+09 3.548000e+09 3.969274e+10 0.945065
    2020-05-26 15:01:00 hk_to_sh 1.115100e+10 1.015700e+10 5.024400e+10 0.960000

    """
    DEFAULT_CONNECT = ["sh_to_hk", "hk_to_sh", "sz_to_hk", "hk_to_sz"]
    if connect is None:
        connect = DEFAULT_CONNECT
    else:
        connect = ensure_list_of_string(connect)
        check_items_in_container(connect, DEFAULT_CONNECT, 'connect')

    DEFAULT_FIELDS = ['buy_turnover', 'sell_turnover', 'quota_balance', 'quota_balance_ratio']
    if fields is None:
        fields = DEFAULT_FIELDS
    else:
        fields = ensure_list_of_string(fields)
        check_items_in_container(fields, DEFAULT_FIELDS, 'fields')

    data = get_client().execute("current_stock_connect_quota", connect=connect)
    res = pd.DataFrame(data)
    if res.empty:
        return None
    res["datetime"] = pd.to_datetime(res["datetime"], format='%Y%m%d%H%M')
    res.set_index(['datetime', 'connect'], inplace=True)
    res = res[fields]
    return res


@export_as_api
def get_stock_connect_quota(connect=None, start_date=None, end_date=None, fields=None):
    """
    获取沪深港通历史每日额度数据

    Parameters
    ----------
    connect : str | list[str], optional
        默认返回全部 connect
        1、输入'hk_to_sh'返回沪股通的额度信息。
        2、输入'hk_to_sz'返回深股通的额度信息。
        3、输入'sh_to_hk'返回港股通（上海）的额度信息。
        4、输入'sz_to_hk'返回港股通（深圳）的额度信息
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        默认为全部历史数据
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        默认为最新日期
    fields : str | list[str], optional
        默认为所有字段。见下方列表

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - quota_balance : float, 余额
        - quota_balance_ratio : float, 占比
        - buy_turnover : float, 买方金额
        - sell_turnover : float, 卖方金额

    """
    DEFAULT_CONNECT = ["sh_to_hk", "hk_to_sh", "sz_to_hk", "hk_to_sz"]
    if connect is None:
        connect = DEFAULT_CONNECT
    else:
        connect = ensure_list_of_string(connect)
        check_items_in_container(connect, DEFAULT_CONNECT, 'connect')

    DEFAULT_FIELDS = ['buy_turnover', 'sell_turnover', 'quota_balance', 'quota_balance_ratio']
    if fields is None:
        fields = DEFAULT_FIELDS
    else:
        fields = ensure_list_of_string(fields)
        check_items_in_container(fields, DEFAULT_FIELDS, 'fields')

    start_date = ensure_date_int(start_date) if start_date else start_date
    end_date = ensure_date_int(end_date) if end_date else end_date

    if start_date and end_date and start_date > end_date:
        raise ValueError("invalid date range: [{!r}, {!r}]".format(start_date, end_date))

    data = get_client().execute(
        "get_stock_connect_quota", connect=connect, start_date=start_date, end_date=end_date, fields=fields
    )
    if not data:
        return None
    res = pd.DataFrame(data)
    res.set_index(['datetime', 'connect'], inplace=True)
    res.sort_index(ascending=True, inplace=True)
    return res


@export_as_api
def is_st_stock(order_book_ids, start_date=None, end_date=None, market="cn"):
    """
    判断一只或多只股票在一段时间（包含起止日期）内是否为 ST 股。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期，默认为股票上市日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，默认为当前日期，如果股票已经退市，则为退市日期
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        查询时间段内是否为 ST 股的查询结果

    Examples
    --------
    >>> is_st_stock("002336.XSHE", "20160411", "20160510")
             002336.XSHE
    2016-04-11 False
    2016-04-12 False
    ...
    2016-05-09 True
    2016-05-10 True

    >>> is_st_stock(["002336.XSHE", "000001.XSHE"], "2016-04-11", "2016-05-10")
       002336.XSHE 000001.XSHE
    2016-04-11 False False
    2016-04-12 False False
    ...
    2016-05-09 True False
    2016-05-10 True False

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, type="CS", market=market)

    if len(order_book_ids) == 1:
        instrument = instruments(order_book_ids[0], market=market)
        start_date, end_date = ensure_dates_base_on_listed_date(instrument, start_date, end_date, market)
        if start_date is None:
            return

    start_date, end_date = ensure_date_range(start_date, end_date)

    trading_dates = pd.to_datetime(get_trading_dates(start_date, end_date, market=market))
    data = get_client().execute(
        "get_st_days", order_book_ids, start_date=start_date, end_date=end_date
    )
    df = pd.DataFrame(data=False, columns=order_book_ids, index=trading_dates)
    df.index.name = "date"
    for idx, dates in data.items():
        for date in dates:
            date = to_datetime(date)
            df.at[date, idx] = True
    return df


@export_as_api
def _is_st_stock(order_book_id, date=None, market="cn"):
    """判断股票在给定日期是否是ST股
    :param order_book_id: 股票id
    :param date:  (Default value = None)
    :param market:  (Default value = "cn")
    :returns: True or False
    """
    order_book_id = ensure_order_book_id(order_book_id, type="CS", market=market)
    date = ensure_date_or_today_int(date)
    df = is_st_stock(order_book_id, start_date=date, end_date=date, market=market)
    if df is None or df.empty:
        return False
    else:
        return df[order_book_id][0]


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def is_suspended(order_book_ids, start_date=None, end_date=None, market="cn"):
    """
    判断某只股票在一段时间（包含起止日期）是否全天停牌。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码。传入单只或多支股票的 order_book_id
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期，默认为股票上市日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，默认为当前日期，如果股票已经退市，则为退市日期
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    pandas.DataFrame
        如果在查询期间内股票尚未上市，或已经退市，则函数返回 None；如果开始日期早于股票上市日期，则以股票上市日期作为开始日期。

    Examples
    --------
    获取武钢股份从 2016 年 6 月 24 日至今（2016 年 8 月 31 日）的停牌情况：

    >>> is_suspended('武钢股份', start_date='20160624')
                   600005.XSHG
    2016-06-24       False
    2016-06-27        True
    2016-06-28        True
    2016-06-29        True
    2016-06-30        True

    >>> is_suspended(['武钢股份','000001.XSHE'], start_date='20160624')
       000001.XSHE 600005.XSHG
    2016-06-24 False False
    2016-06-27 False True
    2016-06-28 False True

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, type="CS", market=market)

    if len(order_book_ids) == 1:
        instrument = instruments(order_book_ids[0], market=market)
        start_date, end_date = ensure_dates_base_on_listed_date(instrument, start_date, end_date, market)
        if start_date is None:
            return
    if end_date is None:
        end_date = datetime.date.today()
    start_date, end_date = ensure_date_range(start_date, end_date)

    trading_dates = pd.to_datetime(get_trading_dates(start_date, end_date, market=market))
    df = pd.DataFrame(data=False, columns=order_book_ids, index=trading_dates)
    df.index.name = 'date'
    data = get_client().execute("get_suspended_days", order_book_ids, start_date, end_date, market=market)
    for idx, dates in data.items():
        for date in dates:
            date = to_datetime(int(date))
            df.at[date, idx] = True
    df.sort_index(inplace=True)
    return df


stock_fields = {"shares_holding": "shares_holding", "holding_ratio": "holding_ratio"}
special_symbols = ["all_connect", "shanghai_connect", "shenzhen_connect"]
symbols_map = {"shanghai_connect": "SH", "shenzhen_connect": "SZ"}


@export_as_api
def get_stock_connect(order_book_ids, start_date=None, end_date=None, fields=None, expect_df=True):
    """
    获取股票或者股票列表在一段时间内的在香港上市交易的持股情况。

    Parameters
    ----------
    order_book_ids : str
        可输入 order_book_id 或 symbol。另，
        1、输入'shanghai_connect'可返回沪股通的全部股票数据。
        2、输入'shenzhen_connect'可返回深股通的全部股票数据。
        3、输入'all_connect'可返回沪股通、深股通的全部股票数据。
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期，默认为'2017-03-17'
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，默认为'2018-03-16'
    fields : str | list[str], optional
        默认为所有字段。见下方列表
    expect_df : bool, optional
        默认返回 pandas dataframe。如果调为 False，则返回原有的数据结构

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - shares_holding : float, 持股量
        - holding_ratio : float, 持股比例
        - adjusted_holding_ratio : float, 调整后持股比例

    Examples
    --------
    获取德赛电池持股概况

    >>> get_stock_connect('000049.XSHE',start_date='2018-05-08',end_date='2018-05-10')
                                shares_holding holding_ratio    adjusted_holding_ratio
    order_book_id   trading_date
    000049.XSHE     2018-05-08 194295.0 0.09             0.0947
                    2018-05-09 144228.0 0.07             0.0703
                    2018-05-10 136628.0 0.06             0.0666

    """
    if expect_df is False:
        warnings.warn(
            "'expect_df=False' is deprecated, and will be removed in future",
            category=DeprecationWarning,
            stacklevel=2
        )
    if order_book_ids not in ("shanghai_connect", "shenzhen_connect", "all_connect"):
        order_book_ids = ensure_order_book_ids(order_book_ids, type="CS")
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields is not None:
        fields = ensure_list_of_string(fields)
        for f in fields:
            if f not in ("shares_holding", "holding_ratio", "adjusted_holding_ratio"):
                raise ValueError("invalid field: {}".format(f))
    else:
        fields = ["shares_holding", "holding_ratio", "adjusted_holding_ratio"]
    data = get_client().execute("get_stock_connect", order_book_ids, start_date, end_date, fields)
    if not data:
        return None
    df = pd.DataFrame(data, columns=["trading_date", "order_book_id"] + fields)

    if not expect_df and not is_panel_removed:
        df = df.set_index(["trading_date", "order_book_id"])
        df = df.to_panel()
        df.major_axis.name = None
        df.minor_axis.name = None
        if len(order_book_ids) == 1:
            df = df.minor_xs(order_book_ids[0])
        if len(fields) == 1:
            df = df[fields[0]]
        if len(order_book_ids) != 1 and len(fields) != 1:
            warnings.warn("Panel is removed after pandas version 0.25.0."
                          " the default value of 'expect_df' will change to True in the future.")
        return df
    else:
        df.sort_values(["order_book_id", "trading_date"], inplace=True)
        df.set_index(["order_book_id", "trading_date"], inplace=True)
        if expect_df:
            return df

        if len(order_book_ids) != 1 and len(fields) != 1:
            raise_for_no_panel()

        if len(order_book_ids) == 1:
            df.reset_index(level=0, drop=True, inplace=True)
            if len(fields) == 1:
                df = df[fields[0]]
            return df
        else:
            df = df.unstack(0)[fields[0]]
            df.index.name = None
            df.columns.name = None
            return df


MARGIN_FIELDS = (
    "margin_balance",
    "buy_on_margin_value",
    "short_sell_quantity",
    "margin_repayment",
    "short_balance_quantity",
    "short_repayment_quantity",
    "short_balance",
    "total_balance",
)

MARGIN_SUMMARY_MAP = {"SH": "XSHG", "XSHG": "XSHG", "SZ": "XSHE", "XSHE": "XSHE", "BJ": "BJSE", "BJSE": "BJSE"}


@export_as_api
def get_securities_margin(
        order_book_ids, start_date=None, end_date=None, fields=None, expect_df=True, market="cn"
):
    """
    获取融资融券信息。包括深证融资融券数据以及上证融资融券数据情况。既包括个股数据，也包括市场整体数据。需要注意，融资融券的开始日期为 2010 年 3 月 31 日;根据交易所的原始数据，上交所个股跟整个市场的输出信息列表不一致，个股没有融券余量金额跟融资融券余额两项, 而深交所个股跟整个市场的输出信息列表一致。

    Parameters
    ----------
    order_book_ids : str | list[str]
        可输入 order_book_id, order_book_id list。另外，输入'XSHG'或'sh'代表整个上证整体情况；'XSHE'或'sz'代表深证整体情况
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期，默认取最近三个月的数据
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，默认为当前有数据的最新日期
    fields : str | list[str], optional
        默认为所有字段。见下方列表
    expect_df : bool, optional
        默认返回 pandas dataframe。如果调为 False，则返回原有的数据结构
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - margin_balance : float, 融资余额
        - buy_on_margin_value : float, 融资买入额
        - margin_repayment : float, 融资偿还额
        - short_balance : float, 融券余额
        - short_balance_quantity : float, 融券余量
        - short_sell_quantity : float, 融券卖出量
        - short_repayment_quantity : float, 融券偿还量
        - total_balance : float, 融资融券余额

    Examples
    --------
    获取沪深两个市场一段时间内的融资余额

    >>> get_securities_margin(['XSHE', 'XSHG'],start_date='20160801', end_date='20160802', fields='margin_balance')
                             margin_balance
    order_book_id date
    XSHE        2016-08-01    383762696120
                 2016-08-02    382892321734
    XSHG        2016-08-01    476355670754
                 2016-08-02    476393053057

    """
    if expect_df is False:
        if market != "cn":
            raise ValueError("'expect_df' can not be False when market is not 'cn'")
        else:
            warnings.warn(
                "'expect_df=False' is deprecated, and will be removed in future",
                category=DeprecationWarning,
                stacklevel=2
            )
    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")
    all_list = []
    for order_book_id in order_book_ids:
        if order_book_id.upper() in MARGIN_SUMMARY_MAP:
            all_list.append(MARGIN_SUMMARY_MAP[order_book_id.upper()])
        else:
            inst = instruments(order_book_id, market)

            if inst is not None and inst.type in ["CS", "ETF", "LOF"]:
                all_list.append(inst.order_book_id)
            else:
                warnings.warn("{} is not stock, ETF, or LOF.".format(order_book_id))
    order_book_ids = all_list
    if not order_book_ids:
        raise ValueError("no valid securities in {}".format(order_book_ids))
    if fields is None:
        fields = list(MARGIN_FIELDS)
    else:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, MARGIN_FIELDS, "fields")
        fields = ensure_order(fields, MARGIN_FIELDS)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if end_date > ensure_date_or_today_int(None):
        end_date = ensure_date_or_today_int(get_previous_trading_date(datetime.date.today()))
    trading_dates = pd.to_datetime(get_trading_dates(start_date, end_date, market=market))

    data = get_client().execute(
        "get_securities_margin", order_book_ids, start_date, end_date, market=market
    )
    if not data:
        return

    if not expect_df and not is_panel_removed:

        pl = pd.Panel(items=fields, major_axis=trading_dates, minor_axis=order_book_ids)
        for r in data:
            for field in fields:
                value = r.get(field)
                pl.at[field, r["date"], r["order_book_id"]] = value

        if len(order_book_ids) == 1:
            pl = pl.minor_xs(order_book_ids[0])
        if len(fields) == 1:
            pl = pl[fields[0]]
        if len(order_book_ids) != 1 and len(fields) != 1:
            warnings.warn("Panel is removed after pandas version 0.25.0."
                          " the default value of 'expect_df' will change to True in the future.")
        return pl
    else:
        df = pd.DataFrame(data)
        df.sort_values(["order_book_id", "date"], inplace=True)
        df.set_index(["order_book_id", "date"], inplace=True)
        df = df.reindex(columns=fields)
        if expect_df:
            return df

        if len(order_book_ids) != 1 and len(fields) != 1:
            raise_for_no_panel()

        if len(order_book_ids) == 1:
            df.reset_index(level=0, drop=True, inplace=True)
            if len(fields) == 1:
                df = df[fields[0]]
            return df
        else:
            df = df.unstack(0)[fields[0]]
            df.index.name = None
            df.columns.name = None
            return df


MARGIN_TYPE = ("stock", "cash")
EXCHANGE_TYPE = {"SZ": "XSHE", "sz": "XSHE", "xshe": "XSHE", "SH": "XSHG", "sh": "XSHG", "xshg": "XSHG", "BJ": "BJSE", "bj": "BJSE", "bjse": "BJSE"}
EXCHANGE_CONTENT = ["XSHE", "XSHG", "BJSE"]


@export_as_api
@may_trim_bjse
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def get_margin_stocks(date=None, exchange=None, margin_type='stock', market="cn"):
    """
    获取某个日期深证、上证融资融券股票列表。

    Parameters
    ----------
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询日期，默认为今天上一交易日
    exchange : str, optional
        交易所，默认为 None，返回所有字段。可选字段包括：'XSHE', 'sz' 代表深交所；'XSHG', 'sh' 代表上交所
    margin_type : str, optional
        'stock' 代表融券卖出，'cash'，代表融资买入，默认为'stock'
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    list
        证券列表 - 如果所查询日期没有融资融券股票列表，则返回空 list

    """
    if date:
        if is_trading_date(date, market):
            date = ensure_date_int(date)
        else:
            date = get_previous_trading_date(date)
            date = date.year * 10000 + date.month * 100 + date.day
    else:
        date = get_previous_trading_date(datetime.date.today())
        date = date.year * 10000 + date.month * 100 + date.day

    if exchange is None:
        exchange = EXCHANGE_CONTENT
    else:
        exchange = ensure_string(exchange, "exchange")
        if exchange in EXCHANGE_TYPE:
            exchange = EXCHANGE_TYPE[exchange]
        check_items_in_container(exchange, EXCHANGE_CONTENT, "exchange")
        exchange = [exchange]

    margin_type = ensure_string(margin_type, "margin_type")
    check_items_in_container(margin_type, MARGIN_TYPE, "margin_type")

    data = get_client().execute(
        "get_margin_stocks", date, exchange, margin_type, market=market
    )

    if not data:
        return []
    else:
        return sorted(data)


@export_as_api
@may_trim_bjse
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def get_eligible_securities_margin(date=None, exchange=None, market="cn"):
    """
    获取某个日期深证、上证融资融券可充抵保证金证券信息。

    Parameters
    ----------
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询日期，默认为今天上一交易日
    exchange : str, optional
        交易所，默认为 None，返回所有字段。可选字段包括：'XSHE', 'sz' 代表深交所；'XSHG', 'sh' 代表上交所
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    list
        证券列表 - 如果所查询日期没有融资融券可充抵保证金证券列表，则返回空 list

    """
    if date:
        if is_trading_date(date, market):
            date = ensure_date_int(date)
        else:
            date = get_previous_trading_date(date)
            date = date.year * 10000 + date.month * 100 + date.day
    else:
        date = get_previous_trading_date(datetime.date.today())
        date = date.year * 10000 + date.month * 100 + date.day

    if exchange:
        exchange = ensure_string(exchange, "exchange")
        if exchange in EXCHANGE_TYPE:
            exchange = EXCHANGE_TYPE[exchange]
        check_items_in_container(exchange, EXCHANGE_CONTENT, "exchange")

    data = get_client().execute(
        "get_eligible_securities_margin", date, exchange, market=market
    )

    if not data:
        return []
    else:
        return data


share_fields = {
    "total": "total_shares",
    "circulation_a": "a_cir_shares",
    "non_circulation_a": "a_non_cir_shares",
    "total_a": "a_total_shares",
    'preferred_shares': 'preferred_shares',
    "free_circulation": "free_circulation"
}
hk_share_fields = ["total_a", "total", "not_hk_shares",
                   "preferred_shares", "authorized_shares", "total_hk", "total_hk1"]

reversed_fields = {v: k for k, v in share_fields.items()}


@export_as_api
@support_hk_order_book_id
@compatible_with_parm(name="country", value="cn", replace="market")
def get_shares(order_book_ids, start_date=None, end_date=None, fields=None, expect_df=True, market="cn"):
    """
    获取股票或者股票列表在一段时间内的股本数据（包含起止日期）。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，不传入 start_date ,end_date 则 默认返回最近三个月的数据
    fields : str | list[str], optional
        默认为所有字段。见下方列表
    expect_df : bool, optional
        默认返回 pandas dataframe,如果调为 False ,则返回原有的数据结构
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - total : float, 总股本
        - circulation_a : float, 流通 A 股
        - management_circulation : float, 已过禁售期的高管持有的股份（已废弃）
        - non_circulation_a : float, 非流通 A 股
        - total_a : float, A 股总股本
        - free_circulation : float, 自由流通股本（提供范围为 2005 年至今）
        - preferred_shares : float, 优先股

    Examples
    --------
    获取平安银行总股本数据

    >>> get_shares('000001.XSHE', start_date='20160801', end_date='20160806', fields='total')
                                        total
    order_book_id     date
    000001.XSHE     2016-08-01     1.717041e+10
                        2016-08-02     1.717041e+10

    """
    if expect_df is False:
        if market != "cn":
            raise ValueError("'expect_df' can not be False when market is not 'cn'")
        else:
            warnings.warn(
                "'expect_df=False' is deprecated, and will be removed in future",
                category=DeprecationWarning,
                stacklevel=2
            )
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields:
        fields = ensure_list_of_string(fields, "fields")
        if 'management_circulation' in fields:
            fields.remove('management_circulation')
            if fields:
                warnings.warn("management_circulation is removed")
            else:
                raise ValueError("management_circulation is removed")
        if market == "cn":
            check_items_in_container(fields, set(share_fields), "fields")
            fields = [share_fields[i] for i in fields]
        else:
            check_items_in_container(fields, set(hk_share_fields), "fields")
    else:
        if market == "cn":
            fields = list(share_fields.values())
        else:
            fields = hk_share_fields

    all_shares = get_client().execute("get_shares_v2", order_book_ids, fields, start_date=start_date, end_date=end_date,
                                      market=market)
    if not all_shares:
        return
    dates = get_trading_dates_in_type(start_date, end_date, expect_type="datetime", market=market)
    df = pd.DataFrame(all_shares)
    unique = set(df.order_book_id)
    missing = [obid for obid in order_book_ids if obid not in unique]
    if missing:
        missing_df = pd.DataFrame({"order_book_id": missing, "date": df.date.iloc[-1]})
        df = pd.concat([df, missing_df])
    if 'preferred_shares' in df.columns:
        df['preferred_shares'] = df['preferred_shares'].fillna(0)
    df.set_index(["date", "order_book_id"], inplace=True)
    df.sort_index(inplace=True)
    df = df.unstack(level=1)
    index = df.index.union(dates)
    df = df.reindex(index)
    df = df.ffill()
    df = df.loc[list(dates)]
    df = df.dropna(how="all")
    df = df[fields]
    if not is_panel_removed and not expect_df:
        pl = df.stack(1).to_panel()
        if market == "cn":
            pl.items = [reversed_fields[i] for i in pl.items]
        else:
            pl.items = [i for i in pl.items if i in hk_share_fields]
        if len(order_book_ids) == 1:
            pl = pl.minor_xs(order_book_ids[0])
        if len(fields) == 1 and market == "cn":
            pl = pl[reversed_fields[fields[0]]]
        if len(order_book_ids) != 1 and len(fields) != 1:
            warnings.warn("Panel is removed after pandas version 0.25.0."
                          " the default value of 'expect_df' will change to True in the future.")
        return pl
    else:
        df = df.stack(1)
        df.index.set_names(["date", "order_book_id"], inplace=True)
        de_listed_map = {i: instruments(i, market=market).de_listed_date for i in order_book_ids}
        max_end_date = df.index.levels[0].max() + pd.Timedelta(days=1)
        de_listed_map = {k: (pd.to_datetime(v) if v != '0000-00-00' else max_end_date) for k, v in
                         de_listed_map.items()}
        i0 = df.index.get_level_values(0)
        i1 = df.index.get_level_values(1).map(de_listed_map)
        mask = i1 > i0
        df = df[mask]
        if df.empty:
            return None
        df = df.reorder_levels(["order_book_id", "date"]).sort_index()
        if market == "cn":
            df.rename(columns=reversed_fields, inplace=True)
        if expect_df:
            return df

        if len(order_book_ids) != 1 and len(fields) != 1:
            raise_for_no_panel()

        if len(order_book_ids) == 1:
            df.reset_index(level=0, drop=True, inplace=True)
            if len(fields) == 1 and market == "cn":
                df = df[reversed_fields[fields[0]]]
            return df
        else:
            if market == "cn":
                df = df.unstack(0)[reversed_fields[fields[0]]]
            else:
                df = df.unstack(0)[fields[0]]
            df.index.name = None
            df.columns.name = None
            return df


allotment_fields = [
    "proportion",
    "allotted_proportion",
    "allotted_shares",
    "allotment_price",
    "book_closure_date",
    "ex_right_date", ]


@export_as_api
@compatible_with_parm(name="country", value="cn", replace="market")
def get_allotment(order_book_ids, start_date=None, end_date=None, fields=None, market="cn"):
    """
    获取股票配股信息

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期, 如'1991-01-01'
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，如'2014-01-04'
    fields : str | list[str], optional
        字段名称，默认返回全部
    market : str, optional
        地区代码，如'cn'

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - order_book_id : str, 股票合约代码
        - declaration_announcement_date : pandas.Timestamp, 首次信息发布日期
        - proportion : float, 配股比例(每一股对应的配股比例)
        - allotted_proportion : float, 实际配股比例(每一股对应的配股比例)
        - allotted_shares : float, 实际配股数量
        - allotment_price : float, 每股配股价格
        - book_closure_date : pandas.Timestamp, 股权登记日
        - ex_right_date : pandas.Timestamp, 除权除息日

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)

    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)

    if fields:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, allotment_fields, "fields")
    else:
        fields = allotment_fields

    all_allotment = get_client().execute("get_allotment", order_book_ids, fields, start_date, end_date, market=market)
    if not all_allotment:
        return
    df = pd.DataFrame(all_allotment)
    df.set_index(["order_book_id", "declaration_announcement_date"], inplace=True)
    df.sort_index(inplace=True)
    df = df[fields]
    return df


@export_as_api
def get_symbol_change_info(order_book_ids, market="cn"):
    """
    获取合约简称变更信息。

    Parameters
    ----------
    order_book_ids : str | list[str]
        给出单个或多个 order_book_id
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - change_date : pandas.Timestamp, 简称变更日期

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    data = get_client().execute("get_symbol_change_info", order_book_ids, market)
    if data:
        df = pd.DataFrame(data)
        df.set_index(['order_book_id', 'change_date'], inplace=True)
        df.sort_index(inplace=True)
        return df


@export_as_api
def get_special_treatment_info(order_book_ids, market="cn"):
    """
    获取合约特殊处理状态信息。

    Parameters
    ----------
    order_book_ids : str | list[str]
        给出单个或多个 order_book_id
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - change_date : pandas.Timestamp, 特别处理(或撤销)实施日期
        - info_date : pandas.Timestamp, 信息发布日期
        - symbol : str, 证券简称
        - type : str, 特别处理(或撤销)类别
        - description : str, 特别处理(或撤销)事项描述

    Examples
    --------
    获取单个合约特殊处理状态数据

    >>> rqdatac.get_special_treatment_info('000020.XSHE')
                               info_date    symbol        type              description
    order_book_id change_date
    000020.XSHE   1999-04-27   1999-04-24   ST华发Ａ       ST
                  2000-03-29   2000-03-28   深华发Ａ       撤销ST
                  2004-04-27   2004-04-26   ST华发Ａ       ST                None

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    data = get_client().execute("get_special_treatment_info", order_book_ids, market)
    if data:
        df = pd.DataFrame(data)
        df.set_index(['order_book_id', 'change_date'], inplace=True)
        df.sort_index(inplace=True)
        return df


@export_as_api
def get_incentive_plan(order_book_ids, start_date=None, end_date=None, market="cn"):
    """
    获取合约股权激励数据

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，给出单个或多个 order_book_id
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期。注：如使用开始日期，则必填结束日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期。注：若使用结束日期，则开始日期必填
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - info_date : pandas.Timestamp, 信息发布日期
        - first_info_date : pandas.Timestamp, 首次信息发布日期
        - effective_date : pandas.Timestamp, 生效日期
        - shares_num : float, 激励股票数量
        - incentive_price : float, 激励股票数量(股)
        - incentive_mode : str, 激励模式
        - info_type : str, 公告类型，草案或者调整
t
    Examples
    --------
    获取单个合约股权激励数据

    >>> rqdatac.get_incentive_plan('002074.XSHE')
                                  first_info_date   effective_date  shares_num    incentive_price incentive_mode info_type
    order_book_id info_date
    002074.XSHE   2021-08-28      2021-08-28        2021-08-28      29980000.0    39.30           股票期权        草案
                  2022-04-29      2022-04-29        2022-04-29      60000000.0    18.77           股票期权        草案

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    end_date = ensure_date_or_today_int(end_date)
    if start_date:
        start_date = ensure_date_int(start_date)
        if start_date > end_date:
            raise ValueError("invalid date range: [{!r}, {!r}]".format(start_date, end_date))
    data = get_client().execute("get_incentive_plan_v2", order_book_ids, start_date, end_date, market)
    if data:
        df = pd.DataFrame(data)
        df["rice_create_tm"] = pd.to_datetime(df["rice_create_tm"] + 3600 * 8, unit="s")
        df.set_index(['order_book_id', 'info_date'], inplace=True)
        df.sort_index(inplace=True)
        return df


@export_as_api
def get_investor_ra(order_book_ids, start_date=None, end_date=None, market='cn'):
    """
    获取合约投资者关系活动数据

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，给出单个或多个 order_book_id
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期。注：如使用开始日期，则必填结束日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期。注：若使用结束日期，则开始日期必填
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - info_date : pandas.Timestamp, 信息发布日期
        - participant : str, 参与人员
        - institution : str, 调研机构
        - detail : str, 与会描述

    Examples
    --------
    获取单个合约投资者关系活动数据

    >>> rqdatac.get_investor_ra('002507.XSHE')
                                  participant  institute   detail
    order_book_id info_date
    002507.XSHE   2012-08-15          唐桦      博时基金     None
                  2012-08-15         张延鹏      朱雀投资    None

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    end_date = ensure_date_or_today_int(end_date)
    if start_date:
        start_date = ensure_date_int(start_date)
        if start_date > end_date:
            raise ValueError("invalid date range: [{!r}, {!r}]".format(start_date, end_date))
    data = get_client().execute("get_investor_ra_v2", order_book_ids, start_date, end_date, market)
    if not data:
        return None
    df = pd.DataFrame(data)
    result_columns = ['participant', 'institute', 'detail']
    if "rice_create_tm" in df.columns:
        df["rice_create_tm"] = pd.to_datetime(df["rice_create_tm"] + 3600 * 8, unit="s")
        result_columns.append("rice_create_tm")
    df.rename(columns={'date': 'info_date'}, inplace=True)
    df.set_index(['order_book_id', 'info_date'], inplace=True)
    df = df.reindex(columns=result_columns).astype(str)
    for col in df.columns:
        df.loc[df[col] == 'nan', col] = None
    return df


ANNOUNCEMENT_FIELDS = ["media", "category", "title",
                       "language", "file_type", "info_type", "announcement_link"]


@export_as_api
def get_announcement(order_book_ids, start_date=None, end_date=None, fields=None, market="cn"):
    """
    获取合约公司公告数据

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，给出单个或多个 order_book_id
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期。注：如使用开始日期，则必填结束日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期。注：若使用结束日期，则开始日期必填
    fields : str | list[str], optional
        可选字段见下方返回，若不指定，则默认获取所有字段
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - order_book_ids : str, 合约代码
        - info_date : pandas.Timestamp, 发布日期
        - meida : str, 媒体出处
        - category : str, 内容类别
        - title : str, 标题
        - language : str, 语言
        - file_type : str, 文件格式
        - info_type : str, 信息类别
        - announcement_link : str, 公告链接
        - create_tm : pandas.Timestamp, 入库时间

    Examples
    --------
    获取一个合约某个时间段内的公司公告数据

    >>> rqdatac.get_announcement('000001.XSHE',20221001,20221010)
                              media  category                           title language file_type info_type                                  announcement_link           create_tm
    order_book_id info_date
    000001.XSHE   2022-10-09  中国货币网        16    平安银行股份有限公司2022年第117期同业存单发行公告     简体中文       PDF     发行上市书  https://www.chinamoney.com.cn/dqs/cm-s-notice-... 2022-10-09 16:43:03
                  2022-10-10  中国货币网        99  平安银行股份有限公司2022年第117期同业存单发行情况公告     简体中文       PDF      临时公告  https://www.chinamoney.com.cn/dqs/cm-s-notice-... 2022-10-10 16:41:28

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields is not None:
        fields = ensure_list_of_string(fields)
        check_items_in_container(fields, ANNOUNCEMENT_FIELDS, "fields")
    else:
        fields = ANNOUNCEMENT_FIELDS
    data = get_client().execute("get_announcement_v2", order_book_ids, start_date, end_date, fields, market=market)
    if not data:
        return None
    df = pd.DataFrame(data)
    df['rice_create_tm'] = pd.to_datetime(df['rice_create_tm'] + 3600 * 8, unit="s")
    df.set_index(["order_book_id", "info_date"], inplace=True)
    df.sort_index(inplace=True)
    return df


@export_as_api
def get_holder_number(order_book_ids, start_date=None, end_date=None, market="cn"):
    """
    获取股东户数数据

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，给出单个或多个 order_book_id
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期，默认为去年当日
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，默认为去年当日
    market : str, optional
        默认是中国市场('cn')，目前仅支持中国市场

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - order_book_ids : str, 合约代码
        - info_date : pandas.Timestamp, 发布日期
        - end_date : pandas.Timestamp, 截止日期
        - share_holders : float, 股东总户数(户)
        - avg_share_holders : float, 户均持股数(股/户)
        - a_share_holders : float, A 股股东户数(户)
        - avg_a_share_holders : float, A 股股东户均持股数(股/户)
        - avg_circulation_share_holders : float, 无限售 A 股股东户均持股数(股/户)

    Examples
    --------
    获取一个合约最近一年的股东户数数据

    >>> rqdatac.get_holder_number('000001.XSHE')
                               end_date  share_holders  a_share_holders  avg_circulation_share_holders  avg_share_holders  avg_a_share_holders
    order_book_id info_date
    000001.XSHE   2023-03-09 2022-12-31       487200.0         487200.0                        39830.0           39831.52             39831.52
                  2023-03-09 2023-02-28       477304.0         477304.0                        40656.0           40657.36             40657.36

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    start_date, end_date = ensure_date_range(start_date, end_date, delta=relativedelta(years=1))
    data = get_client().execute("get_holder_number", order_book_ids, start_date, end_date, market)
    if not data:
        return None
    df = pd.DataFrame.from_records(data, index=["order_book_id", "info_date"])
    df.sort_index(inplace=True)
    return df


_VALID_OPINION_TYPE = {
    'unqualified', 'unqualified_with_explanation', 'qualified', 'disclaimer',
    'adverse', 'unaudited', 'qualified_with_explanation', 'uncertainty_audit',
    'material_uncertainty'
}


@export_as_api
def get_audit_opinion(order_book_ids, start_quarter, end_quarter, date=None, type=None, opinion_types=None,
                      market="cn"):
    """
    获取财务报告审计意见

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码
    start_quarter : str
        财报回溯查询的起始报告期，例如'2015q2'代表 2015 年半年报
    end_quarter : str
        财报回溯查询的截止报告期，例如'2015q4'代表 2015 年年报，该参数必填。
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询日期，默认查询日期为当前最新日期
    type : str, optional
        需要返回的审计报告类型, 可选值包括 financial_statements, internal_control
    opinion_types : str | list[str], optional
        需要返回的审计意见类型, 可选值包括
        "unqualified", "unqualified_with_explanation", "qualified", "disclaimer",
        "adverse", "unaudited", "qualified_with_explanation", "uncertainty_audit",
        "material_uncertainty"
    market : str, optional
        市场，默认'cn'为中国内地市场

    Returns
    -------
    pandas.DataFrame

    """
    if opinion_types is not None:
        opinion_types = ensure_list_of_string(opinion_types, "opinion_type")
        check_items_in_container(opinion_types, _VALID_OPINION_TYPE, "opinion_type")
    if type is not None:
        type = ensure_string(type, "type")
        check_items_in_container(type, {"financial_statements", "internal_control"}, "type")
    date = ensure_date_or_today_int(date)
    order_book_ids = ensure_list_of_string(order_book_ids)

    check_quarter(start_quarter, 'start_quarter')
    start_quarter_int = ensure_date_int(quarter_string_to_date(start_quarter))

    check_quarter(end_quarter, 'end_quarter')
    end_quarter_int = ensure_date_int(quarter_string_to_date(end_quarter))
    result_df = pd.DataFrame(
        get_client().execute("get_audit_opinion", order_book_ids, start_quarter_int, end_quarter_int, date, type,
                             opinion_types, market)
    )
    if result_df.empty:
        return
    result_df.sort_values(['order_book_id', 'end_date', 'info_date'])
    result_df["end_date"] = result_df["end_date"].apply(
        lambda d: "{}q{}".format(d.year, math.ceil(d.month / 3)))
    result_df.rename(columns={"end_date": "quarter"}, inplace=True)
    result_df.set_index(['order_book_id', 'quarter'], inplace=True)
    result_df.sort_index(inplace=True)
    return result_df


@export_as_api
def get_restricted_shares(order_book_ids, start_date=None, end_date=None, market="cn"):
    """
    获取限售解禁明细数据

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，给出单个或多个 order_book_id
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期。注：如使用开始日期，则必填结束日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期。注：若使用结束日期，则开始日期必填
    market : str, optional
        默认是中国内地市场('cn') 。

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - order_book_ids : str, 合约代码
        - info_date : pandas.Timestamp, 发布日期
        - relieve_date : pandas.Timestamp, 解禁日期
        - shareholder_attr : str, 股东属性
        - relieve_shares : float, 解除限售股份数量(股)
        - auctual_relieve_shares : float, 实际上市流通数量(股)(提供范围为 2024-01-01 至今)
        - reason : str, 解禁原因

    Examples
    --------
    获取一个合约某个时间段内的解禁明细数据

    >>> rqdatac.get_restricted_shares('000001.XSHE',20100101,20240101)
                             relieve_date          shareholder_name shareholder_attr  relieve_shares auctual_relieve_shares       reason
    order_book_id info_date
    000001.XSHE   2010-06-25   2010-06-28          中国平安保险（集团）股份有限公司               企业    1.812557e+08                   None     股权分置限售流通
                  2010-09-16   2013-11-12            中国平安人寿保险股份有限公司               企业    6.073280e+08                   None   增发A股法人配售上市

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("get_restricted_shares", order_book_ids, start_date, end_date, market)
    if not data:
        return None
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "info_date"], inplace=True)
    df.sort_index(inplace=True)
    return df


@export_as_api
def st_warning(order_book_ids, start_date=None, end_date=None, fields=None, market="cn"):
    """
    获取st预警信息

    :param order_book_ids: 股票合约id
    :param start_date: 开始日期
    :param end_date: 结束日期
    :param market:str 市场，默认'cn'为中国内地市场
    """
    start_date, end_date = ensure_date_range(start_date, end_date)
    valid_fields = [
        "price_risk_exchange", "price_risk_rq", "mrk_cap_risk_exchange", "mrk_cap_risk_rq",
        "cum_volume_risk_exchange", "cum_volume_risk_rq", "share_holders_risk_exchange",
        "share_holders_risk_rq", "equity_risk_st_star", "profit_revenue_risk_st_star",
        "three_years_profit_risk_st", "dividend_risk_st"
    ]
    if fields is not None:
        fields = ensure_list_of_string(fields)
        check_items_in_container(fields, set(valid_fields), "fields")
    order_book_ids = ensure_order_book_ids(order_book_ids, type="CS", market=market)
    data = get_client().execute("st_warning", order_book_ids, start_date, end_date, fields)
    if not data:
        return None
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "date", "risk_type"], inplace=True)
    df["val"] = 1
    df = df["val"].unstack("risk_type", fill_value=0)
    return df


@export_as_api
def get_staff_count(order_book_ids, start_date=None, end_date=None, market="cn"):
    """
    获取员工数量数据

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期，默认值 None，返回全部数据
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期，默认值 None，返回全部数据
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - order_book_ids : str, 合约代码
        - info_date : pandas.Timestamp, 发布日期
        - end_date : pandas.Timestamp, 截止日期
        - total_staff : int, 职工总数

    Examples
    --------
    获取 000001.XSHE 员工总数

    >>> rqdatac.get_staff_count('000001.XSHE',start_date = 20240101,end_date = 20250801)
                            end_date staff_count
    order_book_id info_date
    000001.XSHE 2024-03-15 2023-12-31 43119
                2024-08-16 2024-06-30 40830

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute(
        "get_staff_count", order_book_ids, start_date, end_date, market=market
    )
    if not data:
        return None
    df = pd.DataFrame(data)
    df['info_date'] = df['info_date'].map(to_datetime)
    df['end_date'] = df['end_date'].map(to_datetime)
    df.set_index(["order_book_id", "info_date"], inplace=True)
    df.sort_index(inplace=True)
    return df
