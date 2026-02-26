# -*- coding: utf-8 -*-
import warnings

import pandas as pd
import numpy as np
from rqdatac.services.constant import RATETYPE_CN, RATECOMP_CN

from rqdatac.client import get_client
from rqdatac.validators import (
    ensure_int,
    ensure_date_int,
    ensure_order_book_ids,
    ensure_date_range,
    ensure_dates_base_on_listed_date,
    ensure_list_of_string, ensure_date_or_today_int, check_items_in_container)
from rqdatac.utils import to_datetime, int8_to_datetime
from rqdatac.decorators import export_as_api, ttl_cache
from rqdatac.services.calendar import (
    get_trading_dates,
)
from rqdatac.services import shenwan
from rqdatac.rqdatah_helper import rqdatah_serialize, http_conv_instruments, rqdatah_no_index_mark
from rqdatac.services.get_price import get_price

INS_COLUMNS = [
    "order_book_id",
    "symbol",
    "full_name",
    "exchange",
    "bond_type",
    "trade_type",
    "value_date",
    "maturity_date",
    "par_value",
    "coupon_rate",
    "coupon_frequency",
    "coupon_method",
    "compensation_rate",
    "total_issue_size",
    "de_listed_date",
    "stock_code",
    "conversion_start_date",
    "conversion_end_date",
    "redemption_price",
    "issue_price",
    "call_protection",
    "listed_date",
    "early_maturity_date",
    "stop_trading_date",
    "issue_method",
    "list_announcement_date",
    "pref_allocation_registration_date",
    "pref_allocation_payment_end_date",
]


class Instrument:
    def __init__(self, attrs):
        self.__dict__.update(attrs)
        self.__cache = {}

    def __str__(self):
        return "{}(\n{}\n)".format(
            type(self).__name__,
            ",\n".join(["{}={!r}".format(k, v) for k, v in self.items() if not k.startswith("_")]),
        )

    __repr__ = __str__

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, item):
        return self.__dict__[item]

    def get(self, item, default=None):
        return self.__dict__.get(item, default)

    def items(self):
        return self.__dict__.items()

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def __cache_get(self, v):
        return self.__cache.get(v)

    def __cache_set(self, k, v):
        self.__cache[k] = v

    def coupon_rate_table(self):
        """变动利率可转债信息"""
        if "coupon_rate_table" in self.__cache:
            return self.__cache_get("coupon_rate_table")
        info = get_client().execute("convertible.get_coupon_rate_table", self.order_book_id)
        info = pd.DataFrame(info).set_index(['start_date', 'end_date']) if info else None
        self.__cache_set("coupon_rate_table", info)
        return info

    def option(self, option_type=None):
        if option_type is not None:
            option_type = ensure_int(option_type)
            if option_type not in (1, 2, 3, 4, 5, 6, 7):
                raise ValueError("option_type: expect value in (None, 1, 2, 3, 4, 5, 6, 7)")

        data = get_client().execute("convertible.option", self.order_book_id, option_type)
        if not data:
            return

        df = pd.DataFrame(data)
        if 'payment_year' in df.columns:
            sort_fields = ['option_type', 'payment_year']
        else:
            sort_fields = ['option_type']
        df = df.sort_values(sort_fields).reset_index()
        column_order = ['option_type', 'start_date', 'end_date', 'payment_year', 'level', 'window_days',
                        'reach_days', 'frequency', 'price', 'if_include_interest', 'remark']
        column = [i for i in column_order if i in df.columns]
        return df[column]


@ttl_cache(12 * 3600)
def _all_instruments_dict(market="cn"):
    return {
        i['order_book_id']: Instrument(i)
        for i in get_client().execute("convertible.all_instruments", market=market)
    }


@export_as_api(namespace="convertible")
@rqdatah_no_index_mark
def all_instruments(date=None, market="cn"):
    """获取所有可转债基础信息,传入日期可筛选该日上市状态合约列表

    Parameters
    ----------
    date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        指定日期，筛选指定日期可交易的合约
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - order_book_id : str - 可转债合约代码
        - full_name : str - 债券全称
        - symbol : str - 债券简称
        - call_protection : int - 强赎保护期（月计），即此段时间不可强制赎回
        - issue_price : float - 发行价格
        - total_issue_size : float - 发行总规模
        - listed_date : pd.Timestamp - 上市日
        - de_listed_date : datetime - 债券摘牌日
        - stop_trading_date : datetime - 停止交易日
        - value_date : pd.Timestamp - 起息日
        - maturity_date : pd.Timestamp - 到期日(初期公告披露的日期)
        - early_maturity_date : pd.Timestamp - 实际到期日
        - par_value : float - 面值
        - coupon_rate : float - 发行票面利率
        - coupon_frequency : float - 付息频率
        - compensation_rate : float - 到期补偿利率
        - conversion_start_date : pd.Timestamp - 转换期起始日
        - conversion_end_date : pd.Timestamp - 转换期截止日
        - redemption_price : float - 到期赎回价格
        - stock_code : str - 对应股票的 order_book_id
        - exchange : str - 交易所
        - coupon_method : str - 债券计息方式
        - trade_type : str - 交易方式
        - bond_type : str - 债券分类(eb 可交换债券/cb 可转换债券/separately_traded 可分离债)
        - issue_method : str - 发行方式
        - list_announcement_date : pd.Timestamp - 上市公告书发布日
        - pref_allocation_registration_date : pd.Timestamp - 老股东优先配售股权登记日
        - pref_allocation_payment_end_date : pd.Timestamp - 老股东优先配售缴款日

    Examples
    --------
    获取所有可转债基础信息：

    >>> convertible.all_instruments()
      order_book_id  symbol  full_name  exchange  bond_type  trade_type  value_date  maturity_date  par_value  coupon_rate  ...  coupon_method    total_issue_size
    0  100001.XSHG  南化转债  南宁化工股份有限公司可转换公司债券  XSHG  convertible  clean_price  1998-08-03  2003-08-03  100.0  1.00  ...  stepup_rate    1.500000e+08
    1  100009.XSHG  机场转债  上海国际机场股份有限公司可转换公司债券  XSHG  convertible  clean_price  2000-02-25  2005-02-25  100.0  0.80  ...  fixed_rate    1.350000e+09
    2  100016.XSHG  民生转债  中国民生银行股份有限公司可转换公司债券  XSHG  convertible  clean_price  2003-02-27  2008-02-27  100.0  1.50  ...  fixed_rate    4.000000e+09
    ...
    """
    profile = lambda v: (
        v.order_book_id,
        v.symbol,
        v.full_name,
        v.exchange,
        v.bond_type,
        v.trade_type,
        v.value_date,
        v.maturity_date,
        v.par_value,
        v.coupon_rate,
        v.coupon_frequency,
        v.coupon_method,
        v.compensation_rate,
        v.total_issue_size,
        v.de_listed_date,
        v.stock_code,
        v.conversion_start_date,
        v.conversion_end_date,
        v.redemption_price,
        v.issue_price,
        v.call_protection,
        v.listed_date,
        v.early_maturity_date,
        getattr(v, 'stop_trading_date', None),
        v.issue_method,
        v.list_announcement_date,
        v.pref_allocation_registration_date,
        v.pref_allocation_payment_end_date
    )

    def judge(listed_date, de_listed_date):
        if listed_date and de_listed_date:
            return listed_date <= date and de_listed_date > date
        if listed_date:
            return listed_date <= date
        else:
            return False

    if date:
        date = to_datetime(date)
        data = [profile(v) for v in _all_instruments_dict(market).values() if judge(v.listed_date, v.de_listed_date)]
    else:
        data = [profile(v) for v in _all_instruments_dict(market).values()]
    df = pd.DataFrame(
        data,
        columns=INS_COLUMNS,
    )
    df.sort_values('order_book_id', inplace=True)
    return df.reset_index(drop=True)


@export_as_api(namespace="convertible")
@rqdatah_serialize(converter=http_conv_instruments)
def instruments(order_book_ids, market="cn"):
    """获取可转债合约基础信息。

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    Instrument | list[Instrument]

    Examples
    --------
    获取 110074.XSHG 的基础信息：

    >>> convertible.instruments("110074.XSHG")

    Instrument(
    order_book_id='110074.XSHG',
    symbol='精达转债',
    full_name='铜陵精达特种电磁线股份有限公司公开发行可转换公司债券',
    exchange='XSHG',
    bond_type='cb',
    trade_type='dirty_price',
    value_date=datetime.datetime(2020, 8, 19, 0, 0),
    listed_date=datetime.datetime(2020, 9, 21, 0, 0),
    maturity_date=datetime.datetime(2026, 8, 19, 0, 0),
    early_maturity_date=None,
    par_value=100.0,
    coupon_rate=0.004,
    coupon_frequency=1,
    coupon_method='stepup_rate',
    compensation_rate=0.1,
    total_issue_size=787000000.0,
    de_listed_date=datetime.datetime(2026, 8, 19, 0, 0),
    stock_code='600577.XSHG',
    conversion_start_date=datetime.datetime(2021, 2, 25, 0, 0),
    conversion_end_date=datetime.datetime(2026, 8, 18, 0, 0),
    redemption_price=112.0,
    stop_trading_date=None,
    issue_price=100.0,
    issue_method='上网定价,老股东优先配售',
    list_announcement_date=datetime.datetime(2020, 9, 17, 0, 0),
    pref_allocation_registration_date=datetime.datetime(2020, 8, 18, 0, 0),
    pref_allocation_payment_end_date=datetime.datetime(2020, 8, 19, 0, 0),
    call_protection=6.0
    )

    获取 110030.XSHG 格力转债的票息率

    >>> convertible.instruments("110030.XSHG").coupon_rate_table()
    """
    all_dict = _all_instruments_dict(market)
    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")
    if len(order_book_ids) == 1:
        try:
            return all_dict[order_book_ids[0]]
        except KeyError:
            warnings.warn('unknown convertible order_book_id: {}'.format(order_book_ids))
            return
    all_list = (all_dict.get(i) for i in order_book_ids)
    return [i for i in all_list if i]


@export_as_api(namespace="convertible")
def get_cash_flow(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取可转债合约的现金流数据

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        开始日期，默认为初始的兑付日
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        结束日期，默认则返回开始日期后续所有兑付日
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - payment_date : pd.Timestamp - 理论兑付日
        - payment_date_act : pd.Timestamp - 实际兑付日
        - record_date : pd.Timestamp - 债券登记日
        - interest_payment_pretax : float - 每百元面额付息(税前)
        - interest_payment : float - 每百元面额付息
        - principal_payment : float - 每百元面额兑付现金
        - cash_flow_pretax : float - 税前现金流
        - cash_flow : float - 税后现金流

    Examples
    --------
    获取 110032.XSHG 的现金流情况：

    >>> convertible.get_cash_flow('110032.XSHG')
                                record_date cash_flow_pretax principal_payment interest_payment_pretax   payment_date_act cash_flow interest_payment
    order_book_id payment_date
    110032.XSHG     2017-01-04       2017-01-03 0.200             0.0                 0.200                   2017-01-10     0.1600     0.1600
                    2018-01-04       2018-01-03 0.500             0.0                 0.500                   2018-01-10     0.4000     0.4000
                    2019-01-04       2019-01-03 1.000             0.0                 1.000                   2019-01-10     0.8000     0.8000
                    2019-03-20       2019-03-19 100.304             100.0             0.304                   2019-03-26     100.2432 0.2432
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("convertible.get_cash_flow", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "payment_date"], inplace=True)
    return df


@export_as_api(namespace="convertible")
def get_call_info(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取可转债合约一段时期的强制赎回信息

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        开始日期，默认为初始的信息发布日
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        结束日期，默认为当前日期
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - info_date : pd.Timestamp - 信息发布日
        - exercise_price : float - 行权价格
        - interest_included : int - 0 对应不包含，1 对应包含。Null 对应不明确
        - interest_amount : float - 应计利息
        - exercise_date : pd.Timestamp - 行权日
        - call_amount : int - 赎回债券票面金额
        - record_date : pd.Timestamp - 理论登记日，不跳过假日

    Examples
    --------
    获取 110020.XSHG 的强赎情况

    >>> convertible.get_call_info('110020.XSHG')
                          call_amount  exercise_date  exercise_price  interest_amount  interest_included  record_date
    order_book_id  info_date
    110020.XSHG   2015-01-22    8111000.0      2015-03-11     104.0                1.6                     1    2015-03-10
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("convertible.get_call_info", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "info_date"], inplace=True)
    return df


@export_as_api(namespace="convertible")
def get_put_info(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取可转债合约一段时期的持有人回售信息

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        开始日期，默认为初始的信息发布日
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        结束日期，默认为当前日期
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - info_date : pd.Timestamp - 信息发布日
        - exercise_price : float - 行权价格
        - interest_included : int - 0 对应不包含，1 对应包含。Null 对应不明确
        - interest_amount : float - 应计利息
        - enrollment_start_date : pd.Timestamp - 回售登记开始日期
        - enrollment_end_date : pd.Timestamp - 回售登记结束日期
        - payment_date : pd.Timestamp - 资金到账日
        - put_amount : int - 回售债券票面金额
        - put_code : str - 回售代码

    Examples
    --------
    获取 132002.XSHG 的回售情况：

    >>> convertible.get_put_info('132002.XSHG')
                         enrollment_end_date  enrollment_start_date  exercise_price  interest_amount  interest_included  payment_date  put_amount    put_code
    order_book_id  info_date
    132002.XSHG  2018-06-25            2018-07-06    2018-07-02                      107.0  0.08                           1      2018-07-11  1.154681e+09  182187
                  2018-07-16            2018-07-20    2018-07-16                      107.0  0.12                           1      2018-07-25  5.166000e+06  182152
                  2018-07-24            2018-08-03    2018-07-30                      107.0  0.16                           1      2018-08-08  6.792000e+06  182153
    ...
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("convertible.get_put_info", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "info_date"], inplace=True)
    return df


@export_as_api(namespace="convertible")
def get_conversion_price(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取可转债合约一段时期的转股价变动。信息来源为交易所的可转债转股统计公告

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        开始日期，默认为初始的信息发布日
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        结束日期，默认为当前日期
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - info_date : pd.Timestamp - 交易所信息发布日期
        - conversion_price : float - 本次转股价
        - effective_date : pd.Timestamp - 转股价截止日期

    Examples
    --------
    获取 110013.XSHG 的截止某日的转股价变动情况：

    >>> convertible.get_conversion_price('110013.XSHG', end_date=20110704)
                         conversion_price  effective_date
    order_book_id  info_date
    110013.XSHG  2011-01-21  7.29              2011-01-25
                  2011-07-04  7.27              2011-07-04
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("convertible.get_conversion_price", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "info_date"], inplace=True)
    return df


@export_as_api(namespace="convertible")
def get_conversion_info(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取可转债合约一段时期的转股规模变动

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        开始日期，默认为初始的信息发布日
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        结束日期，默认为当前日期
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - info_date : pd.Timestamp - 信息发布日
        - total_amount_converted : int - 累计转债已经转为股票的金额（元），累计每次转股金额
        - total_shares_converted : float - 累计转股数
        - remaining_amount : int - 尚未转股的转债金额（元）
        - amount_converted : int - 本期转债已转为股票的金额（元）, 近似本期转股价与转股数乘积取值
        - shares_converted : int - 本期转股股数
        - end_date : pd.Timestamp - 截止日期
        - conversion_price : float - 本次转股价

    Examples
    --------
    获取 110044.XSHG 的转股规模变动情况：

    >>> convertible.get_conversion_info('110044.XSHG')
                          amount_converted  conversion_price  end_date  remaining_amount  shares_converted  total_amount_converted  total_shares_converted
    order_book_id  info_date
    110044.XSHG  2019-01-04  455562.48                     6.91  2019-01-03  7.995444e+08             65928.0    4.555625e+05           65928.0
                 2019-01-07  683792.87                     6.91  2019-01-04  7.988606e+08             98957.0    1.139355e+06           164885.0
                 2019-01-08  86068043.98                     6.91  2019-01-07  7.127926e+08             12455578.0  8.720740e+07           12620463.0
    ...
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    data = get_client().execute("convertible.get_conversion_info", order_book_ids, start_date, end_date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df.set_index(["order_book_id", "info_date"], inplace=True)
    return df


@export_as_api(namespace="convertible")
def is_suspended(order_book_ids, start_date=None, end_date=None):
    """判断某只可转债或列表在一段时间内是否全天停牌。若在查询期间内转债尚未上市，或已退市，函数则报错提示；若开始日期早于转债上市日期，则以转债上市日期作为开始日期

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        开始日期，默认为转债上市日期
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        结束日期，默认为当前日期，如果转债已经退市，则为退市日期

    Returns
    -------
    pd.DataFrame

    Examples
    --------
    获取国轩转债从 2020 年 5 月 1 日至 2020 年 5 月 30 日的停牌情况：

    >>> convertible.is_suspended('128086.XSHE',20200501,20200530)
             128086.XSHE
    2020-05-06 False
    2020-05-07 False
    2020-05-08 False
    2020-05-11 False
    2020-05-12 False
    2020-05-13 False
    2020-05-14 False
    2020-05-15 False
    2020-05-18 False
    2020-05-19 False
    2020-05-20 True
    2020-05-21 True
    2020-05-22 True
    2020-05-25 True
    2020-05-26 True
    2020-05-27 True
    2020-05-28 True
    2020-05-29 False
    """
    order_book_ids = ensure_order_book_ids(order_book_ids)
    if len(order_book_ids) == 1:
        instrument = instruments(order_book_ids[0], market="cn")
        start_date, end_date = ensure_dates_base_on_listed_date(instrument, start_date, end_date, "cn")
        if start_date is None:
            return
    start_date, end_date = ensure_date_range(start_date, end_date)

    trading_dates = pd.to_datetime(get_trading_dates(start_date, end_date, market="cn"))
    df = pd.DataFrame(data=False, columns=order_book_ids, index=trading_dates)
    df.index.name = "date"
    data = get_client().execute("convertible.is_suspended", order_book_ids, start_date, end_date, market="cn")
    for (order_book_id, date) in data:
        date = to_datetime(date)
        df.at[date, order_book_id] = True
    return df


@export_as_api(namespace="convertible")
def rating(date=None, credit_level=None, institution=None, rating_type=None, target='debt'):
    """
    Get rating information for company or bond
    :param date: str, int, or datatime
        1): 存续债券的判定是date在[value_date,maturity_date]之间
        2): 控制credit_date返回当前最新的日期
    :param credit_level: eg: 'AAA'
    :param institution: rating company name
    :param rating_type: rating type name
    :param target: 'debt' or 'issuer'
    :return:
    """
    check_items_in_container(target, ['debt', 'issuer'], 'target')
    if institution is not None:
        check_items_in_container(institution, RATECOMP_CN, 'institution')
        if institution == "中诚信证券评估有限公司":
            institution = "中诚信证评数据科技有限公司"
    if rating_type is not None:
        check_items_in_container(rating_type, RATETYPE_CN, 'rating_type')
    date = ensure_date_int(date) if date else None

    res = get_client().execute("convertible.rating", date, credit_level, institution, rating_type, target)

    if date and res and target == 'debt':
        ins = instruments(res)
        if not isinstance(ins, list):
            ins = [ins]
        res = [i.order_book_id for i in ins if i.value_date <= int8_to_datetime(date) <= i.maturity_date]
    return res


@export_as_api(namespace="convertible")
def get_latest_rating(order_book_ids, date, institution=None, rating_type=None, target='debt'):
    """
    获取在给定日期之前的最新评级记录.
    返回credit_date和参数date前差距时间最短的一条记录，无需返回所有评级机构最新记录

    :param order_book_ids: str or List[str]债券id列表
    :param date: str or int or datetime.date) 评级日期; 会返回该日期之前的最新评级记录.
    :param institution: str or List[str] or None 评级机构; 若为None, 则返回所有评级机构的最新记录
    :param rating_type: str or None 评级类型; 如果给定的话, 只返回该评级类型下最新的评级信息,
        如果设为None, 则不管评级类型, 直接返回最新评级记录
    :param target: str 评级类型, 可选值为 'debt'(代表债券评级) 或者 'issuer'(代表主体评级);
    :return: a pandas DataFrame with order_book_id as index.
    """
    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")
    check_items_in_container(target, ['debt', 'issuer'], 'target')
    if institution is not None:
        institution = ensure_list_of_string(institution, "institution")
        check_items_in_container(institution, RATECOMP_CN, 'institution')
        if "中诚信证券评估有限公司" in institution:
            institution.append("中诚信证评数据科技有限公司")

    data = get_client().execute(
        "convertible.get_latest_rating",
        order_book_ids,
        ensure_date_int(date),
        institution,
        rating_type,
        target
    )
    if not data:
        return
    df = pd.DataFrame.from_records(data)
    df.sort_values(["order_book_id", "credit_date"], ascending=False, inplace=True)
    df.set_index("order_book_id", inplace=True)
    return df


@export_as_api(namespace="convertible")
def get_instrument_industry(order_book_ids, source='citics', level=1, date=None, market="cn"):
    """获取某个日期转债所属的行业分类，**转债行业分类即为对应正股上市公司行业分类**

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    source : str, optional
        指定行业分标准，默认为 citics；
        `citics`- 中信 2010 行业分类， `citics_2019` - 中信 2019 行业分类, `gildata` - 聚源行业分类
    level : int, optional
        行业分类级别，共三级，默认返回一级分类。参数 0,1,2,3 一一对应，其中 0 返回三级分类完整情况
    date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        行业分类指定查询日期，默认为当前最新，获取转债对应正股指定日期行业分类
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - first_industry_code : int - 一级行业分类代码
        - first_industry_name : str - 一级行业分类名称
        - second_industry_code : int - 二级行业分类代码
        - second_industry_name : str - 二级行业分类名称
        - third_industry_code : int - 三级行业分类代码
        - third_industry_name : str - 三级行业分类名称

    Examples
    --------
    获取当前转债所对应的中信一级行业分类

    >>> convertible.get_instrument_industry('113029.XSHG')
    first_industry_code first_industry_name
    order_book_id
    113029.XSHG 27  电力设备
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, type="Convertible")
    all_dict = _all_instruments_dict(market)
    # array of [(order_book_id, stock_code)]
    mapper = np.array([(i, all_dict[i].stock_code) for i in order_book_ids if i in all_dict])

    # 调用股票行业接口
    res = shenwan.get_instrument_industry(set(mapper[:, 1]), source, level, date, market)
    if res is None:
        return

    # 转换order_book_id为可转债id
    res = res.reindex(mapper[:, 1])
    res.index = pd.Index(mapper[:, 0], name='order_book_id')
    return res


@export_as_api(namespace="convertible")
def get_industry(industry, source='citics', date=None, market="cn"):
    """通过传入行业名称、行业指数代码或者行业代号，拿到指定行业的转债列表

    Parameters
    ----------
    industry : str
        对应行业分类名称
    source : str, optional
        指定行业分标准，默认为 citics；
        `citics`- 中信 2010 行业分类， `citics_2019` - 中信 2019 行业分类, `gildata` - 聚源行业分类
    date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        行业分类指定查询日期。
        默认返回该行业所有转债列表；指定日期返回指定行业分类下该日期仍在上市状态下的转债列表；
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    list
        所属目标行业的可转债order_book_id列表

    Examples
    --------
    获取指定行业分类、日期上市状态可转债 id 列表

    >>> convertible.get_industry(industry='电气设备',source='citics_2019',date='2020-01-26')
    ['113505.XSHG',
     '113546.XSHG',
     '113549.XSHG',
     '123014.XSHE',
     '123030.XSHE',
     '123034.XSHE',
     '128018.XSHE',
     '128042.XSHE',
     '128089.XSHE']
    """
    # 调用股票行业接口
    order_book_ids = shenwan.get_industry(industry, source, date, market)
    if order_book_ids is None:
        return

    order_book_ids = set(order_book_ids)

    all_dict = _all_instruments_dict(market)
    if date:
        oids = []
        date = to_datetime(date)
        for ins in all_dict.values():
            if ins.stock_code in order_book_ids:
                if ins.de_listed_date == "0000-00-00" or ins.de_listed_date is None:
                    ins.de_listed_date = pd.to_datetime("2099-12-31")
                if ins.listed_date is None:
                    ins.listed_date = pd.to_datetime("2099-12-31")
                if ins.listed_date <= date <= ins.de_listed_date:
                    oids.append(ins.order_book_id)
    else:
        oids = [ins.order_book_id for ins in all_dict.values() if ins.stock_code in order_book_ids]
    return sorted(oids)


@export_as_api(namespace="convertible")
def get_indicators(order_book_ids, start_date=None, end_date=None, fields=None):
    """获取可转债衍生指标

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        查询开始日期
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        查询结束日期，start_date ,end_date 不传参数时默认返回最近3个月数据
    fields : str | list[str], optional
        查询字段，默认返回所有字段。可选字段：
        'conversion_coefficient', 'conversion_value', 'conversion_premium',
        'yield_to_maturity', 'yield_to_maturity_pretax', 'yield_to_put', 'yield_to_put_pretax',
        'double_low_factor', 'call_trigger_price', 'put_trigger_price',
        'conversion_price_reset_trigger_price', 'turnover_rate', 'remaining_size',
        'convertible_market_cap_ratio', 'pb_ratio', 'put_qualified_days', 'call_qualified_days',
        'conversion_price_reset_qualified_days', 'put_status', 'call_status',
        'conversion_price_reset_status', 'pure_bond_value_1', 'pure_bond_value_premium_1',
        'iv', 'delta', 'theta', 'gamma', 'vega'

    Returns
    -------
    pd.DataFrame
        Multi-index DataFrame (index: order_book_id, date)，包含以下可选字段：

        - conversion_coefficient : float - 转股系数
        - conversion_value : float - 转股价值
        - conversion_premium : float - 转股溢价率
        - yield_to_maturity : float - 税后到期收益率
        - yield_to_maturity_pretax : float - 税前到期收益率
        - yield_to_put : float - 税后回售收益率
        - yield_to_put_pretax : float - 税前回售收益率
        - double_low_factor : float - 双低指标
        - call_trigger_price : float - 赎回触发价
        - put_trigger_price : float - 回售触发价
        - conversion_price_reset_trigger_price : float - 下修触发价
        - turnover_rate : float - 换手率
        - remaining_size : float - 剩余规模（元）
        - convertible_market_cap_ratio : float - 转债市值占比
        - pb_ratio : float - 市净率
        - put_qualified_days : float - 回售已满足天数
        - call_qualified_days : float - 赎回已满足天数
        - conversion_price_reset_qualified_days : float - 转股价下修已满足天数
        - put_status : float - 回售条款满足状态
        - call_status : float - 强赎条款满足状态
        - conversion_price_reset_status : float - 下修条款满足状态
        - pure_bond_value_1 : float - 纯债价值
        - pure_bond_value_premium_1 : float - 纯债溢价率
        - iv : float - 隐含波动率
        - delta : float - delta
        - theta : float - theta
        - gamma : float - gamma
        - vega : float - vega
        - pure_bond_value_premium : float - deprecate
        - pure_bond_value_premium_pretax : float - deprecate
        - pure_bond_value : float - deprecate
        - pure_bond_value_pretax : float - deprecate

    Examples
    --------
    获取指定日期可转债列表衍生指标数据

    >>> convertible.get_indicators(['110031.XSHG','110033.XSHG'],start_date=20200803, end_date=20200803)
      call_qualified_days call_status call_trigger_price conversion_coefficient conversion_premium ...
    order_book_id date
    110031.XSHG 2020-08-03 0 1 28.028 4.638219 0.323528 ...
    110033.XSHG 2020-08-03 0 1 9.347 13.908206 0.153650 ...
    """
    all_fields = [
        "conversion_coefficient", "conversion_value", "conversion_premium", "pure_bond_value_premium",
        "pure_bond_value_premium_pretax", "yield_to_maturity", "yield_to_maturity_pretax", "yield_to_put",
        "yield_to_put_pretax", "pure_bond_value", "pure_bond_value_pretax", "double_low_factor",
        "call_trigger_price", "put_trigger_price", "conversion_price_reset_trigger_price", "turnover_rate",
        "remaining_size", "convertible_market_cap_ratio", "pb_ratio", "put_qualified_days",
        "call_qualified_days", "conversion_price_reset_qualified_days", "put_status", "call_status",
        "conversion_price_reset_status", "pure_bond_value_1", "pure_bond_value_premium_1",
        "iv", "delta", "theta", "gamma", "vega",
    ]
    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")
    if fields is None:
        fields = all_fields
    else:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, all_fields, 'fields')

    # 默认返回最新3个月的数据
    if start_date is None and end_date is None:
        start_date, end_date = ensure_date_range(start_date, end_date)
    start_date = ensure_date_int(start_date) if start_date is not None else start_date
    end_date = ensure_date_int(end_date) if end_date is not None else end_date

    data = get_client().execute(
        "convertible.get_indicators", order_book_ids, start_date, end_date, fields
    )
    if not data:
        return

    data = pd.DataFrame(data)
    data.set_index(["order_book_id", "date"], inplace=True)
    data.sort_index(inplace=True)
    return data


@export_as_api(namespace="convertible")
def get_coupon_rate_table(order_book_ids):
    """ 变动利率可转债信息

    :param order_book_ids: str or List[str] 合约代码
    :return: DataFrame
        index: ['order_book_id', 'start_date', 'end_date']
        columns: ['coupon_rate']
    """
    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")
    info = get_client().execute("convertible.get_coupon_rate_tables", order_book_ids)
    info = pd.DataFrame(info).set_index(['order_book_id', 'start_date', 'end_date']) if info else None

    return info


@export_as_api(namespace="convertible")
def get_accrued_interest_eod(order_book_ids, start_date=None, end_date=None):
    """获取可转债应计利息数据，应计利息从转债起息日起算

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        查询起始日期
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        查询截止日期，start_date ,end_date 不传参数时默认返回最近三个月的数据

    Returns
    -------
    pd.DataFrame
        索引为日期，列为order_book_ids的DataFrame

    Examples
    --------
    获取指定可转债的应计利息数据

    >>> convertible.get_accrued_interest_eod('110072.XSHG','20200805','20201101')
                110072.XSHG
    date
    2020-08-18 0.000000
    2020-08-19 0.000548
    2020-08-20 0.001096
    2020-08-21 0.001644
    2020-08-22 0.002192
    ...
    2020-10-28 0.038904
    2020-10-29 0.039452
    2020-10-30 0.040000
    2020-10-31 0.040548
    2020-11-01 0.041096
    """
    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")

    if start_date is None or end_date is None:
        start_date, end_date = ensure_date_range(start_date, end_date)
    start_date = to_datetime(start_date)
    end_date = to_datetime(end_date)

    ins = instruments(order_book_ids)
    if ins is None:
        return

    ins = ins if isinstance(ins, list) else [ins]
    # 去掉当天已经完成赎回的转债
    # 有的 id 比如 123095.XSHE 投资人缴款后突然终止上市，然后就退款了, 导致 maturity_date 为 None
    order_book_ids = [c.order_book_id for c in ins
        if c.bond_type != "separately_traded"
        and c.maturity_date is not None
        and c.maturity_date > start_date
    ]
    if not order_book_ids:
        return None

    # 获得强赎信息
    called_info = get_call_info(order_book_ids)

    # 获取coupon_rate_table
    coupon_rate_tables = get_coupon_rate_table(order_book_ids)
    # sort 一下确保后面取到的 end_date 的最后一条记录是最新的.
    coupon_rate_tables.sort_index(inplace=True)
    coupon_rate_tables.reset_index(inplace=True)

    res_list = []
    for oid, sub_df in coupon_rate_tables.groupby("order_book_id"):
        record_date = sub_df.iloc[-1].end_date
        if called_info is not None and oid in called_info.index.get_level_values(0):
            # 获取登记时间
            record_date = called_info.loc[oid].record_date[0]
            sub_df = sub_df[sub_df["start_date"] <= record_date]

        # 生成时间段内对应的coupon_rate
        date_range = pd.date_range(sub_df.iloc[0].start_date, sub_df.iloc[-1].end_date)
        df = pd.DataFrame({"date": date_range})
        df = pd.merge_asof(df, sub_df, left_on="date", right_on="end_date", direction="forward")
        df.dropna(inplace=True)
        df = df[df["date"] <= record_date]

        # 计算利息, 算头不算尾
        df["value"] = df.apply(
            lambda row: (
                ((row['date'] - row['start_date']).days + 1) / 365 * row["coupon_rate"] * 100
                if row["date"] != row["end_date"] else 0
            ),
            axis=1
        )

        # 闰日用前一天的利息补
        # FIXME 闰日之后的所有利息需往前挪一天
        df.loc[(df["date"].dt.month == 2) & (df["date"].dt.day == 29), "value"] = np.nan
        df = df.ffill()

        df = df[(df.date >= start_date) & (df.date <= end_date)]
        df = df.pivot(index="date", columns="order_book_id", values="value")
        df.columns.name = None
        df = df.sort_index()
        res_list.append(df)

    res = pd.concat(res_list, axis=1) if res_list else None
    return res


@export_as_api(namespace="convertible")
def get_call_announcement(order_book_ids, start_date=None, end_date=None, market="cn"):
    """查询可转债赎回提示性公告数据，包含赎回和不赎回的信息

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        查询起始日期
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        查询截止日期，start_date ,end_date 不传参数时默认返回所有数据
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - info_date : pd.Timestamp - 公告日
        - first_info_date : pd.Timestamp - 首次发布赎回公告日
        - if_call : bool - 是否赎回
        - if_issuer call : bool - 是否发行人赎回 (True-发行人赎回，False-到期赎回)
        - call_price : float - 赎回价格(扣税,元/张)
        - call_price_before_tax : float - 赎回价格(含税,元/张)
        - stop_exe_start_date : pd.Timestamp - 触发不行权区间起始日
        - stop_exe_end_date : pd.Timestamp - 触发不行权区间截止日
        - update_time : pd.Timestamp - 数据入库时间

    Examples
    --------
    获取指定可转债 id 赎回提示性公告数据

    >>> convertible.get_call_announcement('113541.XSHG')
                                    update_time   call_price  if_call  first_info_date  stop_exe_start_date call_price_before_tax stop_exe_end_date
    order_book_id   info_date
    113541.XSHG 2021-11-23  2021-11-22 17:09:26         NaN     False            NaT          2021-11-23                   NaN        2022-05-22
                2022-06-14  2022-06-14 00:00:00     100.746 True     2022-05-24                 NaT               100.932               NaT
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    end_date = ensure_date_or_today_int(end_date)
    if start_date:
        start_date = ensure_date_int(start_date)
        if start_date > end_date:
            raise ValueError("invalid date range: [{!r}, {!r}]".format(start_date, end_date))
    data = get_client().execute("convertible.get_call_announcement", order_book_ids, start_date, end_date, market)
    if data:
        df = pd.DataFrame(data)
        df.set_index(['order_book_id', 'info_date'], inplace=True)
        df.sort_index(inplace=True)
        return df


@export_as_api(namespace="convertible")
def get_credit_rating(order_book_ids, start_date=None, end_date=None, institutions=None, market="cn"):
    """查询可转债债项评级数据

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        查询起始日期
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        查询截止日期，start_date ,end_date 不传参数时默认返回所有数据
    institutions : str, optional
        默认返回所有评级机构。可选项见下方说明
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - order_book_ids : str - 可转债合约代码
        - credit_date : pd.Timestamp - 债项评级日期
        - info_date : pd.Timestamp - 公告发布日期
        - info_source : str - 信息来源
        - institution : str - 债项评级机构
        - credit : str - 债项评级
        - rice_create_tm : pd.Timestamp - 米筐入库时间

    Notes
    -----
    institutions 可选项：
    上海新世纪资信评估投资服务有限公司, 上海资信有限公司, 东方金诚国际信用评估有限公司,
    中债资信评估有限责任公司, 中国诚信信用管理股份有限公司, 中证鹏元资信评估股份有限公司,
    中诚信国际信用评级有限责任公司, 中诚信证评数据科技有限公司, 云南省资信评估事务所,
    大公国际资信评估有限公司, 大普信用评级股份有限公司, 安融信用评级有限公司,
    惠誉博华信用评级有限公司, 惠誉国际信用评级有限公司, 标准普尔评级公司,
    标普信用评级(中国)有限公司, 福建省资信评级委员会, 穆迪评级公司,
    联合信用评级有限公司, 联合资信评估股份有限公司, 远东资信评估有限公司

    Examples
    --------
    获取指定可转债 id 债项评级数据

    >>> convertible.get_credit_rating('110031.XSHG')
                            info_date info_source                                             institution          credit    rice_create_tm
    order_book_id credit_date
    110031.XSHG     2014-08-21 2015-06-10 6-10 资信评级机构为本次发行可转换公司债券出具的资信评级报告 联合信用评级有限公司    AAA   2023-12-11 07:00:18
                    2015-07-31 2015-08-04 航天信息2014年可转换公司债券跟踪评级分析报告             联合信用评级有限公司     AAA   2023-12-11 07:00:18
    ...
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)
    if institutions:
        institutions = ensure_list_of_string(institutions)
    data = get_client().execute("convertible.get_credit_rating", order_book_ids, start_date, end_date, institutions)
    if data:
        df = pd.DataFrame.from_records(data, index=["order_book_id", "credit_date"])
        df["rice_create_tm"] = pd.to_datetime(df["rice_create_tm"] + 3600*8, unit="s")
        df.sort_index(inplace=True)
        return df


@export_as_api(namespace="convertible")
def get_std_discount(order_book_ids, start_date=None, end_date=None, market="cn"):
    """查询可转债标准劵折算率

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        开始日期，默认为当前交易日
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        结束日期，默认为当前交易日
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - discount_factor : float - 标准券折算率(每百元面值折算成标准券所乘的系数)

    Examples
    --------
    获取指定可转债 id 的标准劵折算率

    >>> convertible.get_std_discount('110059.XSHG', start_date=20240615, end_date=20240621)
                                discount_factor
    order_book_id     date
    110059.XSHG       2024-06-17     0.73
                      2024-06-18     0.73
                      2024-06-19     0.73
                      2024-06-20     0.73
                      2024-06-21     0.73
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    end_date = ensure_date_or_today_int(end_date)
    if start_date:
        start_date = ensure_date_int(start_date)
        if start_date > end_date:
            raise ValueError("invalid date range: [{!r}, {!r}]".format(start_date, end_date))
    data = get_client().execute("convertible.get_std_discount", order_book_ids, start_date, end_date, market)
    if data:
        df = pd.DataFrame(data)
        df.set_index(['order_book_id', 'date'], inplace=True)
        df.sort_index(inplace=True)
        return df


@export_as_api(namespace="convertible")
def get_close_price(order_book_ids, start_date=None, end_date=None, fields=None, market="cn"):
    """查询可转债当日收盘价的全价和净价数据

    Parameters
    ----------
    order_book_ids : str | list[str]
        可转债合约代码
    start_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        查询起始日期
    end_date : int | str | datetime.date | datetime.datetime | pd.Timestamp, optional
        查询截止日期，start_date ,end_date 不传参数时默认返回最近三个月的数据
    fields : str | list[str], optional
        查询字段，默认返回所有字段。可选字段：'clean_price', 'dirty_price'
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；

    Returns
    -------
    pd.DataFrame
        包含以下字段的DataFrame：

        - datetime : pd.Timestamp - 交易日期
        - clean_price : float - 可转债当日收盘价净价
        - dirty_price : float - 可转债当日收盘价全价

    Examples
    --------
    获取指定可转债 id 赎回提示性公告数据

    >>> convertible.get_close_price(['132020.XSHG','132026.XSHG'],start_date='2024-04-30', end_date='2024-04-30')
                             clean_price  dirty_price
    order_book_id date
    132020.XSHG     2024-04-30   110.673   111.207247
    132026.XSHG     2024-04-30   125.525   125.616507
    """
    all_fields = ["clean_price", "dirty_price"]
    all_instruments_dict = _all_instruments_dict(market=market)
    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")
    order_book_ids = [order_book_id for order_book_id in order_book_ids if order_book_id in all_instruments_dict]
    if not order_book_ids:
        return
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields is None:
        fields = all_fields
    else:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, all_fields, 'fields')
        fields = list(set(fields))

    # 获取证券的历史数据
    close_px = get_price(order_book_ids, start_date, end_date, fields=["close"], adjust_type='none', market=market)
    # 获取可转债应计利息
    accrued_interest = get_accrued_interest_eod(order_book_ids, start_date, end_date)
    if close_px is None or accrued_interest is None:
        return

    accrued_interest = accrued_interest.unstack()
    accrued_interest.index.names = ['order_book_id', 'date']
    accrued_interest.name = 'accrued_interest'

    df = pd.merge(close_px, accrued_interest, on=['order_book_id', 'date'], how='left')
    result = pd.DataFrame({'dirty_price': df['close'], 'clean_price': df['close'] - df['accrued_interest']})

    # 获取 clean_price交易类型对应的 order_book_ids列表
    clean_order_book_ids = [order_book_id for order_book_id in result.index.levels[0] if
                            all_instruments_dict[order_book_id]['trade_type'] != 'dirty_price']

    # 更新 ret_df 中 'clean_price' 交易类型工具的净价和全价
    result.loc[clean_order_book_ids, 'clean_price'] = df.loc[clean_order_book_ids, 'close']
    result.loc[clean_order_book_ids, 'dirty_price'] = df.loc[clean_order_book_ids, 'close'] + df.loc[
        clean_order_book_ids, 'accrued_interest']

    result = result[fields]
    return result
