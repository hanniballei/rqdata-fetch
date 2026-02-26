# -*- coding: utf-8 -*-
import warnings
import pandas as pd

from rqdatac.validators import (
    ensure_date_or_today_int,
    ensure_date_int,
    ensure_list_of_string,
    ensure_order_book_ids
)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api, may_trim_bjse
from rqdatac.rqdatah_helper import rqdatah_serialize, http_conv_list_to_csv


@export_as_api
@rqdatah_serialize(converter=http_conv_list_to_csv, name='concept')
def concept_list(date=None, market="cn"):
    """获取所有股票概念.

    :param date: 可指定日期，默认按当前日期返回.
    :param market: 地区代码, 如 'cn' (Default value = "cn")
    :returns: 符合指定日期内出现过的所有概念列表

    """
    msg = "'concept_list' is deprecated, please use 'get_concept_list' instead"
    warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
    date = ensure_date_or_today_int(date)
    return get_client().execute("concept_list", date, market=market)


@export_as_api
@may_trim_bjse
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def concept(*concepts, **kwargs):
    """获取对应某个概念的股票列表。

    可指定日期，默认按当前日期返回。目前支持的概念列表可以查询以下网址:
    https://www.ricequant.com/api/research/chn#concept-API-industry

    :param concepts: 概念字符串,如 '民营医院'
    :param date: 可指定日期，默认按当前日期返回.
    :param market: 地区代码, 如 'cn'
    :returns: 符合对应概念的股票列表

    """
    msg = "'concept' is deprecated, please use 'get_concept' instead"
    warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
    date = kwargs.pop("date", None)
    market = kwargs.pop("market", "cn")
    date = ensure_date_or_today_int(date)
    if kwargs:
        raise ValueError('unknown kwargs: {}'.format(kwargs))
    return get_client().execute("concept", concepts, date, market=market)


@export_as_api
@rqdatah_serialize(converter=http_conv_list_to_csv, name='concept')
def concept_names(order_book_id, date=None, expect_type="str", market="cn"):
    """获取证券所属的概念列表。

    :param order_book_id: 证券ID
    :param date: 可指定日期，默认按当前日期返回。
    :param expect_type: 期望返回结果类型，可选址为："str"：返回字符串，"list"：返回列表，默认为str。
    :param market: 地区代码, 如 "cn" (Default value = "cn")
    :returns: 概念列表

    """

    date = ensure_date_or_today_int(date)
    data = get_client().execute("concept_names", order_book_id, date, market=market)
    if expect_type == "str":
        return data
    elif expect_type == "list":
        return data.split("|")
    raise ValueError("expect_type should be str like 'str' or 'list'")


@export_as_api
def get_concept_list(start_date=None, end_date=None, market="cn"):
    """
    获得股票概念列表。

    Parameters
    ----------
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询 概念纳入日期 开始时间，不传入默认返回所有时段数据
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询 概念纳入日期 结束时间，不传入默认返回所有时段数据
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.Series

    Examples
    --------
    >>> get_concept_list(start_date='2019-01-01', end_date='2020-01-01')
    date
    2019-01-17    360私有化
    2019-01-17      油气改革
    2019-01-17    浦东国资改革
    2019-01-17     海南自贸区
    2019-01-17      海绵城市
                   ...
    2019-07-15     ETC概念
    2019-07-17     光刻机概念
    2019-08-12       维生素
    2019-12-12      胎压监测
    2019-12-20      无线耳机

    """
    if start_date is not None:
        start_date = ensure_date_int(start_date)
    if end_date is not None:
        end_date = ensure_date_int(end_date)
    result = get_client().execute("get_concept_list", start_date, end_date, market=market)
    if not result:
        return None
    df = pd.DataFrame.from_records(result, index="date")
    df.sort_index(inplace=True)
    return df["concept"]


@export_as_api
@may_trim_bjse
def get_concept(concepts, start_date=None, end_date=None, market="cn"):
    """
    获取所选概念对应股票列表

    Parameters
    ----------
    concepts : str or multiple str
        概念名称。可以从概念列表中选择一个或多个概念填写
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询 股票纳入概念日期 开始时间，不传入默认返回所有时段数据
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询 股票纳入概念日期 结束时间，不传入默认返回所有时段数据
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - order_book_id : str, 合约代码
        - inclusion_date : pandas.Timestamp, 股票纳入概念日期

    Examples
    --------
    >>> get_concept(['ETC概念','维生素'],start_date='2019-01-01', end_date='2021-01-01')
     order_book_id inclusion_date
    concept
    ETC概念 000938.XSHE 2019-07-15
    ETC概念 002104.XSHE 2019-10-16
    ETC概念 002161.XSHE 2019-08-08
    维生素 000597.XSHE 2019-08-12
    维生素 000952.XSHE 2019-08-12
    维生素 002001.XSHE 2019-08-12

    """
    if start_date is not None:
        start_date = ensure_date_int(start_date)
    if end_date is not None:
        end_date = ensure_date_int(end_date)
    concepts = ensure_list_of_string(concepts, "concepts")
    result = get_client().execute("get_concept", concepts, start_date, end_date)
    if not result:
        return None
    return pd.DataFrame.from_records(result, index="concept")


@export_as_api
def get_stock_concept(order_book_ids, market="cn"):
    """
    获取单支或多支股票所对应的所有概念标签

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可输入 order_book_id, order_book_id list
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - order_book_id : str, index, 合约代码
        - concept : str, index, 所输入合约代码对应的概念
        - inclusion_date : pandas.Timestamp, column, 股票纳入概念日期

    Examples
    --------
    >>> get_stock_concept (['000002.XSHE','000504.XSHE'])
                          concept
    order_book_id inclusion_date
    000002.XSHE     2019-01-17 房地产
                    2019-01-17 深港通
                    2019-01-17 租售同权
                    2019-01-17 广东自贸区
                    2019-01-22 举牌概念
                    2019-03-04 粤港澳大湾区

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, type="CS", market=market)
    result = get_client().execute("get_stock_concept", order_book_ids, market)
    if not result:
        return None
    result = pd.DataFrame.from_records(result, index=["order_book_id", "inclusion_date"])
    result.sort_index(inplace=True)
    return result
