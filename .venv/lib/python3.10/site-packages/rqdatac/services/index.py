# -*- coding: utf-8 -*-

import bisect
import pandas as pd

from rqdatac.client import get_client
from rqdatac.decorators import export_as_api, compatible_with_parm, may_trim_bjse
from rqdatac.hk_decorators import return_hk_order_book_ids
from rqdatac.validators import (
    ensure_date_int,
    ensure_date_range,
    ensure_list_of_string,
    ensure_order_book_ids,
    ensure_order_book_id,
    check_items_in_container,
)
from rqdatac.utils import int8_to_datetime
from rqdatac.services.calendar import get_trading_dates_in_type
from rqdatac.rqdatah_helper import rqdatah_serialize, http_conv_index_compoents


@export_as_api
@may_trim_bjse
@return_hk_order_book_ids
@compatible_with_parm(name="country", value="cn", replace="market")
@rqdatah_serialize(converter=http_conv_index_compoents, name="order_book_id")
def index_components(
    order_book_id,
    date=None,
    start_date=None,
    end_date=None,
    return_create_tm=False,
    market="cn",
):
    """获取指数成分

    Parameters
    ----------
    order_book_id : str
        指数代码，传入 order_book_id，例如'000001.XSHG'
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        查询日期，默认为当天
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        指定开始日期，不能和 date 同时指定
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        指定结束日期, 需和 start_date 同时指定并且应当不小于开始日期
    return_create_tm : bool
        设置为 True 的时候，传入 date, 返回 tuple, 第一个元素是列表, 第二个元素是入库时间 ;
        传入 start_date, end_date, 返回 dict, 其中 key 是日期, value 是 tuple, 其中第一个元素是列表, 第二个元素是入库时间
    market : str
        默认是中国市场('cn')，目前仅支持中国市场

    Returns
    -------
    list or dict
        构成该指数股票的 order_book_id list

    Examples
    --------
    获取 000001.XSHG 当前最新的成分股列表

    >>> index_components('000001.XSHG')
    ['600000.XSHG',
     '600004.XSHG',
     ...]

    获取 000001.XSHG 当前最新的成分股列表, 返回入库时间字段

    >>> index_components('000001.XSHG', return_create_tm=True)
    (['600000.XSHG',
     '600004.XSHG',
     ...], Timestamp('2025-11-20 07:22:59'))

    获取 000300.XSHG 在 2019-07-01 至 2019-07-06 的成分股列表

    >>> index_components('000300.XSHG',start_date = '20190701',end_date ='20190706')
    {datetime.datetime(2019, 7, 1, 0, 0): ['300433.XSHE',
     '601901.XSHG',
     ...
     '300070.XSHE'],
    datetime.datetime(2019, 7, 5, 0, 0): ['300433.XSHE',
     '601901.XSHG',
     ...
     '300070.XSHE']}

    获取 000300.XSHG 在 2019-07-01 至 2019-07-06 的成分股列表, 返回入库时间字段

    >>> index_components('000300.XSHG',start_date = '20190701',end_date ='20190706', return_create_tm=True)
    {datetime.datetime(2019, 7, 1, 0, 0): (['300433.XSHE',
     '601901.XSHG',
     ...
     '300070.XSHE'], Timestamp('2021-03-26 16:39:34')),
    datetime.datetime(2019, 7, 5, 0, 0): (['300433.XSHE',
     '601901.XSHG',
     ...
     '300070.XSHE'], Timestamp('2021-03-26 16:39:34'))}
    """
    order_book_id = ensure_order_book_id(order_book_id)

    if date and (start_date or end_date):
        raise ValueError("date cannot be input together with start_date or end_date")
    elif (start_date and not end_date) or (end_date and not start_date):
        raise ValueError("start_date and end_date need to be applied together")

    if start_date:
        start_date, end_date = ensure_date_range(start_date, end_date)
        trading_dates = get_trading_dates_in_type(
            start_date, end_date, expect_type="int"
        )
        if not trading_dates:
            return
        data = get_client().execute(
            "index_components_v2",
            order_book_id,
            trading_dates[0],
            trading_dates[-1],
            return_create_tm=return_create_tm,
            market=market,
        )
        if not data:
            return
        if return_create_tm:
            data = {
                d["trade_date"]: (
                    d["component_ids"],
                    pd.to_datetime(d["rice_create_tm"] + 3600 * 8, unit="s"),
                )
                for d in data
            }
        else:
            data = {d["trade_date"]: d["component_ids"] for d in data}
        dates = sorted(data.keys())
        date0 = dates[0]
        res = {}
        for trading_date in trading_dates:
            if trading_date < date0:
                continue
            position = bisect.bisect_right(dates, trading_date) - 1
            res[int8_to_datetime(trading_date)] = data[dates[position]]
        return res

    if date:
        date = ensure_date_int(date)
    result = get_client().execute(
        "index_components",
        order_book_id,
        date,
        return_create_tm=return_create_tm,
        market=market,
    )
    if not result:
        return None
    if return_create_tm:
        create_tm = pd.to_datetime(result.pop("rice_create_tm") + 3600 * 8, unit="s")
        return result["component_ids"], create_tm
    else:
        return result


@export_as_api
@may_trim_bjse
@return_hk_order_book_ids
def index_weights(
    order_book_id, date=None, start_date=None, end_date=None, market="cn"
):
    """获取某一指数的历史构成以及权重。

    注意，该数据为月度更新。

    Parameters
    ----------
    order_book_id : str
        指数代码，可传入 order_book_id，例如'000001.XSHG'或'沪深 300'。目前所支持的指数列表可以参考指数数据表
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        查询日期，默认为当天
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        指定开始日期，不能和 date 同时指定
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        指定结束日期, 需和 start_date 同时指定并且应当不小于开始日期
    market : str
        地区代码, 如 'cn'

    Returns
    -------
    pandas.Series
        每只股票在指数中的构成权重。

    Examples
    --------
    获取上证 50 指数在距离 20160801 最近的一次指数构成结果

    >>> index_weights('000016.XSHG', '20160801')
    Order_book_id
    600000.XSHG    0.03750
    600010.XSHG    0.00761
    600016.XSHG    0.05981
    600028.XSHG    0.01391
    600029.XSHG    0.00822
    600030.XSHG    0.03526
    600036.XSHG    0.04889
    600050.XSHG    0.00998
    600104.XSHG    0.02122
    Name: weight, dtype: float64

    获取上证 50 指数在 20160801 到 20160810 的指数构成结果

    >>> index_weights('000016.XSHG', start_date='20160801', end_date='20160810')
                               weight
    date       order_book_id
    2016-08-01 600000.XSHG    0.03750
               600010.XSHG    0.00761
               600016.XSHG    0.05981
               600028.XSHG    0.01391
               600029.XSHG    0.00822
    ...                           ...
    2016-08-10 601919.XSHG    0.00544
               601985.XSHG    0.00872
               601988.XSHG    0.01944
               601989.XSHG    0.01681
               601998.XSHG    0.00507
    """
    index_name = ensure_order_book_id(order_book_id)
    if date and (start_date or end_date):
        raise ValueError("date cannot be input together with start_date or end_date")
    elif (start_date and not end_date) or (end_date and not start_date):
        raise ValueError("start_date and end_date need to be applied together")

    if start_date:
        start_date, end_date = ensure_date_range(start_date, end_date)
        trading_dates = get_trading_dates_in_type(
            start_date, end_date, expect_type="int"
        )
        if not trading_dates:
            return
        data = get_client().execute(
            "index_weights_v2",
            index_name,
            trading_dates[0],
            trading_dates[-1],
            market=market,
        )
        if not data:
            return

        data = {ensure_date_int(d["date"]): d["data"] for d in data}
        dates = sorted(data.keys())
        for trading_date in trading_dates:
            if trading_date < dates[0]:
                continue
            if trading_date not in data:
                position = bisect.bisect_right(dates, trading_date) - 1
                data[trading_date] = data[dates[position]]

        data = [
            {
                "date": int8_to_datetime(date),
                "order_book_id": c["order_book_id"],
                "weight": c["weight"],
            }
            for date, component_ids in data.items()
            if date in trading_dates
            for c in component_ids
        ]
        return pd.DataFrame(data).set_index(["date", "order_book_id"]).sort_index()

    if date:
        date = ensure_date_int(date)

    data = get_client().execute("index_weights", index_name, date, market=market)
    if not data:
        return
    s = pd.Series({d["order_book_id"]: d["weight"] for d in data})
    s.index.name = "order_book_id"
    s.name = "weight"
    return s


@export_as_api
def index_indicator(
    order_book_ids, start_date=None, end_date=None, fields=None, market="cn"
):
    """获取指数每日估值指标。

    目前仅支持部分市值加权指数的市盈率和市净率，支持的市值指数列表可点
    https://assets.ricequant.com/vendor/rqdata/market-index-list_20230522.xlsx 下载

    Parameters
    ----------
    order_book_ids : str | list[str]
        可输入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        开始日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        结束日期，start_date ,end_date 不传参数时默认返回最近三个月的数据
    fields : str | list[str], optional
        查询字段
        - 'pe_ttm': 市盈率 ttm （对应成分股的市值之和 / 成分股最新一期的滚动归属母公司净利润之和）
        - 'pe_lyr': 市盈率 lyr （对应成分股的市值之和 / 成分股最新年报归属母公司净利润之和）
        - 'pb_ttm': 市净率 ttm （对应成分股的市值之和 / 成分股最新一期的滚动归属母公司权益之和）
        - 'pb_lyr': 市净率 lyr （对应成分股的市值之和 / 成分股最新年报归属母公司权益之和）
        - 'pb_lf':  市净率 lf （对应成分股的市值之和 / 成分股最新一期的归属母公司权益之和）
        - 'total_market_value': 总市值，成分股的总市值之和
        - 'circulation_market_value': 流通市值 成分股流通市值之和
        - 'free_circulation_market_value': 自由流通市值 成分股自由流通市值之和
        - 'dividend_yield_ttm': 股息率 （成分股权重 * 成分股近12个月每股现金分红总额数据 / 成分股总市值 之和）
    market : str
        默认是中国市场('cn')，目前仅支持中国市场

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - 'pe_ttm' : float, 市盈率 ttm （对应成分股的市值之和 / 成分股最新一期的滚动归属母公司净利润之和）
        - 'pe_lyr' : float, 市盈率 lyr （对应成分股的市值之和 / 成分股最新年报归属母公司净利润之和）
        - 'pb_ttm' : float, 市净率 ttm （对应成分股的市值之和 / 成分股最新一期的滚动归属母公司权益之和）
        - 'pb_lyr' : float, 市净率 lyr （对应成分股的市值之和 / 成分股最新年报归属母公司权益之和）
        - 'pb_lf' : float,  市净率 lf （对应成分股的市值之和 / 成分股最新一期的归属母公司权益之和）
        - 'total_market_value': 总市值，成分股的总市值之和
        - 'circulation_market_value': 流通市值 成分股流通市值之和
        - 'free_circulation_market_value': 自由流通市值 成分股自由流通市值之和
        - 'dividend_yield_ttm': 股息率 （成分股权重 * 成分股近12个月每股现金分红总额数据 / 成分股总市值 之和）

    Examples
    --------
    >>> index_indicator(['000016.XSHG','000300.XSHG'],start_date=20170402,end_date=20170408)
                                    pb_lf      pb_lyr     pb_ttm     pe_lyr      pe_ttm
    order_book_id   trade_date
    000016.XSHG     2017-04-05  1.183987   1.196851   1.225406   10.854710   10.778091
                    2017-04-06  1.183772   1.195776   1.225508   10.842157   10.779690
                    2017-04-07  1.185690   1.197714   1.227494   10.859727   10.797159
    000300.XSHG     2017-04-05  1.503460   1.533702   1.563953   13.899681   13.773018
                    2017-04-06  1.505061   1.534583   1.565892   13.904718   13.790629
                    2017-04-07  1.508388   1.537986   1.569359   13.935358   13.820774

    """
    all_fields = ("pe_ttm", "pe_lyr", "pb_ttm", "pb_lyr", "pb_lf", "total_market_value", "circulation_market_value", "free_circulation_market_value", "dividend_yield_ttm")
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)
    if fields is not None:
        fields = ensure_list_of_string(fields)
        for f in fields:
            if f not in all_fields:
                raise ValueError("invalid field: {}".format(f))
    else:
        fields = all_fields

    df = get_client().execute(
        "index_indicator", order_book_ids, start_date, end_date, fields, market=market
    )
    if not df:
        return
    df = pd.DataFrame(df)
    df.set_index(["order_book_id", "trade_date"], inplace=True)
    return df


# 目前支持日度指数的权重，需配合 dps 任务更改
EX_WEIGHTS_INDEXES = [
    "000016.XSHG",
    "000300.XSHG",
    "000905.XSHG",
    "000906.XSHG",
    "000852.XSHG",
    "932000.INDX",
    "000688.XSHG",
    "000922.XSHG",
    "000510.XSHG",
]


@export_as_api
@may_trim_bjse
def index_weights_ex(
    order_book_id, date=None, start_date=None, end_date=None, market="cn"
):
    """获取某一指数的历史构成以及日度权重。

    注意：该数据为基于上月月末成分股自算权重，在指数成分股定期调整实施期间 (为公告日次一交易日起至当月最后一个交易日，以中证系列指数为例，调整于每年 6 月和 12 月第二个星期五后的第一个交易日生效)，结合 ETF 申赎清单进行动态权重调整，确保与指数公司的实际调仓操作保持一致。目前仅支持上证 50、沪深 300、中证 A500、中证 500、中证 800、中证 1000、中证 2000、科创 50、中证红利指数。

    Parameters
    ----------
    order_book_id : str
        指数代码，可传入 order_book_id，目前仅支持上证 50、沪深 300、 中证 500、 中证 800、 中证 1000、中证 2000 指数、科创 50、中证红利。
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        查询日期，默认为当天
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        指定开始日期，不能和 date 同时指定
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        指定结束日期, 需和 start_date 同时指定并且应当不小于开始日期
    market : str
        地区代码, 如 'cn'

    Returns
    -------
    pandas.Series
        每只股票在指数中的构成权重。

    Examples
    --------
    获取上证 50 指数在 20160801 的指数权重

    >>> index_weights_ex('000016.XSHG', '20160801')
    order_book_id
    600000.XSHG    0.03787
    600010.XSHG    0.00761
    600016.XSHG    0.06042
    600028.XSHG    0.01399
    600029.XSHG    0.00799
    600030.XSHG    0.03523
    600036.XSHG    0.04912
    600050.XSHG    0.00985
    600104.XSHG    0.02123
    600109.XSHG    0.00644
    600111.XSHG    0.00795
    600518.XSHG    0.01369
    600519.XSHG    0.04275
    Name: weight, dtype: float64

    """
    check_items_in_container(order_book_id, EX_WEIGHTS_INDEXES, "order_book_id")
    if date and (start_date or end_date):
        raise ValueError("date cannot be input together with start_date or end_date")
    elif (start_date and not end_date) or (end_date and not start_date):
        raise ValueError("start_date and end_date need to be applied together")

    if start_date:
        start_date, end_date = ensure_date_range(start_date, end_date)
        data = get_client().execute(
            "index_weights_ex", order_book_id, start_date, end_date, market=market
        )
        if not data:
            return
        data = [
            {
                "date": d["date"],
                "order_book_id": c["order_book_id"],
                "weight": c["weight"],
            }
            for d in data
            for c in d["data"]
        ]
        return pd.DataFrame(data).set_index(["date", "order_book_id"]).sort_index()

    if date:
        date = ensure_date_int(date)

    data = get_client().execute(
        "index_weights_ex", order_book_id, date, date, market=market
    )
    if not data:
        return
    s = pd.Series({d["order_book_id"]: d["weight"] for d in data[0]["data"]})
    s.index.name = "order_book_id"
    s.name = "weight"
    return s
