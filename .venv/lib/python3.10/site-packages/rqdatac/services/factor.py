# -*- coding: utf-8 -*-
import datetime
import warnings
from collections import OrderedDict

import pandas as pd
import numpy as np

from rqdatac.hk_decorators import support_hk_order_book_id
from rqdatac.services.calendar import (
    get_previous_trading_date,
    get_next_trading_date,
    get_trading_dates,
)
from rqdatac.services.basic import instruments
from rqdatac.utils import int14_to_datetime
from rqdatac.validators import (
    ensure_list_of_string,
    ensure_date_int,
    ensure_date_range,
    ensure_string,
    ensure_string_in,
    check_items_in_container,
    ensure_order_book_ids,
)
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api
from rqdatac.rqdatah_helper import rqdatah_serialize, http_conv_list_to_csv


VALID_FACTOR_TYPES = [
    'income_statement',
    'balance_sheet',
    'cash_flow_statement',
    'eod_indicator',
    'operational_indicator',
    'cash_flow_indicator',
    'financial_indicator',
    'growth_indicator',
    'alpha101',
    'moving_average_indicator',
    'obos_indicator',
    'energy_indicator',
    'other',
]

MIN_FACTOR_DAYS = {
    'cn': 20000104,
    'hk': 20000103,
}


@export_as_api
@rqdatah_serialize(converter=http_conv_list_to_csv, name='factor')
def get_all_factor_names(type=None, market="cn"):
    """
    获取因子字段列表

    默认返回全部因子，可选择返回不同类型的所有因子字段名称列表。

    Parameters
    ----------
    type : str, optional
        默认返回所有因子
        'income_statement'：利润表
        'balance_sheet'：资产负债表
        'cash_flow_statement'：现金流量表
        'eod_indicator'：估值有关指标
        'operational_indicator'：经营衍生指标表
        'cash_flow_indicator'：现金流衍生指标
        'financial_indicator'：财务衍生指标
        'growth_indicator'：增长衍生指标
        'alpha101'：alpha101 因子
        'moving_average_indicator'：均线类指标
        'obos_indicator'：超买超卖指标
        'energy_indicator'：能量指标
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    list

    Examples
    --------
    获取超买超卖指标 因子

    >>> rqdatac.get_all_factor_names(type='obos_indicator')
    ['OBOS', 'KDJ_K', 'KDJ_D', 'KDJ_J', 'RSI6', 'WR', 'LWR1', 'BIAS5', 'BIAS36', 'ACCER', 'CYF', 'SWL', 'ADTM', 'TR', 'DKX', 'TAPI', 'OSC', 'CCI', 'ROC', 'MFI', 'MTM', 'MARSI6', 'SKD_K', 'UDL', 'DI1', 'RSI10', 'LWR2', 'BIAS10', 'BIAS612', 'SWS', 'MAADTM', 'ATR', 'MADKX', 'MATAPI', 'MAMTM', 'MARSI10', 'SKD_D', 'MAUDL', 'DI2', 'BIAS20', 'MABIAS', 'ADX', 'ADXR']

    """
    if type is not None and (not isinstance(type, str) or type not in VALID_FACTOR_TYPES):
        raise ValueError('invalid type: {}'.format(type))
    return get_client().execute("get_all_factor_names", type=type, market=market)


@export_as_api
@support_hk_order_book_id
def get_factor(order_book_ids, factor, start_date=None, end_date=None, universe=None, expect_df=True, market='cn', **kwargs):
    """
    获取因子值

    默认返回指定因子上一个交易日的值.

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list
    factor : str | list[str]
        因子名称，可查询 get_all_factor_names() 得到所有有效因子字段
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期。注：如使用开始日期，则必填结束日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期。注：若使用结束日期，则开始日期必填
    universe : str, optional
        指定因子计算时的股票域，米筐所有公共因子均在全市场范围计算，此参数保留为 None 即可

        .. deprecated:: 2
    expect_df : bool, optional
        默认返回 pandas dataframe。如果调为 False，则返回 原有的数据结构
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    pandas.DataFrame

    Examples
    --------
    获取财务指标因子数据

    >>> get_factor(['000001.XSHE','000002.XSHE'],'debt_to_equity_ratio',start_date='20180102',end_date='20180103')
                                      debt_to_equity_ratio
    order_book_id      date
    000002.XSHE        2018-01-02     7.3097
                       2018-01-03     7.3097
    000001.XSHE        2018-01-02     13.3848
                       2018-01-03     13.3848

    获取技术指标因子数据

    >>> get_factor(['000001.XSHE','600000.XSHG'],['MACD_DIFF','OBOS','AR'],'20200401','20200402')
                                   MACD_DIFF	OBOS	AR
    order_book_id	date
    600000.XSHG	2020-04-01	-0.252972	2.0	86.486486
                2020-04-02	-0.237592	4.0	79.701493
    000001.XSHE	2020-04-01	-0.564380	2.0	110.490694
                2020-04-02	-0.530793	4.0	105.396290

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
    check_items_in_container(market, ('cn', 'hk'), 'market')
    order_book_ids = ensure_order_book_ids(order_book_ids, type="CS", market=market)
    order_book_ids = list(set(order_book_ids))

    factor = ensure_list_of_string(factor)
    factor = list(OrderedDict.fromkeys(factor))

    if start_date and end_date:
        start_date, end_date = ensure_date_range(start_date, end_date, datetime.timedelta(days=15))
        min_day = MIN_FACTOR_DAYS[market]
        if start_date < min_day:
            warnings.warn("start_date is earlier than {}, adjusted to {}".format(min_day, min_day))
            start_date = min_day
    elif start_date:
        raise ValueError("Expect end_date")
    elif end_date:
        raise ValueError("Expect start_date")
    else:
        date = kwargs.pop("date", None)
        date = ensure_date_int(date or get_previous_trading_date(datetime.date.today(), market=market))
        start_date = end_date = date

    if kwargs:
        raise ValueError('unknown kwargs: {}'.format(kwargs))

    data = get_client().execute(
        "get_factor_from_store", order_book_ids, factor, start_date, end_date, universe=universe, market=market
    )

    if not data:
        return

    factor_value_length = len(data[0][2])
    if factor_value_length == 0:
        return

    dates = pd.to_datetime(get_trading_dates(start_date, end_date, market=market))
    days = len(dates)
    if days > factor_value_length:
        _get_factor_warning_msg(dates[factor_value_length], dates[-1])
        dates = dates[0:factor_value_length]

    if expect_df or len(factor) > 1:
        order_book_id_index_map = {o: i for i, o in enumerate(order_book_ids)}
        factor_index_map = {f: i for i, f in enumerate(factor)}
        arr = np.full((len(order_book_ids) * days, len(factor)), np.nan)

        for order_book_id, factor_name, values in data:
            if not values:
                continue
            value_length = min(days, len(values))
            order_book_id_index = order_book_id_index_map[order_book_id]
            factor_index = factor_index_map[factor_name]
            start = order_book_id_index * days
            arr[start: start + value_length, factor_index] = values[-value_length:]

        multi_index = pd.MultiIndex.from_product([order_book_ids, dates], names=["order_book_id", "date"])
        df = pd.DataFrame(index=multi_index, columns=factor, data=arr)
        df = _listed_only(df, order_book_ids, market)
        return df

    order_book_id_index_map = {o: i for i, o in enumerate(order_book_ids)}
    arr = np.full((days, len(order_book_ids)), np.nan)
    for order_book_id, _, values in data:
        arr[:len(values), order_book_id_index_map[order_book_id]] = values
    df = pd.DataFrame(index=dates, columns=order_book_ids, data=arr)

    if len(df.index) == 1:
        return df.iloc[0]
    if len(df.columns) == 1:
        return df[df.columns[0]]
    return df


def _listed_only(df, order_book_ids, market):
    insts = instruments(order_book_ids, market=market)

    # contruct new dataframe manually for better performance.
    indexes, listed_dates, de_listed_dates = [], [], []
    if market == "hk":
        id_field = "unique_id"
    else:
        id_field = "order_book_id"
    for i in insts:
        indexes.append(getattr(i, id_field))
        # listed_date maybe 2999-12-31, which is not listed state, let's set it to Timestamp.max
        listed_dates.append(pd.Timestamp(i.listed_date) if i.listed_date != "2999-12-31" else pd.Timestamp.max)
        de_listed_date = pd.Timestamp.max if i.de_listed_date == "0000-00-00" else pd.Timestamp(i.de_listed_date)
        de_listed_dates.append(de_listed_date)
    info = pd.DataFrame(data={"listed_date": listed_dates, "de_listed_date": de_listed_dates}, index=indexes)
    df = df.join(info, on="order_book_id")

    date = df.index.get_level_values("date")
    listed_df = df[(df["listed_date"] <= date) & (date < df["de_listed_date"])]
    return listed_df.drop(columns=["listed_date", "de_listed_date"])


def _get_factor_warning_msg(start_date, end_date):
    if start_date == end_date:
        end_date = end_date.strftime("%Y%m%d")
        warnings.warn("{} calculation not completed".format(end_date))
    else:
        start_date = start_date.strftime("%Y%m%d")
        end_date = end_date.strftime("%Y%m%d")
        warnings.warn(
            "{} - {} calculation not completed".format(start_date, end_date))


_UNIVERSE_MAPPING = {
    "whole_market": "whole_market",
    "000300.XSHG": "csi_300",
    "000905.XSHG": "csi_500",
    "000906.XSHG": "csi_800",
    "399303.XSHE": "399303_XSHE",
    "000852.XSHG": "000852_XSHG"
}

_METHOD_MAPPING = {"explicit": "explicit_factor_return", "implicit": "implicit_factor_return"}


@export_as_api
def get_factor_return(
    start_date, end_date, factors=None, universe="whole_market", method="implicit", industry_mapping="sws_2021", model="v1", market="cn"
):
    """获取因子收益率

    Parameters
    ----------
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        开始日期（例如： '2017-03-03'）
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        结束日期（例如： '2017-03-20'）
    factors : None | list[str], optional
        因子。默认获取全部因子的因子收益率(None)。
        当 method 参数取值为 'implicit'，可返回全部因子（风格、行业、市场联动）的隐式因子收益率；
        当 method 参数取值为 'explicit'，只返回风格因子的显式因子收益率。
    universe : str | list[str], optional
        股票池。默认调用全市场收益率（'whole_market'）。可选沪深300（'000300.XSHG'）、中证500（'000905.XSHG'）、中证800（'000906.XSHG'）。
    method : str, optional
        计算方法。默认为 'implicit'（隐式因子收益率），可选 'explicit'（显式风格因子收益率）。
    industry_mapping : str, optional
        'sws_2021' - 申万 2021 行业分类
        'citics_2019' - 中信 2019 行业分类
        默认：industry_mapping = 'sws_2021'.
    model : str, optional
        'v1'、'v2' 或 'v2trd' 等模型版本。
    market : str, optional
        地区代码， 现在仅支持 'cn'。

    Returns
    -------
    pandas.DataFrame
        index 为日期， column 为因子字段名称

    Examples
    --------
    获取全部因子的因子收益率

    >>> rqdatac.get_factor_return('20170302','20170307',factors=None,universe='whole_market',industry_mapping='sws_2021')

    factor          beta  book_to_price  comovement  earnings_yield    growth  leverage  liquidity  momentum  non_linear_size  residual_volatility      size      交通运输        传媒      公用事业      农林牧渔      医药生物      商贸零售
    date
    2017-03-02    ...

    """
    industry_mapping = _convert_industry_mapping(industry_mapping)
    start_date, end_date = ensure_date_range(start_date, end_date)

    if factors:
        factors = ensure_list_of_string(factors)
        if method == "implicit":
            # implicit factors fields is the same to get_factor_exposure fields.
            if model == "v1":
                check_items_in_container(factors, _EXPOSURE_FACTORS, "factors")
            elif model == "v2":
                check_items_in_container(factors, _EXPOSURE_FACTORS_RQD6, "factors")
            elif model == "v2trd":
                check_items_in_container(factors, _EXPOSURE_FACTORS_RQD6TRD, "factors")
        elif method == "explicit":
            # explicit factors fields is the same to get_style_factor_exposure fields.
            if model == "v1":
                check_items_in_container(factors, _STYLE_FACTORS, "factors")
            elif model == "v2":
                check_items_in_container(factors, _STYLE_FACTORS_RQD6, "factors")
            elif model == "v2trd":
                check_items_in_container(factors, _STYLE_FACTORS_RQD6TRD, "factors")

    method = ensure_string(method)
    if method not in _METHOD_MAPPING:
        raise ValueError("invalid method: {!r}, valid: explicit, implicit".format(method))
    method = _METHOD_MAPPING[method]

    if universe not in _UNIVERSE_MAPPING:
        raise ValueError(
            "invalid universe: {!r}, valid: {}".format(universe, list(_UNIVERSE_MAPPING.keys()))
        )
    universe = _UNIVERSE_MAPPING[universe]
    check_items_in_container(model, ["v1", "v2", "v2trd"], "model")

    model_to_api = {
        "v1": "get_factor_return_v3",
        "v2": "get_factor_return_rqd6_v2",
        "v2trd": "get_factor_return_rqd6trd",
    }
    data = get_client().execute(
        model_to_api[model], start_date, end_date, factors, universe, method, market=market, industry_mapping=industry_mapping
    )
    data = [item for item in data if universe in item]
    if not data:
        return None
    df = pd.DataFrame(data)
    # convert to required format.
    df = df.pivot(index="date", columns="factor")[universe]
    df.sort_index(inplace=True)
    return df


@export_as_api
def get_live_factor_return(method="implicit", industry_mapping="sws_2021", model="v1", market="cn"):
    """获取实时隐式因子收益率数据

    :param method: 计算方法, 目前只支持 implicit
    :param industry_mapping: 使用的行业类别, 可选 sws_2021, citics_2019, 默认为 sws_2021
    :param model: 选用的模型, 可选模型包括v1, v2; 默认为v1

    :returns: pd.DataFrame.  index数据生成的时间, column为因子字段名称
    """
    industry_mapping = _convert_industry_mapping(industry_mapping)
    method = ensure_string(method)
    if method not in _METHOD_MAPPING:
        raise ValueError("invalid method: {!r}, valid: explicit, implicit".format(method))
    method = _METHOD_MAPPING[method]
    check_items_in_container(model, ["v1", "v2", "v2trd"], "model")
    data= get_client().execute(
        "get_live_factor_return", method, industry_mapping, model, market
    )
    if not data:
        return
    # datetime 作为索引, column 为因子名称, 与 get_factor_return 保持一致
    index = int14_to_datetime(data.pop("datetime"))
    df = pd.DataFrame(data, index=[index])
    df.index.name = "datetime"
    return df


def _get_all_industries(industry_name):
    if industry_name == "sw2021":
        return SHENWAN_INDUSTRY_2021
    elif industry_name == "citics2019":
        return CITICS_INDUSTRY_2019
    else:
        return SHENWAN_INDUSTRY_2014


@export_as_api
def get_factor_exposure(order_book_ids, start_date=None, end_date=None, factors=None, industry_mapping='sws_2021', model="v1", market="cn"):
    """获取一组股票的因子暴露度

    Parameters
    ----------
    order_book_ids : str | list[str]
        证券代码（例如： '600705.XSHG'），可传入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期（例如： '2017-03-03'）
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期（例如： '2017-03-20'），不传入 start_date ,end_date 则 默认返回最近三个月的数据
    factors : None | list[str], optional
        风险因子。默认获取全部风格因子暴露度（ None）。
    industry_mapping : str, optional
        'sws_2021' - 申万 2021 行业分类
        'citics_2019' - 中信 2019 行业分类
        默认：industry_mapping = 'sws_2021'。
    model : str, optional
        'v1' 米筐老风险模型（CNE5）
        'v2' 米筐新风险模型（CNLT）
        'v2trd' 米筐新风险模型交易模型（CNTR）
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        MultiIndex 的 pandas.DataFrame
        index 第一个 level 为 order_book_id，第二个 level 为 date， columns 为因子字段名称。

    Examples
    --------
    获取单一股票的因子暴露度

    >>> rqdatac.get_factor_exposure('600705.XSHG','20170302','20170307',factors=None,industry_mapping='sws_2021' )

    date       order_book_id
    2017-03-02 600705.XSHG   -0.765684 -0.085280       0.368789        0.285730  -0.145475  1.423206            -0.643894        -0.779187           1  1.519396 -1.712015   0    0   0     0     0     0     0     0   0     0   0     0     0    0   0     0   0     0     0     0   0     0     0     1     0     0     0     0   0     0   0
    2017-03-03 600705.XSHG   -0.707014 -0.088001       0.373052        0.290378  -0.171835  1.428292            -0.801934        -0.773588           1  1.521861 -1.711590   0    0   0     0     0     0     0     0   0     0   0     0     0    0   0     0   0     0     0     0   0     0     0     1     0     0     0     0   0     0   0
    2017-03-06 600705.XSHG   -0.733606 -0.142462       0.392585        0.310572  -0.199674  1.423293            -0.796411        -0.792342           1  1.523406 -1.711635   0    0   0     0     0     0     0     0   0     0   0     0     0    0   0     0   0     0     0     0   0     0     0     1     0     0     0     0   0     0   0
    2017-03-07 600705.XSHG   -0.755761 -0.108790       0.403361        0.076728  -0.159671  1.450870            -0.727523        -0.743816           1  1.593371 -1.300758   0    0   0     0     0     0     0     0   0     0   0     0     0    0   0     0   0     0     0     0   0     0     0     1     0     0     0     0   0     0   0

    获取多股票的因子暴露度

    >>> rqdatac.get_factor_exposure(['600705.XSHG','601600.XSHG'],'20170302','20170307',factors=None,industry_mapping='sws_2021' )

    """
    industry_mapping = _convert_industry_mapping(industry_mapping)

    check_items_in_container(industry_mapping, ["sw2014", "sw2021", "citics2019"], "industry_mapping")
    check_items_in_container(model, ["v1", "v2", "v2trd"], "model")
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if not order_book_ids:
        raise ValueError("no valid order_book_id found")

    start_date, end_date = ensure_date_range(start_date, end_date)

    if factors is not None:
        factors = ensure_list_of_string(factors)
        if model == "v1":
            check_items_in_container(factors, _EXPOSURE_FACTORS, "factors")
        elif model == "v2":
            check_items_in_container(factors, _EXPOSURE_FACTORS_RQD6, "factors")
        elif model == "v2trd":
            check_items_in_container(factors, _EXPOSURE_FACTORS_RQD6TRD, "factors")


    model_to_api = {
        "v1": "get_factor_exposure_v2",
        "v2": "get_factor_exposure_rqd6",
        "v2trd": "get_factor_exposure_rqd6trd",
    }
    results = get_client().execute(
        model_to_api[model], order_book_ids, start_date, end_date, factors, industry_mapping, market
    )

    if not results:
        return None
    index_pairs = []
    data = []

    fields = [
        field for field in results[0].keys() if field not in ("order_book_id", "date", "industry")
    ]
    industry_factors = _get_all_industries(industry_mapping)
    for result in results:
        index_pairs.append((result["date"], result["order_book_id"]))
        row_data = [result.get(field, np.nan) for field in fields]

        # 填充行业因子数据
        for industry in industry_factors:
            if result["industry"] == industry:
                industry_label = 1
            else:
                industry_label = 0
            row_data.append(industry_label)

        data.append(row_data)

    index = pd.MultiIndex.from_tuples(index_pairs, names=["date", "order_book_id"])
    fields.extend(industry_factors)
    result_df = pd.DataFrame(columns=fields, index=index, data=data)
    result_df.sort_index(inplace=True)

    no_data_book_id = set(order_book_ids) - set(result_df.index.levels[1])
    if no_data_book_id:
        warnings.warn("No data for this order_book_id :{}".format(no_data_book_id))

    if factors is not None:
        return result_df[factors]
    return result_df


_EXPOSURE_FACTORS = [
    "residual_volatility",
    "growth",
    "liquidity",
    "beta",
    "non_linear_size",
    "leverage",
    "earnings_yield",
    "size",
    "momentum",
    "book_to_price",
    "comovement",
]

_EXPOSURE_FACTORS_RQD6 = [
    "liquidity",
    "leverage",
    "earnings_variability",
    "earnings_quality",
    "profitability",
    "investment_quality",
    "book_to_price",
    "earnings_yield",
    "longterm_reversal",
    "growth",
    "momentum",
    "mid_cap",
    "size",
    "beta",
    "residual_volatility",
    "dividend_yield",
    "comovement"
]

_EXPOSURE_FACTORS_RQD6TRD = _EXPOSURE_FACTORS_RQD6.copy()
_EXPOSURE_FACTORS_RQD6TRD.extend(
    ["sentiment", "seasonality", "shortterm_reversal", "industry_momentum"]
)

# 申万2021行业分类
SHENWAN_INDUSTRY_2021 = [
    u"银行",
    u"计算机",
    u"环保",
    u"商贸零售",
    u"电力设备",
    u"建筑装饰",
    u"建筑材料",
    u"农林牧渔",
    u"电子",
    u"交通运输",
    u"汽车",
    u"纺织服饰",
    u"医药生物",
    u"房地产",
    u"通信",
    u"公用事业",
    u"综合",
    u"机械设备",
    u"石油石化",
    u"有色金属",
    u"传媒",
    u"家用电器",
    u"基础化工",
    u"非银金融",
    u"社会服务",
    u"轻工制造",
    u"国防军工",
    u"美容护理",
    u"煤炭",
    u"食品饮料",
    u"钢铁"
]

# 申万2014 行业分类
SHENWAN_INDUSTRY_2014 = [
    u"农林牧渔",
    u"采掘",
    u"化工",
    u"钢铁",
    u"有色金属",
    u"电子",
    u"家用电器",
    u"食品饮料",
    u"纺织服装",
    u"轻工制造",
    u"医药生物",
    u"公用事业",
    u"交通运输",
    u"房地产",
    u"商业贸易",
    u"休闲服务",
    u"综合",
    u"建筑材料",
    u"建筑装饰",
    u"电气设备",
    u"国防军工",
    u"计算机",
    u"传媒",
    u"通信",
    u"银行",
    u"非银金融",
    u"汽车",
    u"机械设备",
]

# 中信2019 行业分类
CITICS_INDUSTRY_2019 = [
    u"交通运输",
    u"传媒",
    u"农林牧渔",
    u"医药",
    u"商贸零售",
    u"国防军工",
    u"基础化工",
    u"家电",
    u"建材",
    u"建筑",
    u"房地产",
    u"有色金属",
    u"机械",
    u"汽车",
    u"消费者服务",
    u"煤炭",
    u"电力及公用事业",
    u"电力设备及新能源",
    u"电子",
    u"石油石化",
    u"纺织服装",
    u"综合",
    u"综合金融",
    u"计算机",
    u"轻工制造",
    u"通信",
    u"钢铁",
    u"银行",
    u"非银行金融",
    u"食品饮料"
]

_EXPOSURE_FACTORS.extend(sorted(set(SHENWAN_INDUSTRY_2021 + SHENWAN_INDUSTRY_2014 + CITICS_INDUSTRY_2019)))
_EXPOSURE_FACTORS_RQD6.extend(sorted(set(SHENWAN_INDUSTRY_2021 + CITICS_INDUSTRY_2019)))
_EXPOSURE_FACTORS_RQD6TRD.extend(sorted(set(SHENWAN_INDUSTRY_2021 + CITICS_INDUSTRY_2019)))


_STYLE_FACTORS = {
    "residual_volatility",
    "growth",
    "liquidity",
    "beta",
    "non_linear_size",
    "leverage",
    "earnings_yield",
    "size",
    "momentum",
    "book_to_price"
}

_STYLE_FACTORS_RQD6 = {
    "liquidity",
    "leverage",
    "earnings_variability",
    "earnings_quality",
    "profitability",
    "investment_quality",
    "book_to_price",
    "earnings_yield",
    "longterm_reversal",
    "growth",
    "momentum",
    "mid_cap",
    "size",
    "beta",
    "residual_volatility",
    "dividend_yield",
}

_STYLE_FACTORS_RQD6TRD = _STYLE_FACTORS_RQD6.copy()
_STYLE_FACTORS_RQD6TRD.update({"sentiment", "seasonality", "shortterm_reversal", "industry_momentum"})


@export_as_api
def get_style_factor_exposure(order_book_ids, start_date, end_date, factors=None, model="v1",
                              industry_mapping="sws_2021", market="cn"):
    """获取个股风格因子暴露度

    :param order_book_ids: 证券代码（例如：‘600705.XSHG’）
    :param start_date: 开始日期（例如：‘2017-03-03’）
    :param end_date: 结束日期（例如：‘2017-03-20’）
    :param factors: 风格因子。默认调用全部因子的暴露度（'all'）。
        具体因子名称见说明文档 (Default value = None)
    :param market:  (Default value = "cn")
    :param model: 选用的模型, 可选模型包括 v1, v2; 默认为 v1

    """
    industry_mapping = _convert_industry_mapping(industry_mapping)
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    start_date, end_date = ensure_date_range(start_date, end_date)
    check_items_in_container(model, ["v1", "v2", "v2trd"], "model")
    if factors is not None:
        factors = ensure_list_of_string(factors)
        if model == "v1":
            check_items_in_container(factors, _STYLE_FACTORS, "factors")
        elif model == "v2":
            check_items_in_container(factors, _STYLE_FACTORS_RQD6, "factors")
        elif model == "v2trd":
            check_items_in_container(factors, _STYLE_FACTORS_RQD6TRD, "factors")

    model_to_api = {
        "v1": "get_style_factor_exposure",
        "v2": "get_style_factor_exposure_rqd6",
        "v2trd": "get_style_factor_exposure_rqd6trd"
    }
    df = get_client().execute(
        model_to_api[model], order_book_ids, start_date, end_date, factors,
        industry_mapping=industry_mapping, market=market
    )
    if not df:
        return
    return pd.DataFrame(df).set_index(["order_book_id", "date"]).sort_index(level=1)


_DESCRIPTORS = {
    "daily_standard_deviation",
    "cumulative_range",
    "historical_sigma",
    "one_month_share_turnover",
    "three_months_share_turnover",
    "twelve_months_share_turnover",
    "earnings_to_price_ratio",
    "cash_earnings_to_price_ratio",
    "market_leverage",
    "debt_to_assets",
    "book_leverage",
    "sales_growth",
    "earnings_growth",
    "predicted_earning_to_price",
    "short_term_predicted_earnings_growth",
    "long_term_predicted_earnings_growth"
}

_DESCRIPTORS_RQD6 = {
    "one_month_share_turnover",
    "three_months_share_turnover",
    "twelve_months_share_turnover",
    "annualized_trade_value_ratio",
    "market_leverage",
    "debt_to_assets",
    "book_leverage",
    "variation_in_sales",
    "variation_in_earnings",
    "variation_in_cashflows",
    "variation_in_fw_eps",
    "accruals_balancesheet_version",
    "accruals_cashflow_version",
    "asset_turnover",
    "gross_profitability",
    "gross_margin",
    "returns_on_asset",
    "asset_growth",
    "capital_expenditure_growth",
    "issuance_growth",
    "predicted_earning_to_price",
    "earnings_to_price_ratio",
    "cash_earnings_to_price_ratio",
    "enterprice_multiple",
    "sales_growth",
    "earnings_growth",
    "predicted_growth_3_year",
    "relative_strength",
    "historical_alpha",
    "daily_standard_deviation",
    "cumulative_range",
    "historical_sigma",
    "dividend_to_price",
    "longterm_relative_strength",
    "longterm_historical_alpha"
}

_DESCRIPTORS_RQD6TRD = _DESCRIPTORS_RQD6.copy()
_DESCRIPTORS_RQD6TRD.update({"earn", "epibs", "rribs"})


@export_as_api
def get_descriptor_exposure(order_book_ids, start_date, end_date, descriptors=None, model="v1",
                            industry_mapping="sws_2021", market="cn"):
    """获取一组股票的细分风格因子暴露度

    Parameters
    ----------
    order_book_ids : str | list[str]
        证券代码（例如： '600705.XSHG'），可传入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        开始日期（例如： '2017-03-03'）
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        结束日期（例如： '2017-03-20'）
    descriptors : None | list[str], optional
        细分风格因子。默认获取全部细分风格因子的暴露度（ None）。 细分风格因子字段名称见下方
    model : str, optional
        'v1' 米筐老风险模型（CNE5）
        'v2' 米筐新风险模型（CNLT）
        'v2trd' 米筐新风险模型交易模型（CNTR）
    industry_mapping : str, optional
        'sws_2021' - 申万 2021 行业分类
        'citics_2019' - 中信 2019 行业分类
        默认：industry_mapping = 'sws_2021'。
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        MultiIndex 的 pandas.DataFrame
        index 第一个 level 为 order_book_id，第二个 level 为 date， column 为细分风格因子字段名称。

    Examples
    --------
    获取一只股票的全部细分因子暴露度

    >>> rqdatac.get_descriptor_exposure('600705.XSHG','20170302','20170307',descriptors=None)

    book_leverage  cash_earnings_to_price_ratio  ...  three_months_share_turnover  twelve_months_share_turnover
    order_book_id date                                                     ...
    600705.XSHG   2017-03-02       1.716070                     -2.174376  ...                    -0.572241                      0.073076
    600705.XSHG   2017-03-03       1.718035                     -2.161017  ...                    -0.628732                      0.065085
    600705.XSHG   2017-03-06       1.719792                     -2.166595  ...                    -0.681997                      0.063982
    600705.XSHG   2017-03-07       1.716341                     -2.167661  ...                    -0.685791                      0.071536

    获取一只股票一组细分风格因子暴露度

    >>> rqdatac.get_descriptor_exposure('600705.XSHG','20170302','20170307',descriptors=['earnings_growth','cash_earnings_to_price_ratio','sales_growth'])

    cash_earnings_to_price_ratio  earnings_growth  sales_growth
    order_book_id date
    600705.XSHG   2017-03-02                     -2.174376        -0.412443     -0.434541
    600705.XSHG   2017-03-03                     -2.161017        -0.412443     -0.434541
    600705.XSHG   2017-03-06                     -2.166595        -0.412443     -0.434541
    600705.XSHG   2017-03-07                     -2.167661        -0.438942     -0.454265
    """
    industry_mapping = _convert_industry_mapping(industry_mapping)
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    start_date, end_date = ensure_date_range(start_date, end_date)
    check_items_in_container(model, ["v1", "v2", "v2trd"], "model")
    if descriptors is not None:
        if descriptors == "all":
            descriptors = None
        else:
            descriptors = ensure_list_of_string(descriptors)
            if model == "v1":
                check_items_in_container(descriptors, _DESCRIPTORS, "descriptors")
            elif model == "v2":
                check_items_in_container(descriptors, _DESCRIPTORS_RQD6, "descriptors")
            elif model == "v2trd":
                check_items_in_container(descriptors, _DESCRIPTORS_RQD6TRD, "descriptors")

    model_to_api = {
        "v1": "get_descriptor_exposure",
        "v2": "get_descriptor_exposure_rqd6",
        "v2trd": "get_descriptor_exposure_rqd6trd",
    }
    df = get_client().execute(
        model_to_api[model], order_book_ids, start_date, end_date, descriptors,
        industry_mapping=industry_mapping, market=market
    )
    if not df:
        return
    return pd.DataFrame(df).set_index(["order_book_id", "date"]).sort_index(level=1)


@export_as_api
def get_stock_beta(order_book_ids, start_date, end_date, benchmark="000300.XSHG", model="v1",
                   industry_mapping="sws_2021", market="cn"):
    """获取个股相对于某个基准的贝塔

    Parameters
    ----------
    order_book_ids : str | list[str]
        证券代码（例如： '600705.XSHG'），可传入 order_book_id 或 order_book_id 列表
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        开始日期（例如： '2017-03-03'）
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        结束日期（例如： '2017-03-20'）
    benchmark : str, optional
        基准指数。默认为沪深300（'000300.XSHG'），可选上证50（'000016.XSHG'）、中证500（'000905.XSHG'）、中证800（'000906.XSHG'）以及中证全指（'000985.XSHG'）。
    model : str, optional
        'v1'、'v2' 或 'v2trd' 等模型版本。
    industry_mapping : str, optional
        'sws_2021' - 申万 2021 行业分类
        'citics_2019' - 中信 2019 行业分类
        默认：industry_mapping = 'sws_2021'.
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame
        index 为日期， column 为个股的 order_book_id

    Examples
    --------
    获取一只股票的基准贝塔值

    >>> rqdatac.get_stock_beta('600705.XSHG','20170302','20170307')

    order_book_id  600705.XSHG
    date
    2017-03-02        1.396796
    2017-03-03        1.395106
    2017-03-06        1.378063
    2017-03-07        1.399051
    """
    industry_mapping = _convert_industry_mapping(industry_mapping)
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    start_date, end_date = ensure_date_range(start_date, end_date)

    all_benchmark = (
        "000300.XSHG", "000016.XSHG", "000905.XSHG", "000906.XSHG", "000985.XSHG", "000852.XSHG"
    )
    benchmark = ensure_string(benchmark, "benchmark")
    check_items_in_container(benchmark, all_benchmark, "benchmark")
    benchmark = benchmark.replace(".", "_")
    model_to_api = {
        "v1": "get_stock_beta",
        "v2": "get_stock_beta_rqd6",
        "v2trd": "get_stock_beta_rqd6trd",
    }
    df = get_client().execute(
        model_to_api[model], order_book_ids, start_date, end_date, benchmark,
        industry_mapping=industry_mapping, market=market
    )
    if not df:
        return
    df = pd.DataFrame(df)
    df = df.pivot(index="date", columns="order_book_id", values=benchmark).sort_index()
    return df


def get_eigenfactor_adjusted_covariance(date, horizon='daily', model="v1", industry_mapping="sws_2021"):
    """ 获取因子协方差矩阵（特征因子调整）

    :param date: str 日期（例如：‘2017-03-20’）
    :param horizon: str 预测期限。默认为日度（'daily'），可选月度（‘monthly’）或季度（'quarterly'）。
    :param model: 选用的模型, 可选模型包括 v1, v2; 默认为 v1
    :param industry_mapping str                     所选用的行业映射, 可选值包括 sws_2021

    :return: pandas.DataFrame，其中 index 和 column 均为因子名称。
    """
    industry_mapping = _convert_industry_mapping(industry_mapping)
    date = get_previous_trading_date(get_next_trading_date(date))
    date = ensure_date_int(date)
    ensure_string_in(horizon, HORIZON_CONTAINER, 'horizon')
    check_items_in_container(model, ["v1", "v2", "v2trd"], "model")

    model_to_api = {
        "v1": 'get_eigenfactor_adjusted_covariance',
        "v2": 'get_eigenfactor_adjusted_covariance_rqd6',
        "v2trd": 'get_eigenfactor_adjusted_covariance_rqd6trd',
    }
    df = get_client().execute(model_to_api[model], date, horizon, industry_mapping)
    if not df:
        return
    df = pd.DataFrame(df)
    df.drop("date", axis=1, inplace=True)
    return df.reindex(columns=df.index)


@export_as_api
def get_factor_covariance(date, horizon='daily', model="v1", industry_mapping="sws_2021"):
    """获取因子协方差矩阵

    Parameters
    ----------
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        参考日期（例如： '2017-03-03'）
    horizon : str, optional
        预测期限。默认为 'daily'，可选 'monthly' 或 'quarterly'
    model : str, optional
        'v1'、'v2' 或 'v2trd' 等模型版本。
    industry_mapping : str, optional
        'sws_2021' - 申万 2021 行业分类
        'citics_2019' - 中信 2019 行业分类
        默认：industry_mapping = 'sws_2021'

    Returns
    -------
    pandas.DataFrame
        方差-协方差矩阵的 DataFrame，index 和 columns 均为因子名称。

    Examples
    --------
    >>> rqdatac.get_factor_covariance('20170303', horizon='daily')

                             beta  book_to_price  comovement  earnings_yield    growth  ...
    factor
    beta                 0.001163  -2.385022e-04    0.002044   -6.211431e-05 -0.000042  ...
    book_to_price       -0.000239   2.517092e-03   -0.000964   -1.118821e-04  0.000054  ...
    comovement           0.002044  -9.643928e-04    0.016008   -1.260676e-04 -0.000253  ...
    earnings_yield      -0.000062  -1.118821e-04   -0.000126    5.460846e-04 -0.000049  ...

    >>> rqdatac.get_factor_covariance('20170303', horizon='daily', industry_mapping='citics_2019')

    """
    industry_mapping = _convert_industry_mapping(industry_mapping)
    date = get_previous_trading_date(get_next_trading_date(date))
    date = ensure_date_int(date)
    ensure_string_in(horizon, HORIZON_CONTAINER, 'horizon')
    check_items_in_container(model, ["v1", "v2", "v2trd"], "model")

    model_to_api = {
        "v1": "get_factor_covariance",
        "v2": "get_factor_covariance_rqd6",
        "v2trd": "get_factor_covariance_rqd6trd",
    }
    df = get_client().execute(model_to_api[model], date, horizon, industry_mapping)
    if not df:
        return
    df = pd.DataFrame(df)
    df.drop("date", axis=1, inplace=True)
    df = df.reindex(columns=df.index)
    df.index.name = "factor"
    return df


@export_as_api
def get_specific_return(order_book_ids, start_date, end_date, model="v1", industry_mapping="sws_2021"):
    """获取个股特异收益率

    Parameters
    ----------
    order_book_ids : str | list[str]
        证券代码（例如： '600705.XSHG'），可传入 order_book_id 或 order_book_id 列表
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        开始日期（例如： '2017-03-03'）
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        结束日期（例如： '2017-03-20'）
    model : str, optional
        'v1'、'v2' 或 'v2trd' 等模型版本。
    industry_mapping : str, optional
        'sws_2021' - 申万 2021 行业分类
        'citics_2019' - 中信 2019 行业分类
        默认：industry_mapping = 'sws_2021'.

    Returns
    -------
    pandas.DataFrame
        index 为日期， column 为个股的 order_book_id

    Examples
    --------
    获取一只股票的特异收益率

    >>> rqdatac.get_specific_return('600705.XSHG','20170302','20170307')

    order_book_id  600705.XSHG
    date
    2017-03-02       -0.012205
    2017-03-03        0.004288
    2017-03-06       -0.005601
    2017-03-07        0.022702

    获取一组股票的特异收益率

    >>> rqdatac.get_specific_return(['600705.XSHG','600100.XSHG'],'20170302','20170307')

    order_book_id  600100.XSHG  600705.XSHG
    date
    2017-03-02        0.000004    -0.012205
    2017-03-03       -0.004658     0.004288
    2017-03-06       -0.003140    -0.005601
    2017-03-07        0.013050     0.022702
    """
    industry_mapping = _convert_industry_mapping(industry_mapping)
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)
    check_items_in_container(model, ["v1", "v2", "v2trd"], "model")

    model_to_api = {
        "v1": "get_specific_return",
        "v2": "get_specific_return_rqd6",
        "v2trd": "get_specific_return_rqd6trd",
    }
    df = get_client().execute(model_to_api[model], order_book_ids, start_date, end_date, industry_mapping)
    if not df:
        return
    df = pd.DataFrame(df)
    df = df.pivot(index='date', columns='order_book_id', values="specific_return").sort_index()
    return df


@export_as_api
def get_specific_risk(order_book_ids, start_date, end_date, horizon='daily', model="v1", industry_mapping="sws_2021"):
    """获取一组股票的特异波动率

    Parameters
    ----------
    order_book_ids : str | list[str]
        证券代码（例如： '600705.XSHG'），可传入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        开始日期（例如： '2017-03-03'）
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp
        结束日期（例如： '2017-03-20'）
    horizon : str, optional
        预测期限。默认为日度（'daily'），可选月度（'monthly'）或季度（'quarterly'）
    model : str, optional
        'v1'、'v2' 或 'v2trd' 等模型版本。
    industry_mapping : str, optional
        'sws_2021' - 申万 2021 行业分类
        'citics_2019' - 中信 2019 行业分类
        默认：industry_mapping = 'sws_2021'

    Returns
    -------
    pandas.DataFrame
        index 为日期， column 为个股的 order_book_id

    Examples
    --------
    获取一只股票的特异波动率

    >>> rqdatac.get_specific_risk('600705.XSHG','20170303','20170308',horizon='daily')

    order_book_id  600705.XSHG
    date
    2017-03-03        0.191777
    2017-03-06        0.187424
    2017-03-07        0.189880
    2017-03-08        0.186224

    获取一组股票的特异波动率

    >>> rqdatac.get_specific_risk(['600705.XSHG','600100.XSHG'],'20170303','20170308',horizon='daily')

    order_book_id  600100.XSHG  600705.XSHG
    date
    2017-03-03        0.146083     0.191777
    2017-03-06        0.142691     0.187424
    2017-03-07        0.142536     0.189880
    2017-03-08        0.139857     0.186224
    """

    industry_mapping = _convert_industry_mapping(industry_mapping)
    order_book_ids = ensure_order_book_ids(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date)
    ensure_string_in(horizon, HORIZON_CONTAINER, 'horizon')
    check_items_in_container(model, ["v1", "v2", "v2trd"], "model")

    model_to_api = {
        "v1": "get_specific_risk",
        "v2": "get_specific_risk_rqd6",
        "v2trd": "get_specific_risk_rqd6trd",
    }
    df = get_client().execute(model_to_api[model], order_book_ids, start_date, end_date, horizon, industry_mapping)
    if not df:
        return
    df = pd.DataFrame(df)
    df = df.pivot(index="date", columns="order_book_id", values="specific_risk").sort_index()
    return df


def get_cross_sectional_bias(start_date, end_date, type='factor', model="v1", industry_mapping="sws_2021"):
    """ 获取横截面偏差系数

    :param start_date	    str                 	开始日期（例如：‘2017-03-03’）
    :param end_date	        str	                    结束日期（例如：‘2017-03-20’）
    :param type	            str	                    默认为 'factor'，可选 'specific'
    :param model: 选用的模型, 可选模型包括 v1, v2; 默认为 v1
    :param industry_mapping str                     所选用的行业映射, 可选值包括 sws_2021

    :return: pandas.DataFrame，其中 index 为date, column 包含 'daily'、'monthly'  和 'quarterly' 三个字段。
    """
    industry_mapping = _convert_industry_mapping(industry_mapping)
    start_date, end_date = ensure_date_range(start_date, end_date)
    ensure_string_in(type, ['factor', 'specific'], 'horizon')
    check_items_in_container(model, ["v1", "v2", "v2trd"], "model")

    model_to_api = {
        "v1": "get_cross_sectional_bias",
        "v2": "get_cross_sectional_bias_rqd6",
        "v2trd": "get_cross_sectional_bias_rqd6trd",
    }
    df = get_client().execute(model_to_api[model], start_date, end_date, type, industry_mapping)
    if not df:
        return
    df = pd.DataFrame(df)
    df = df.pivot(index='date', columns='horizon', values="bias").sort_index()
    return df


HORIZON_CONTAINER = ['daily', 'monthly', 'quarterly']


@export_as_api
def get_index_factor_exposure(
    order_book_ids, start_date=None, end_date=None, factors=None, market="cn"
):
    """获取因子暴露度

    :param order_book_ids: 股票代码或代码列表
    :param start_date: 如'2013-01-04' (Default value = None)
    :param end_date: 如'2014-01-04' (Default value = None)
    :param factors: 如'yield', 'beta', 'volatility' (Default value = None)
    :param market: 地区代码, 如'cn' (Default value = "cn")
    """
    try:
        order_book_ids = ensure_order_book_ids(order_book_ids, type="INDX", market=market)
    except ValueError:
        return

    start_date, end_date = ensure_date_range(start_date, end_date)

    if factors is not None:
        factors = ensure_list_of_string(factors)
        check_items_in_container(factors, _EXPOSURE_FACTORS, "factors")

    results = get_client().execute(
        "get_index_factor_exposure", order_book_ids, start_date, end_date, factors, market=market
    )

    if not results:
        return None
    df = pd.DataFrame.from_records(results, index=['date', 'order_book_id'])
    df.sort_index(inplace=True)
    return df


# 将行业信息定义成与 rqdatad 统一的形式.
def _convert_industry_mapping(industry_mapping):
    """ 处理用户输入的 industry_mapping 信息 """
    # True 与 False是为了跟旧版本兼容
    input_mapping = {
        True: "sw2021",
        False: "sw2014",
        "sws_2021": "sw2021",
        "sws_2014": "sw2014",
        "citics_2019": "citics2019"
    }
    return input_mapping.get(industry_mapping, industry_mapping)
