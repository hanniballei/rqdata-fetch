# -*- coding: utf-8 -*-
import datetime
import re
import warnings

import six
import pandas as pd

from rqdatac.client import get_client
from rqdatac.services import basic
from rqdatac.validators import (
    ensure_int,
    ensure_date_int,
    ensure_date_or_today_int,
    ensure_order_book_ids,
    ensure_string,
    ensure_string_in,
    check_items_in_container
)
from rqdatac.decorators import export_as_api, may_trim_bjse
from rqdatac.hk_decorators import support_hk_order_book_id
from rqdatac.utils import to_date_str
from rqdatac.rqdatah_helper import rqdatah_serialize, http_conv_list_to_csv, rqdatah_no_index_mark


@export_as_api
@may_trim_bjse
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def shenwan_industry(index_name, date=None, market="cn"):
    """获取申万行业组成

    :param index_name: 申万行业代码或名字, 如'801010.INDX'或'农林牧渔'
    :param date: 如 '2015-01-07' (Default value = None)
    :param market:  (Default value = "cn")
    :returns: 返回输入日期最近交易日的申万行业组成

    """
    if not isinstance(index_name, six.string_types):
        raise ValueError("string expected, got {!r}".format(index_name))

    if not date:
        date = datetime.date.today()
    date = ensure_date_int(date)
    return get_client().execute("shenwan_industry", index_name, date, market=market)


LEVEL_MAP = (
    None,
    ("index_code", "index_name"),
    ("second_index_code", "second_index_name"),
    ("third_index_code", "third_index_name"),
)


@export_as_api
def shenwan_instrument_industry(order_book_ids, date=None, level=1, expect_df=True, market="cn"):
    """获取股票对应的申万行业

    :param order_book_ids: 股票列表，如['000001.XSHE', '000002.XSHE']
    :param date: 如 '2015-01-07' (Default value = None)
    :param level:  (Default value = 1)
    :param expect_df: 返回 MultiIndex DataFrame (Default value = True)
    :param market:  (Default value = "cn")
    :returns: code, name
        返回输入日期最近交易日的股票对应申万行业

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

    if level not in [0, 1, 2, 3]:
        raise ValueError("level should be in 0,1,2,3")
    order_book_ids = ensure_order_book_ids(order_book_ids)

    if not date:
        date = datetime.date.today()
    date = ensure_date_int(date)

    r = get_client().execute("shenwan_instrument_industry", order_book_ids, date, level, market=market)
    if not r:
        return

    if len(order_book_ids) == 1 and not expect_df:
        r = r[0]
        if level != 0:
            return r[LEVEL_MAP[level][0]], r[LEVEL_MAP[level][1]]
        else:
            return (
                r["index_code"],
                r["index_name"],
                r["second_index_code"],
                r["second_index_name"],
                r["third_index_code"],
                r["third_index_name"],
            )

    df = pd.DataFrame(r).set_index("order_book_id")
    if level != 0 and level != 1:
        df.rename(columns=dict(zip(LEVEL_MAP[level], LEVEL_MAP[1])), inplace=True)
    return df


@export_as_api
@may_trim_bjse
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def zx_industry(industry_name, date=None):
    """获取中信行业股票列表

    :param industry_name: 中信行业名称或代码
    :param date: 查询日期，默认为当前最新日期
    :return: 所属目标行业的order_book_id list or None
    """
    if not isinstance(industry_name, six.string_types):
        raise ValueError("string expected, got {!r}".format(industry_name))
    if not date:
        date = datetime.date.today()
    date = ensure_date_int(date)
    return get_client().execute("zx_industry", industry_name, date)


ZX_LEVEL_MAP = (
    None,
    "first_industry_name",
    "second_industry_name",
    "third_industry_name",
)


@export_as_api
def zx_instrument_industry(order_book_ids, date=None, level=1, expect_df=True, market="cn"):
    """获取股票对应的中信行业

    :param order_book_ids: 股票列表，如['000001.XSHE', '000002.XSHE']
    :param date: 如 '2015-01-07' (Default value = None)
    :param level:  (Default value = 1)
    :param expect_df: 返回 MultiIndex DataFrame (Default value = True)
    :returns: code, name
        返回输入日期最近交易日的股票对应中信行业

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
    if level not in [0, 1, 2, 3]:
        raise ValueError("level should be in 0,1,2,3")
    order_book_ids = ensure_order_book_ids(order_book_ids)

    if not date:
        date = datetime.date.today()
    date = ensure_date_int(date)

    r = get_client().execute("zx_instrument_industry", order_book_ids, date, level)
    if not r:
        return
    if len(order_book_ids) == 1 and not expect_df:
        r = r[0]
        if level != 0:
            return [r[ZX_LEVEL_MAP[level]], ]
        else:
            return [
                r["first_industry_name"],
                r["second_industry_name"],
                r["third_industry_name"],
            ]

    df = pd.DataFrame(r).set_index("order_book_id")
    return df


HK_SOURCES = ["sws_2021", "citics_2019", "hsi"]
A_SHARE_SOURCES = ["sws", "citics", "gildata", "citics_2019"]


@export_as_api
@may_trim_bjse
@rqdatah_serialize(converter=http_conv_list_to_csv, name='order_book_id')
def get_industry(industry, source='citics_2019', date=None, market="cn"):
    """
    获取某行业的股票列表

    通过传入行业名称、行业指数代码或者行业代号，拿到指定行业的股票列表

    Parameters
    ----------
    industry : str
        可传入行业名称、行业指数代码或者行业代号
    source : str, optional
        分类依据。 citics: 中信, gildata: 聚源, citics_2019:中信 2019 分类, 默认 source='citics_2019'. 注意：citics 为中信 2019 年新的行业分类未发布前的分类
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询日期，默认为当前最新日期
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    list

    Examples
    --------
    得到当前某一级行业的股票列表：

    >>> get_industry('银行')
    ['000001.XSHE',
     '002142.XSHE',
     '002807.XSHE',
     '002839.XSHE',
     '002936.XSHE',
     '002948.XSHE',
     '002958.XSHE',
     '002966.XSHE',
     '600000.XSHG',
     '600015.XSHG',
     '600016.XSHG',
     '600036.XSHG',
     '600908.XSHG',
     '600919.XSHG',
     '600926.XSHG',
     '600928.XSHG',
     '601009.XSHG',
     '601128.XSHG',
     '601166.XSHG',
     '601169.XSHG',
     '601229.XSHG',
     '601288.XSHG',
     '601328.XSHG',
     '601398.XSHG',
     '601577.XSHG',
     '601818.XSHG',
     '601838.XSHG',
     '601860.XSHG',
     '601939.XSHG',
     '601988.XSHG',
     '601997.XSHG',

    """

    industry = ensure_string(industry, "industry")
    if market == "hk":
        valid_sources = HK_SOURCES
    else:
        valid_sources = A_SHARE_SOURCES

    source = ensure_string_in(source, valid_sources, "source")
    date = ensure_date_or_today_int(date)

    res = get_client().execute("get_industry", industry, source, date, market=market)

    if not res:
        return res

    from rqdatac.services import basic

    # have_sector_name 代表 industry传入的是风格版块，产业板块或者上下游产业版块
    if res[-1] == 'have_sector_name':
        res.pop()
    date = to_date_str(date)
    res = [ins.order_book_id for ins in basic.instruments(res, market=market)
           if ins.listed_date <= date and (
                   ins.de_listed_date == '0000-00-00' or date <= ins.de_listed_date
           )]
    sub_pattern = re.compile('[A-Z]+')
    res = [sub_pattern.sub('', oid[:-4]) + oid[-4:] for oid in res]
    return sorted(res)


@export_as_api
@may_trim_bjse
def get_industry_change(industry, source='citics_2019', level=None, market="cn"):
    """
    获取某行业的股票纳入剔除日期

    通过传入行业名称、行业指数代码或者行业代号，拿到指定行业的股票纳入剔除日期

    Parameters
    ----------
    industry : str
        可传入行业名称、行业指数代码或者行业代号
    source : str, optional
        分类依据。 citics_2019 - 中信新分类（2019 发布）, citics - 中信旧分类（退役中）, gildata -聚源。 默认 source='citics_2019'.
    level : int, optional
        行业分类级别，共三级，默认一级分类。参数 1,2,3 一一对应
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - start_date : pandas.Timestamp, 起始日期
        - cancel_date : pandas.Timestamp, 取消日期，2200-12-31 表示未披露

    Examples
    --------
    得到当前某一级行业的股票纳入剔除日期：

    >>> get_industry_change(industry='银行', level=1,source='citics_2019')
    start_date cancel_date
    order_book_id
    601988.XSHG   2019-12-02  2200-12-31
    601398.XSHG   2019-12-02  2200-12-31
    601328.XSHG   2019-12-02  2200-12-31
    601939.XSHG   2019-12-02  2200-12-31
    601288.XSHG   2019-12-02  2200-12-31

    """

    industry = ensure_string(industry, "industry")
    if market == "hk":
        valid_sources = HK_SOURCES
    else:
        valid_sources = A_SHARE_SOURCES
    source = ensure_string_in(source, valid_sources, "source")
    if level is not None:
        level = ensure_int(level, "level")
        check_items_in_container(level, [1, 2, 3], 'level')

    res = get_client().execute("get_industry_change", industry, source, level, market)

    if not res:
        return

    if market == "hk":
        # 针对港股, 返回的是 unique_id, 这时候通过instrument来转成 order_book_id
        # 就不在 hk_decorators 中处理了, 如果有多个api有这种返回id的场景, 再考虑放到 hk_decorators 中
        for r in res:
            inst = basic.instruments(r["order_book_id"], market="hk")
            if inst is not None:
                r["order_book_id"] = inst.order_book_id
    return pd.DataFrame.from_records(res, index=["order_book_id"])


@export_as_api
@support_hk_order_book_id
def get_instrument_industry(order_book_ids, source='citics_2019', level=1, date=None, market="cn"):
    """
    获取股票的指定行业分类

    通过 order_book_id 传入，拿到某个日期的该股票指定的行业分类

    Parameters
    ----------
    order_book_ids : str | list[str]
        股票合约代码，可输入 order_book_id, order_book_id list
    source : str, optional
        分类依据。citics_2019 - 中信新分类（2019 发布）, citics - 中信旧分类（退役中）, gildata -聚源。 默认 source='citics_2019'.
    level : int, optional
        行业分类级别，共三级，默认返回一级分类。参数 0,1,2,3 一一对应，其中 0 返回三级分类完整情况
        当 source='citics_2019' 时，level 可传入'citics_sector' 获取该股票的衍生板块及风格归属
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询日期，默认为当前最新日期
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - first_industry_code : str, 一级行业代码
        - first_industry_name : str, 一级行业名称
        - second_industry_code : str, 二级行业代码
        - second_industry_name : str, 二级行业名称
        - third_industry_code : str, 三级行业代码
        - third_industry_name : str, 三级行业名称

    Examples
    --------
    得到当前股票所对应的一级行业：

    >>> get_instrument_industry('000001.XSHE')
                       first_industry_code first_industry_name
    order_book_id
    000001.XSHE                    40                  银行

    得到当前股票组所对应的中信行业的全部分类：

    >>> get_instrument_industry(['000001.XSHE','000002.XSHE'],source='citics_2019',level=0)
                  first_industry_code first_industry_name second_industry_code second_industry_name third_industry_code third_industry_name
    order_book_id
    000001.XSHE                    40                  银行                 4020            全国性股份制银行Ⅱ              402010           全国性股份制银行Ⅲ
    000002.XSHE                    42                 房地产                 4210             房地产开发和运营              421010              住宅物业开发

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, market=market)
    if market == "hk":
        valid_sources = HK_SOURCES
    else:
        valid_sources = A_SHARE_SOURCES
    source = ensure_string_in(source, valid_sources, "source")
    if source == "citics_2019":
        check_items_in_container(level, [0, 1, 2, 3, "citics_sector"], 'level')
    else:
        check_items_in_container(level, [0, 1, 2, 3], 'level')
    date = ensure_date_or_today_int(date)

    r = get_client().execute("get_instrument_industry", order_book_ids, source, level, date, market=market)

    if not r:
        return
    res = [i['order_book_id'] for i in r]
    if source == "citics_2019" and level == "citics_sector":
        # is_special industry是否传入的是风格版块，产业板块和上下游产业版块
        from rqdatac.services import basic
        res_list = basic.instruments(res)
        date = to_date_str(date)
        for index, order_book in enumerate(res_list):
            if order_book.de_listed_date == "0000-00-00" or order_book.de_listed_date is None:
                order_book.de_listed_date = "2099-12-31"
            if not order_book.listed_date <= date <= order_book.de_listed_date:
                r.pop(index)

    return pd.DataFrame(r).set_index("order_book_id")


SHENWAN_COLUMNS = [
    "index_code",
    "index_name",
    "second_index_code",
    "second_index_name",
    "third_index_code",
    "third_index_name"
]
OTHER_COLUMNS = [
    "first_industry_code",
    "first_industry_name",
    "second_industry_code",
    "second_industry_name",
    "third_industry_code",
    "third_industry_name"
]


@export_as_api
@rqdatah_no_index_mark
def get_industry_mapping(source="citics_2019", date=None, market="cn"):
    """
    通过传入分类依据，获得对应的一二三级行业代码和名称

    Parameters
    ----------
    source : str, optional
        分类依据。 citics: 中信, gildata: 聚源,citics_2019:中信 2019 分类,默认 source='citics_2019'.注意：citics 为中信 2019 年新的行业分类未发布前的分类
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询日期，默认为当前最新日期
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - first_industry_code : str, 一级行业代码
        - first_industry_name : str, 一级行业名称
        - second_industry_code : str, 二级行业代码
        - second_industry_name : str, 二级行业名称
        - third_industry_code : str, 三级行业代码
        - third_industry_name : str, 三级行业名称

    Examples
    --------
    得到当前行业分类的概览：

    >>> get_industry_mapping()
         first_industry_code first_industry_name second_industry_code second_industry_name third_industry_code third_industry_name
    0                    10                石油石化                 1010                 石油开采              101010                石油开采
    1                    10                石油石化                 1020                 石油化工              102010                  炼油
    2                    10                石油石化                 1020                 石油化工              102040             油品销售及仓储
    3                    10                石油石化                 1020                 石油化工              102050                其他石化
    4                    10                石油石化                 1030                 油田服务              103010                油田服务
    5                    11                  煤炭                 1110               煤炭开采洗选              111010                 动力煤

    """
    if market == "hk":
        valid_sources = HK_SOURCES
    else:
        valid_sources = A_SHARE_SOURCES
    source = ensure_string_in(source, valid_sources, "source")
    if date is None:
        date = datetime.date.today()
    date = ensure_date_int(date)
    res = get_client().execute("get_industry_mapping_v2", source, date, market=market)
    if not res:
        return
    df = pd.DataFrame(res)

    if source.startswith("sws"):
        df.rename(columns=dict(zip(OTHER_COLUMNS, SHENWAN_COLUMNS)), inplace=True)
        columns = SHENWAN_COLUMNS
    else:
        columns = OTHER_COLUMNS

    df = df.dropna().drop_duplicates()
    df = df.sort_values(columns[::2]).reset_index(drop=True)
    return df[columns]
