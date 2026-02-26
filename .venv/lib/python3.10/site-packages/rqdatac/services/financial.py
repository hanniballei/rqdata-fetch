# -*- coding: utf-8 -*-
import datetime

import pandas as pd
import math

from rqdatac.client import get_client

from rqdatac.services.orm.pit_financials_ex import FIELDS_LIST_EX
from rqdatac.services.orm.hk_pit_financials_ex import HK_FIELDS_LIST_EX
from rqdatac.hk_decorators import support_hk_order_book_id
from rqdatac.validators import (
    ensure_list_of_string,
    ensure_string,
    check_items_in_container,
    ensure_date_int,
    ensure_order_book_id,
    check_quarter,
    ensure_date_or_today_int,
    quarter_string_to_date,
    ensure_order_book_ids,
)
from rqdatac.decorators import export_as_api

ENTERPRISE_TYPE_MAP = {
    13: "business_bank",
    31: "securities_firms",
    33: "trust",
    35: "insurance_company",
    39: "other_financial_institution",
    99: "general_enterprise",
}

INFO_TYPE_MAP = {
    10: "发行上市书",
    20: "定期报告",
    30: "业绩快报",
    50: "章程制度",
    70: "临时公告",
    90: "交易所通报",
    91: "交易所临时停(复)牌公告",
    99: "其他",
    110101: "定期报告:年度报告",
    110102: "定期报告:半年度报告",
    110103: "定期报告:第一季报",
    110104: "定期报告:第三季报",
    110105: "定期报告:审计报告",
    110106: "定期报告:第二季报",
    110107: "定期报告:第四季报",
    110108: "定期报告:第五季报",
    110109: "定期报告:第二季报（更正后）",
    110110: "定期报告:第四季报（更正后）",
    110111: "定期报告:第五季报（更正后）",
    110201: "定期报告:年度报告(关联方)",
    110202: "定期报告:半年度报告(关联方)",
    110203: "定期报告:第一季报(关联方)",
    110204: "定期报告:第三季报(关联方)",
    120101: "临时公告:审计报告(更正后)",
    120102: "临时公告:年度报告(更正后)",
    120103: "临时公告:半年度报告(更正后)",
    120104: "临时公告:第一季报(更正后)",
    120105: "临时公告:第三季报(更正后)",
    120106: "临时公告:公开转让说明书(更正后)",
    120107: "临时公告:业绩快报",
    120108: "临时公告:业绩快报(更正后)",
    120201: "临时公告:跟踪评级报告",
    120202: "临时公告:同业存单发行计划",
    120203: "临时公告:比较式财务报表",
    120204: "临时公告:关联方",
    120205: "临时公告:其他",
    120206: "临时公告:前期差错更正",
    120207: "临时公告:第一季度报告",
    120208: "临时公告:第二季度报告",
    120209: "临时公告:第三季度报告",
    120210: "临时公告:第四季度报告",
    120211: "临时公告：年度报告",
    130101: "发行上市书:募集说明书",
    130102: "发行上市书:招股说明书(申报稿)",
    130103: "发行上市书:招股意向书",
    130104: "发行上市书:上市公告书",
    130105: "发行上市书:审阅报告",
    130106: "发行上市书:招股说明书",
    130107: "发行上市书:公开转让说明书",
    130108: "发行上市书:发行公告",
    130109: "发行上市书:审计报告",
    130110: "发行上市书:关联方",
    130111: "发行上市书:其他",
    140101: "发行披露文件:第一季报",
    140102: "发行披露文件:半年度报告",
    140103: "发行披露文件：第三季报",
    140104: "发行披露文件：审计报告",
    140105: "发行披露文件：募集说明书",
    140106: "发行披露文件：跟踪评级报告"
}


@export_as_api
@support_hk_order_book_id
def get_pit_financials_ex(order_book_ids, fields, start_quarter, end_quarter,
                          date=None, statements='latest', market='cn'):
    """
    查询季度财务信息(point-in-time 形式)

    以给定一个报告期回溯的方式获取季度基础财务数据（三大表），即利润表，资产负债表，现金流量表。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list
    fields : list
        需要返回的财务字段。
    start_quarter : str
        财报回溯查询的起始报告期，例如'2015q2'代表 2015 年半年报 。
    end_quarter : str
        财报回溯查询的截止报告期，例如'2015q4'代表 2015 年年报。
    date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        查询日期，默认查询日期为当前最新日期
    statements : str, optional
        基于查询日期，返回某一个报告期的所有记录或最新一条记录，设置 statements 为 all 时返回所有记录，statements 等于 latest 时返回最新的一条记录，默认为 latest.
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    pandas.DataFrame
        包含以下字段：
        - quarter : str, 报告期
        - info_date : pandas.Timestamp, 公告发布日
        - fields : list, 需要返回的财务字段。需要返回的财务字段。支持的字段仅限利润表、资产负债表、现金流量表三大表字段，具体字段见下方返回。
        - if_adjusted : int, 是否为非当期财报数据, 0 代表当期，1 代表非当期（比如 18 年的财报会披露本期和上年同期的数值，17 年年报的财务数值在 18 年年报中披露的记录则为非当期， 17 年年报的财务数值在 17 年年报中披露则为当期。

    Examples
    --------
    获取股票 2018q2-2018q3 各报告期最新一次记录

    >>> get_pit_financials_ex(fields=['revenue','net_profit'], start_quarter='2018q2', end_quarter='2018q3',order_book_ids=['000001.XSHE','000048.XSHE'])
                      info_date revenue if_adjusted net_profit
    order_book_id quarter
    000001.XSHE 2018q2 2019-08-08 5.724100e+10 1 1.337200e+10
                2018q3 2019-10-22 8.666400e+10 1 2.045600e+10
    000048.XSHE 2018q2 2019-08-31 7.362684e+08 1 -3.527276e+07
                2018q3 2019-10-31 1.216331e+09 1 -4.566952e+07

    获取股票 2018q2 所有的记录

    >>> get_pit_financials_ex(fields=['revenue','net_profit'], start_quarter='2018q2', end_quarter='2018q2',order_book_ids=['000001.XSHE','000048.XSHE'],statements='all')
                      info_date revenue if_adjusted net_profit
    order_book_id quarter
    000001.XSHE 2018q2 2018-08-16 5.724100e+10 0 1.337200e+10
                2018q2 2019-08-08 5.724100e+10 1 1.337200e+10
    000048.XSHE 2018q2 2018-08-31 1.063670e+09 0 7.790205e+07
                2018q2 2018-10-31 1.060487e+09 0 7.880372e+07
                2018q2 2019-06-15 7.362684e+08 0 -3.527276e+07
                2018q2 2019-08-31 7.362684e+08 1 -3.527276e+07
    """
    fields = ensure_list_of_string(fields, 'fields')
    if market == "hk":
        check_items_in_container(fields, HK_FIELDS_LIST_EX, "fields")
        fields.extend(["fiscal_year", "standard"])
    else:
        check_items_in_container(fields, FIELDS_LIST_EX, "fields")
    fields.extend(['order_book_id', 'info_date', 'end_date', 'if_adjusted', 'rice_create_tm'])
    fields = list(set(fields))
    fields[fields.index("info_date")], fields[0] = fields[0], fields[fields.index("info_date")]

    check_quarter(start_quarter, 'start_quarter')
    start_quarter_int = ensure_date_int(quarter_string_to_date(start_quarter))

    check_quarter(end_quarter, 'end_quarter')
    end_quarter_int = ensure_date_int(quarter_string_to_date(end_quarter))

    if start_quarter > end_quarter:
        raise ValueError(
            'invalid quarter range: [{!r}, {!r}]'.format(
                start_quarter, end_quarter))

    date = ensure_date_or_today_int(date)

    order_book_ids = ensure_list_of_string(order_book_ids, 'order_book_ids')

    if statements not in ['all', 'latest']:
        raise ValueError("invalid statements , got {!r}".format(statements))

    pit_financial_df = pd.DataFrame(
        get_client().execute("get_pit_financials_ex", order_book_ids, fields, start_quarter_int, end_quarter_int, date,
                             statements, market))
    if pit_financial_df.empty:
        return
    # convert rice_create_tm to datetime
    pit_financial_df['rice_create_tm'] = pd.to_datetime(pit_financial_df['rice_create_tm'] + 3600 * 8, unit='s')
    pit_financial_df = pit_financial_df.reindex(columns=fields)
    pit_financial_df.sort_values(['order_book_id', 'end_date', 'info_date'])
    pit_financial_df["end_date"] = pit_financial_df["end_date"].apply(
        lambda d: "{}q{}".format(d.year, math.ceil(d.month / 3)))
    pit_financial_df.rename(columns={"end_date": "quarter"}, inplace=True)
    pit_financial_df.set_index(['order_book_id', 'quarter'], inplace=True)
    pit_financial_df['if_adjusted'] = pit_financial_df['if_adjusted'].map(lambda x: 1 if x == 1 else 0).astype(int)
    pit_financial_df.sort_index(inplace=True)
    return pit_financial_df


@export_as_api
@support_hk_order_book_id
def current_performance(
        order_book_ids, info_date=None, quarter=None, interval="1q", fields=None, market="cn"
):
    """
    查询财务快报数据

    默认返回给定的 order_book_id 当前最近一期的快报数据。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码。
    info_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        公告日期。如果不填(info_date 和 quarter 都为空)，则返回当前日期的最新发布的快报。如果填写，则从 info_date 当天或者之前最新的报告开始抓取。
    quarter : str, optional
        info_date 参数优先级高于 quarter。如果 info_date 填写了日期，则不查看 quarter 这个字段。 如果 info_date 没有填写而 quarter 有填写，则财报回溯查询的起始报告期，例如'2015q2', '2015q4'分别代表 2015 年半年报以及年报。默认只获取当前报告期财务信息
    interval : str, optional
        查询财务数据的间隔。例如，填写'5y'，则代表从报告期开始回溯 5 年，每年为相同报告期数据；'3q'则代表从报告期开始向前回溯 3 个季度。不填写默认抓取一期。
    fields : str | list[str], optional
        抓取对应有效字段返回。默认返回所有字段。具体快报字段见下方。
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame

    Examples
    --------
    获取单只股票过去一个报告期的快报数据

    >>> current_performance('000004.XSHE')
          end_date  info_date  operating_revenue    gross_profit    operating_profit    total_profit    np_parent_owners    net_profit_cut    net_operate_cashflow...roe_cut_weighted_yoy    net_operate_cash_flow_per_share_yoy    net_asset_psto_opening
    0   2017-12-31  2018-04-14    1.386058e+08           NaN             8796946.37       9716431.21      8566720.65         NaN                NaN                    NaN                                NaN                               NaN

    获取单只股票多个报告期的总利润

    >>> current_performance('000004.XSHE',quarter='2017q4',fields='total_profit',interval='2q')
      end_date  info_date  total_profit
    0  2017-12-31  2018-04-14  9716431.21
    1  2015-12-31  2016-04-15  10808606.48

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, 'CS', market=market)

    end_date = None
    if info_date:
        info_date = ensure_date_int(info_date)
    elif quarter:
        splited = quarter.lower().split("q")
        if len(quarter) != 6 or len(splited) != 2:
            raise ValueError(
                "invalid argument {}: {}, valid parameter: {}".format(
                    "quarter", quarter, "string format like '2016q1'"
                )
            )

        year, quarter = int(splited[0]), int(splited[1])
        if not 1 <= quarter <= 4:
            raise ValueError(
                "invalid argument {}: {}, valid parameter: {}".format(
                    "quarter", quarter, "quarter should be in [1, 4]"
                )
            )
        month, day = QUARTER_DATE_MAP[quarter]
        end_date = ensure_date_int(datetime.datetime(year, month, day))
    else:
        info_date = ensure_date_int(datetime.date.today())
    ensure_string(interval, "interval")
    if interval[-1] not in ("y", "q", "Y", "Q"):
        raise ValueError(
            "invalid argument {}: {}, valid parameter: {}".format(
                "interval", interval, "interval unit should be q(quarter) or y(year)"
            )
        )

    try:
        int(interval[:-1])
    except ValueError:
        raise ValueError(
            "invalid argument {}: {}, valid parameter: {}".format(
                "interval", interval, "string like 4q, 2y"
            )
        )
    interval = interval.lower()

    if fields is not None:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, PERFORMANCE_FIELDS, "fields")
    else:
        fields = PERFORMANCE_FIELDS

    data = get_client().execute(
        "current_performance_v2", order_book_ids, info_date, end_date, fields, market=market
    )
    if not data:
        return
    df = pd.DataFrame(data)
    df.sort_values(by=["order_book_id", "end_date", "info_date", "mark"], ascending=[True, False, False, True], inplace=True)
    df.drop_duplicates(subset=['order_book_id', "end_date"], keep="first", inplace=True)
    num = int(interval[:-1])
    unit = interval[-1]
    if unit == "y":
        latest_month = df.iloc[0]["end_date"].month
        df["month"] = df.end_date.apply(lambda x: x.month)
        df = df[df.month == latest_month]
    df.reset_index(drop=True, inplace=True)
    df = df.groupby('order_book_id').head(num)
    df.set_index(['order_book_id', 'end_date'], inplace=True)
    return df[['info_date'] + fields]


PERFORMANCE_FORECAST_FIELDS = [
    "forecast_type",
    "forecast_description",
    "forecast_growth_rate_floor",
    "forecast_growth_rate_ceiling",
    "forecast_earning_floor",
    "forecast_earning_ceiling",
    "forecast_np_floor",
    "forecast_np_ceiling",
    "forecast_eps_floor",
    "forecast_eps_ceiling",
    "net_profit_yoy_const_forecast",
    "forecast_ne_floor",
    "forecast_ne_ceiling",
]


@export_as_api
@support_hk_order_book_id
def performance_forecast(order_book_ids, info_date=None, end_date=None, fields=None, market="cn"):
    """
    查询业绩预告数据

    默认返回给定的 order_book_ids 当前最近一期的业绩预告数据。
    业绩预告主要用来调取公司对即将到来的财务季度的业绩预期的信息。有时同一个财务季度会有多条记录，分别是季度预期和累计预期（即本年至今）。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list。
    info_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        公告日期。如果不填(info_date 和 end_date 都为空)，则返回当前日期的最新发布的业绩预告。如果填写，则从 info_date 当天或者之前最新的报告开始抓取。注：info_date 优先级高于 end_date
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        对应财务预告期末日期，如'20150331'。
    fields : str | list[str], optional
        抓取对应有效字段返回。默认返回所有字段。具体业绩预告字段见下方
    market : str, optional
        默认是中国内地市场('cn')

    Returns
    -------
    pandas.DataFrame

    Examples
    --------
    获取单只股票过去一个报告期的预告数据

    >>> performance_forecast('000001.XSHE')
        info_date  end_date  forecast_type  forecast_description  forecast_growth_rate_floor  forecast_growth_rate_ceiling  forecast_earning_floor  forecast_earning_ceiling  forecast_np_floor  forecast_np_ceiling  forecast_eps_floor  forecast_eps_ceiling  net_profit_yoy_const_forecast
    0  2016-01-21  2015-12-31  预增          累计利润              5.0                      15.0                          NaN                  NaN                      2.079206e+10      2.277225e+10          1.48              1.62                  16.0

    获取多只股票过去一个报告期指定字段的预告数据

    >>> performance_forecast(['000001.XSHE','000006.XSHE'],fields=['forecast_description','forecast_earning_floor'])
            info_date end_date forecast_description forecast_earning_floor
    order_book_id
    000001.XSHE 2016-01-21 2015-12-31 累计利润         NaN
    000006.XSHE 2020-04-09 2020-12-31 累计收入         NaN

    """
    order_book_ids = ensure_order_book_ids(order_book_ids, type='CS')
    if info_date:
        info_date = ensure_date_int(info_date)
    elif end_date:
        end_date = ensure_date_int(end_date)
    else:
        info_date = ensure_date_int(datetime.datetime.today())

    if fields:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, PERFORMANCE_FORECAST_FIELDS, "fields")
    else:
        fields = PERFORMANCE_FORECAST_FIELDS

    data = get_client().execute(
        "performance_forecast_v2", order_book_ids, info_date, end_date, fields, market=market
    )
    if not data:
        return

    have_rice_create_tm = "rice_create_tm" in data[0]
    if len(order_book_ids) > 1:
        columns = ["order_book_id", "info_date", "end_date"] + fields
    else:
        columns = ["info_date", "end_date"] + fields
    if have_rice_create_tm:
        columns.append("rice_create_tm")

    df = pd.DataFrame(data, columns=columns)

    if len(order_book_ids) > 1:
        df.set_index("order_book_id", inplace=True)
    if have_rice_create_tm:
        df['rice_create_tm'] = pd.to_datetime(df['rice_create_tm'] + 3600 * 8, unit='s')
    return df


PERFORMANCE_FIELDS = [
    "operating_revenue",
    "gross_profit",
    "operating_profit",
    "total_profit",
    "np_parent_owners",
    "net_profit_cut",
    "net_operate_cashflow",
    "total_assets",
    "se_parent_owners",
    "se_without_minority",
    "total_shares",
    "basic_eps",
    "eps_weighted",
    "eps_cut_epscut",
    "eps_cut_weighted",
    "roe",
    "roe_weighted",
    "roe_cut",
    "roe_cut_weighted",
    "net_operate_cashflow_per_share",
    "equity_per_share",
    "operating_revenue_yoy",
    "gross_profit_yoy",
    "operating_profit_yoy",
    "total_profit_yoy",
    "np_parent_minority_pany_yoy",
    "ne_t_minority_ty_yoy",
    "net_operate_cash_flow_yoy",
    "total_assets_to_opening",
    "se_without_minority_to_opening",
    "basic_eps_yoy",
    "eps_weighted_yoy",
    "eps_cut_yoy",
    "eps_cut_weighted_yoy",
    "roe_yoy",
    "roe_weighted_yoy",
    "roe_cut_yoy",
    "roe_cut_weighted_yoy",
    "net_operate_cash_flow_per_share_yoy",
    "net_asset_psto_opening",
]

QUARTER_DATE_MAP = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}
