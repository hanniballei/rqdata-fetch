# -*- coding: utf-8 -*-
import datetime
import warnings
import pandas as pd
import numpy as np

from rqdatac.services.basic import instruments
from rqdatac.services.calendar import (
    get_next_trading_date,
    is_trading_date,
    get_previous_trading_date,
    current_trading_date,
)
from rqdatac.services.live import get_ticks

from rqdatac.validators import (
    ensure_string,
    ensure_list_of_string,
    check_items_in_container,
    ensure_instruments,
    ensure_date_range,
    is_panel_removed,
    raise_for_no_panel,
    ensure_order_book_ids,
)
from rqdatac.utils import (
    to_date_int,
    to_datetime,
    to_date,
    to_time,
    int8_to_datetime,
    int17_to_datetime_v,
    int17_to_datetime,
    date_to_int8,
    string_types
)
from rqdatac.share.errors import GatewayError
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api, ttl_cache, compatible_with_parm, retry
from rqdatac.hk_decorators import support_hk_order_book_id
from rqdatac.share.errors import PermissionDenied, MarketNotSupportError, NoSuchService


@export_as_api
@support_hk_order_book_id
@compatible_with_parm(name="country", value="cn", replace="market")
def get_price(
        order_book_ids,
        start_date=None,
        end_date=None,
        frequency="1d",
        fields=None,
        adjust_type="pre",
        skip_suspended=False,
        expect_df=True,
        time_slice=None,
        market="cn",
        **kwargs
):
    """
    获取指定合约或合约列表的行情数据（包含起止日期，周线、日线、分钟线和tick）。

    目前支持中国市场的股票、期货、期权、ETF、常见指数和上金所现货的行情数据，如黄金、铂金和白银产品。

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期
    frequency : str, optional
        历史数据的频率。 现在支持周/日/分钟/tick 级别的历史数据，默认为'1d'。
        1m - 分钟线
        1d - 日线
        1w - 周线，只支持'1w'
        日线和分钟可选取不同频率，例如'5m'代表 5 分钟线。
    fields : str | list[str], optional
        字段名称
    adjust_type : str, optional
        权息修复方案，仅对股票和 ETF 有效，默认为'pre'。
        不复权 - 'none'，
        前复权 - 'pre'，后复权 - 'post'，
        前复权 - 'pre_volume', 后复权 - 'post_volume'
        两组前后复权方式仅 volume 字段处理不同，其他字段相同。其中'pre'、'post'中的 volume 采用拆分因子调整；'pre_volume'、'post_volume'中的 volume 采用复权因子调整。
    skip_suspended : bool, optional
        是否跳过停牌数据。默认为 False，不跳过，用停牌前数据进行补齐。True 则为跳过停牌期。
    expect_df : bool, optional
        默认返回 pandas dataframe。如果调为 False，则返回原有的数据结构,周线数据需设置 expect_df=True
    time_slice : str | datetime.time, optional
        开始、结束时间段。默认返回当天所有数据。
        支持分钟 / tick 级别的切分，详见下方范例。
    market : str, optional
        默认是中国内地市场('cn') 。可选'cn' - 中国内地市场；'hk' - 香港市场

    Returns
    -------
    pandas.DataFrame

    Examples
    --------
    获取单一股票历史日线行情:

    >>> get_price('000001.XSHE', start_date='2015-04-01', end_date='2015-04-12', fields='close',frequency='1m',expect_df=True)
                                        close
    order_book_id  datetime
    000001.XSHE  2015-04-01 09:31:00  10.3985
                  2015-04-01 09:32:00  10.3721
                  2015-04-01 09:33:00  10.3655

    获取股票列表历史 tick 行情:

    >>> get_price(['000001.XSHE', '000002.XSHE'], start_date='2019-04-01', end_date='2019-04-01',frequency='tick')
                                      trading_date   open   last   high    low  prev_close       volume  ...      a5_v       b1_v      b2_v      b3_v      b4_v      b5_v  change_rate
    order_book_id datetime                                                                               ...
    000002.XSHE   2019-04-01 09:15:00   2019-04-01   0.00  30.72   0.00   0.00       30.72          0.0  ...       0.0     1100.0       0.0       0.0       0.0       0.0     0.000000
                  2019-04-01 09:15:09   2019-04-01   0.00  30.72   0.00   0.00       30.72          0.0  ...       0.0   158400.0       0.0       0.0       0.0       0.0     0.000000
                  2019-04-01 09:15:18   2019-04-01   0.00  30.72   0.00   0.00       30.72          0.0  ...       0.0   164300.0       0.0       0.0       0.0       0.0     0.000000
    ···
    000001.XSHE   2019-04-01 14:56:33   2019-04-01  12.83  13.19  13.55  12.83       12.82  192947985.0  ...  574800.0   201700.0  605367.0  107200.0  180800.0  254236.0     0.028861
                  2019-04-01 14:56:36   2019-04-01  12.83  13.19  13.55  12.83       12.82  192963385.0  ...  574800.0   219000.0  605367.0  106400.0  180800.0  254336.0     0.028861
                  2019-04-01 14:56:39   2019-04-01  12.83  13.19  13.55  12.83       12.82  192979885.0  ...  574800.0   236200.0  605367.0  106400.0  180800.0  254236.0     0.028861

    获取单一期货 20220512 - 20220513 每个交易日夜盘 23:55 至日盘 09:05 的历史分钟行情

    >>> get_price('AG2209', start_date='20220512', end_date='20220513',frequency='1m',time_slice=('23:55', '09:05'))
                                      trading_date     low   close  volume    high  open_interest  total_turnover    open
    order_book_id datetime
    AG2209        2022-05-11 23:55:00   2022-05-12  4794.0  4795.0    25.0  4796.0        37996.0       1797975.0  4796.0
                  2022-05-11 23:56:00   2022-05-12  4795.0  4795.0     2.0  4795.0        37996.0        143850.0  4795.0
                  2022-05-11 23:57:00   2022-05-12  4795.0  4795.0     0.0  4795.0        37996.0             0.0  4795.0
                  2022-05-11 23:58:00   2022-05-12  4792.0  4792.0    46.0  4795.0        38020.0       3307065.0  4795.0
                  2022-05-11 23:59:00   2022-05-12  4792.0  4794.0    58.0  4794.0        37998.0       4169835.0  4792.0
    ...                                        ...     ...     ...     ...     ...            ...             ...     ...
                  2022-05-13 09:01:00   2022-05-13  4656.0  4664.0   350.0  4666.0        40266.0      24478875.0  4656.0
                  2022-05-13 09:02:00   2022-05-13  4659.0  4660.0   134.0  4665.0        40297.0       9368910.0  4664.0

    获取单一股票不复权的历史周线行情:

    >>> get_price('000001.XSHE',start_date='2015-04-01', end_date='2015-04-12',frequency='1w',adjust_type='none',expect_df=True)
      total_turnover low open close volume num_trades high
    order_book_id date
    000001.XSHE 2015-04-10 2.281686e+10 16.15 16.15 19.8 1.284539e+09 554132.0 19.8
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
    sliceable = frequency.endswith(("m", "tick"))
    # check time_slice
    if time_slice:
        if not sliceable:
            warnings.warn("param [time_slice] only take effect when getting minbar or tick.")
        if not isinstance(time_slice, (tuple, list)) or len(time_slice) != 2:
            raise ValueError("time_slice: invalid, expect tuple or list value like ('09:55', '10:11'), got {}".format(time_slice))
        start, end = to_time(time_slice[0]), to_time(time_slice[1])

    df = _get_price(
        order_book_ids, start_date, end_date, frequency,
        fields, adjust_type, skip_suspended, expect_df, market, **kwargs
    )

    if df is None or not sliceable or not time_slice:
        # 非tick、minbar或者不指定切片时间，直接返回
        return df

    # parse slice time_slice
    index = df.index.get_level_values('datetime')
    if start > end:
        # 期货夜盘，可以指定end<start,表示从夜盘到第二天日盘
        mask = (start <= index.time) | (index.time <= end)
    else:
        mask = (start <= index.time) & (index.time <= end)

    return df[mask]


@retry(3, suppress_exceptions=(GatewayError, ), delay=3.0)
def _get_price(
        order_book_ids,
        start_date=None,
        end_date=None,
        frequency="1d",
        fields=None,
        adjust_type="pre",
        skip_suspended=False,
        expect_df=True,
        market="cn",
        **kwargs
):
    # tick数据
    if frequency == "tick":
        return get_tick_price(order_book_ids, start_date, end_date, fields, expect_df, market)
    elif frequency.endswith(("d", "m", "w")):
        duration = int(frequency[:-1])
        frequency = frequency[-1]
        assert 1 <= duration <= 240, "frequency should in range [1, 240]"
        if frequency == 'w' and duration not in (1,):
            raise ValueError("Weekly frequency should be str '1w'")
    else:
        raise ValueError("frequency should be str like 1d, 1m, 5m or tick")
    # 验证adjust_type
    if "adjusted" in kwargs:
        adjusted = kwargs.pop("adjusted")
        adjust_type = "pre" if adjusted else "none"

    if kwargs:
        raise ValueError('unknown kwargs: {}'.format(kwargs))

    valid_adjust = ["pre", "post", "none", "pre_volume", "post_volume"]
    ensure_string(adjust_type, "adjust_type")
    check_items_in_container(adjust_type, valid_adjust, "adjust_type")
    order_book_ids = ensure_list_of_string(order_book_ids, "order_book_ids")

    assert isinstance(skip_suspended, bool), "'skip_suspended' should be a bool"
    assert isinstance(expect_df, bool), "'expect_df' should be a bool"

    order_book_ids, stocks, funds, indexes, futures, futures888, spots, options, convertibles, repos = classify_order_book_ids(
        order_book_ids, market
    )
    if not order_book_ids:
        warnings.warn("no valid instrument")
        return
    start_date, end_date = _ensure_date(
        start_date, end_date, stocks, funds, indexes, futures, spots, options, convertibles, repos
    )
    from rqdatac.services.detail.get_price_df import get_price_df, get_week_df
    if frequency != 'w':
        df = get_price_df(
            order_book_ids, start_date, end_date, frequency, duration, fields, adjust_type, skip_suspended,
            stocks, funds, indexes, futures, futures888, spots, options, convertibles, repos, market
        )
        if df is None or expect_df:
            return df
        # 单个合约
        if len(df.index.levels[0]) == 1:
            df.reset_index(level=0, inplace=True, drop=True)
            # df.index.name = None
            if len(df.columns) == 1:
                df = df[df.columns[0]]
            return df
        # 单个字段
        elif len(df.columns) == 1:
            field = df.columns[0]
            df = df.unstack(0)[field]
            # df.index.name = None
            df.columns.name = None
            return df
        raise_for_no_panel(False)
        warnings.warn("Panel is removed after pandas version 0.25.0."
                      " the default value of 'expect_df' will change to True in the future.")
        # 交换index的顺序，以制作panel
        return df.swaplevel().to_panel()
    else:
        if not expect_df:
            raise ValueError(
                "Weekly frequency can only return a DataFrame object, set 'expect_df' to True to resolve this")
        if skip_suspended:
            raise ValueError(
                "Weekly frequency does not support skipping suspended trading days, set 'skip_suspended' to False to resolve this")
        start_date, end_date = _weekly_start_end_date_handler(start_date, end_date)
        if start_date > end_date:
            # 如果*当周没有结束*
            # 或者start date 和 end date 不能涵盖当周所有的交易日，查询该周的数据时返回为空。
            return None
        return get_week_df(order_book_ids, start_date, end_date, fields, adjust_type, market,
                           *(stocks, funds, indexes, futures, futures888, spots, options, convertibles, repos))


def _ensure_date(start_date, end_date, stocks, funds, indexes, futures, spots, options, convertibles, repos):
    default_start_date, default_end_date = ensure_date_range(start_date, end_date)

    only_futures = futures and (not stocks) and (not funds) and (not indexes) and (not spots) and (
        not options) and (not convertibles) and (not repos)
    if only_futures and len(futures) == 1:
        # 如果只有一只期货, 则给 start_date 和 end_date 合适的默认值
        # 连续合约的listed_date和de_listed_date都为0, 因此需要特殊处理
        if futures[0].listed_date != "0000-00-00":
            default_start_date = to_date_int(futures[0].listed_date)
        if futures[0].de_listed_date != "0000-00-00":
            default_end_date = to_date_int(futures[0].de_listed_date)

    start_date = to_date_int(start_date) if start_date else default_start_date
    end_date = to_date_int(end_date) if end_date else default_end_date
    if start_date < 20000104:
        warnings.warn("start_date is earlier than 2000-01-04, adjusted to 2000-01-04")
        start_date = 20000104
    return start_date, end_date


def _ensure_fields(fields, fields_dict, stocks, funds, futures, futures888, spots, options, convertibles, indexes,
                   repos):
    has_dominant_id = False
    future_only = futures and not any([stocks, funds, spots, options, convertibles, indexes, repos])
    all_fields = set(fields_dict["common"])
    if futures:
        all_fields.update(fields_dict["future"])
    if stocks:
        all_fields.update(fields_dict["stock"])
    if funds:
        all_fields.update(fields_dict["fund"])
    if spots:
        all_fields.update(fields_dict["spot"])
    if options:
        all_fields.update(fields_dict["option"])
    if convertibles:
        all_fields.update(fields_dict["convertible"])
    if indexes:
        all_fields.update(fields_dict["index"])
    if repos:
        all_fields.update(fields_dict["repo"])
    if future_only and futures888 and len(futures) == len(futures888) and not fields:
        has_dominant_id = True

    if fields:
        fields = ensure_list_of_string(fields, "fields")
        fields_set = set(fields)
        if len(fields_set) < len(fields):
            warnings.warn("duplicated fields: %s" % [f for f in fields if fields.count(f) > 1])
            fields = list(fields_set)
        # 只有期货类型
        if 'dominant_id' in fields:
            fields.remove("dominant_id")
            if not fields:
                raise ValueError("can't get dominant_id separately, please use futures.get_dominant")
            if futures888:
                has_dominant_id = True
            else:
                warnings.warn(
                    "only if one of the order_book_id is future and contains 88/888/99/889 can the dominant_id be selected in fields")
        check_items_in_container(fields, all_fields, "fields")
        return fields, has_dominant_id
    else:
        return list(all_fields), has_dominant_id


def classify_order_book_ids(order_book_ids, market):
    ins_list = ensure_instruments(order_book_ids, market=market)
    _order_book_ids = []
    stocks = []
    funds = []
    indexes = []
    futures = []
    futures_888 = {}
    spots = []
    options = []
    convertibles = []
    repos = []
    for ins in ins_list:
        if ins.order_book_id not in _order_book_ids:
            _order_book_ids.append(getattr(ins, "unique_id", ins.order_book_id))
            if ins.type == "CS":
                stocks.append(getattr(ins, "unique_id", ins.order_book_id))
            elif ins.type == "INDX":
                indexes.append(ins.order_book_id)
            elif ins.type in {"ETF", "LOF", "SF", "FUND", "REITs"}:
                funds.append(ins.order_book_id)
            elif ins.type == "Future":
                if ins.order_book_id.endswith(("88", "889")):
                    futures_888[ins.order_book_id] = ins.underlying_symbol
                futures.append(ins)
            elif ins.type == "Spot":
                spots.append(ins.order_book_id)
            elif ins.type == "Option":
                options.append(ins.order_book_id)
            elif ins.type == "Convertible":
                convertibles.append(ins.order_book_id)
            elif ins.type == "Repo":
                repos.append(ins.order_book_id)
    return _order_book_ids, stocks, funds, indexes, futures, futures_888, spots, options, convertibles, repos


def _weekly_start_end_date_handler(start_date, end_date):
    start_date = to_date(start_date)
    monday = start_date - datetime.timedelta(days=start_date.weekday())
    first_trading_day_in_week = monday if is_trading_date(monday) else get_next_trading_date(monday)
    if first_trading_day_in_week < start_date:
        start_date = monday + datetime.timedelta(weeks=1)

    end_date = to_date(end_date)
    if end_date > datetime.date.today():
        end_date = datetime.date.today()
    friday = end_date - datetime.timedelta(days=end_date.weekday()) + datetime.timedelta(days=4)
    last_trading_day_in_week = friday if is_trading_date(friday) else get_previous_trading_date(friday)
    if last_trading_day_in_week > end_date:
        end_date = friday - datetime.timedelta(weeks=1)

    return to_date_int(start_date), to_date_int(end_date)


@ttl_cache(15 * 60)
def daybar_for_tick_price(order_book_id, market="cn"):
    ins = instruments(order_book_id, market=market)
    today = to_date_int(datetime.datetime.today())

    if ins.type in ("Future", "Spot", "Option"):
        fields = ["prev_settlement", "open", "prev_close", "limit_up", "limit_down"]
    elif ins.type in ("LOF", "ETF", "REITs", "FUND"):
        fields = ["open", "prev_close", "limit_up", "limit_down", "iopv"]
    elif ins.type in ("CS", "Convertible"):
        fields = ["open", "prev_close", "limit_up", "limit_down"]
    else:
        fields = ["open", "prev_close"]

    df = get_price(
        ins.order_book_id,
        "2004-12-31",
        today,
        frequency="1d",
        fields=fields,
        adjust_type="none",
        skip_suspended=False,
        expect_df=is_panel_removed,
        market=market,
    )

    if df is not None and isinstance(df.index, pd.MultiIndex):
        df.reset_index(level=0, inplace=True)
    return df


EQUITIES_TICK_FIELDS = [
    "trading_date", "open", "last", "high", "low",
    "prev_close", "volume", "total_turnover", "limit_up", "limit_down",
    "a1", "a2", "a3", "a4", "a5", "b1", "b2", "b3", "b4", "b5", "a1_v", "a2_v", "a3_v",
    "a4_v", "a5_v", "b1_v", "b2_v", "b3_v", "b4_v", "b5_v", "change_rate",
    "num_trades",
]
HK_EQUITIES_TICK_FIELDS = EQUITIES_TICK_FIELDS + [
    "a6", "a7", "a8", "a9", "a10",
    "a6_v", "a7_v", "a8_v", "a9_v", "a10_v",
    "b6", "b7", "b8", "b9", "b10",
    "b6_v", "b7_v", "b8_v", "b9_v", "b10_v",
]
FUND_TICK_FIELDS = EQUITIES_TICK_FIELDS + ["iopv", "prev_iopv"]
FUTURE_TICK_FIELDS = EQUITIES_TICK_FIELDS + ["open_interest", "prev_settlement"]
EQUITIES_TICK_COLUMNS = EQUITIES_TICK_FIELDS
HK_EQUITIES_TICK_COLUMNS = HK_EQUITIES_TICK_FIELDS
FUTURE_TICK_COLUMNS = [
    "trading_date", "open", "last", "high", "low", "prev_settlement",
    "prev_close", "volume", "open_interest", "total_turnover", "limit_up", "limit_down",
    "a1", "a2", "a3", "a4", "a5", "b1", "b2", "b3", "b4", "b5", "a1_v", "a2_v", "a3_v",
    "a4_v", "a5_v", "b1_v", "b2_v", "b3_v", "b4_v", "b5_v", "change_rate",
]
FUND_TICK_COLUMNS = FUND_TICK_FIELDS
RELATED_DABAR_FIELDS = {"open", "prev_settlement", "prev_close", "limit_up", "limit_down", "change_rate"}


def get_tick_price(order_book_ids, start_date, end_date, fields, expect_df, market):
    df = get_tick_price_multi_df(order_book_ids, start_date, end_date, fields, market)
    if df is not None and not expect_df and isinstance(order_book_ids, string_types):
        df.reset_index(level=0, drop=True, inplace=True)
    return df


def convert_history_tick_to_multi_df(data, dt_name, fields, convert_dt, market="cn"):
    line_no = 0
    dt_set = set()
    obid_level = []
    obid_slice_map = {}
    for i, (obid, d) in enumerate(data):
        dates = d.pop("date")
        if len(dates) == 0:
            continue
        times = d.pop("time")
        dts = d[dt_name] = dates.astype(np.int64) * 1000000000 + times.astype(np.int64)

        dts_len = len(dts)

        if not obid_level or obid_level[-1] != obid:
            obid_level.append(obid)
        obid_slice_map[(i, obid)] = slice(line_no, line_no + dts_len, None)

        dt_set.update(dts)
        line_no += dts_len

    if line_no == 0:
        return

    daybars = {}
    if set(fields) & RELATED_DABAR_FIELDS:
        ins_list = ensure_instruments(obid_level, market=market)
        for ins in ins_list:
            ins_id = getattr(ins, "unique_id", ins.order_book_id)
            daybar = daybar_for_tick_price(ins_id, market=market)
            if daybar is not None:
                if ins.type in ["ETF", "LOF", "REITs", "FUND"]:
                    daybar['prev_iopv'] = daybar['iopv'].shift(1)
            daybars[ins_id] = daybar
        fields_ = list(set(fields) | {"last", "volume"})
    else:
        fields_ = fields

    obid_idx_map = {o: i for i, o in enumerate(obid_level)}
    obid_label = np.empty(line_no, dtype=object)
    dt_label = np.empty(line_no, dtype=object)
    arr = np.full((line_no, len(fields_)), np.nan)
    r_map_fields = {f: i for i, f in enumerate(fields_)}

    dt_arr_sorted = np.array(sorted(dt_set))
    dt_level = convert_dt(dt_arr_sorted)

    for i, (obid, d) in enumerate(data):
        if dt_name not in d:
            continue
        dts = d[dt_name]
        slice_ = obid_slice_map[(i, obid)]
        for f, value in d.items():
            if f == dt_name:
                dt_label[slice_] = dt_arr_sorted.searchsorted(dts, side='left')
            else:
                arr[slice_, r_map_fields[f]] = value

        obid_label[slice_] = obid_idx_map[obid]

        trading_date = to_datetime(_to_trading_date(int17_to_datetime(dts[-1])))
        if "trading_date" in r_map_fields:
            trading_date_int = date_to_int8(trading_date)
            arr[slice_, r_map_fields["trading_date"]] = trading_date_int

        daybar = daybars.get(obid)
        if daybar is not None:
            try:
                last = daybar.loc[trading_date]
            except KeyError:
                continue
            day_open = last["open"]
            if "open" in r_map_fields:
                arr[slice_, r_map_fields["open"]] = [day_open if v > 0 else 0.0 for v in d["volume"]]
            if "prev_close" in r_map_fields:
                _prev_close = arr[slice_, r_map_fields["prev_close"]][0]
                if _prev_close != _prev_close:
                    arr[slice_, r_map_fields["prev_close"]] = last["prev_close"]
            if instruments(obid, market=market).type in ("ETF", "LOF", "REITs", "FUND"):
                if "prev_iopv" in r_map_fields:
                    arr[slice_, r_map_fields["prev_iopv"]] = last["prev_iopv"]

            if instruments(obid, market=market).type in ("CS", "ETF", "LOF", "REITs", "FUND", "Future", "Spot", "Option", "Convertible"):
                if "limit_up" in r_map_fields:
                    arr[slice_, r_map_fields["limit_up"]] = last["limit_up"]
                if "limit_down" in r_map_fields:
                    arr[slice_, r_map_fields["limit_down"]] = last["limit_down"]

            if instruments(obid, market=market).type in ("Future", "Option", "Spot"):
                if "prev_settlement" in r_map_fields:
                    arr[slice_, r_map_fields["prev_settlement"]] = last["prev_settlement"]
                if "change_rate" in r_map_fields:
                    arr[slice_, r_map_fields["change_rate"]] = arr[slice_, r_map_fields["last"]] / last[
                        "prev_settlement"] - 1
            elif "change_rate" in r_map_fields:
                arr[slice_, r_map_fields["change_rate"]] = arr[slice_, r_map_fields["last"]] / last["prev_close"] - 1

    try:
        func_is_singletz = getattr(pd._libs.lib, 'is_datetime_with_singletz_array')
        setattr(pd._libs.lib, 'is_datetime_with_singletz_array', lambda *args: True)
    except AttributeError:
        func_is_singletz = None
    multi_idx = pd.MultiIndex(
        [obid_level, dt_level],
        [obid_label, dt_label],
        names=('order_book_id', dt_name)
    )
    df = pd.DataFrame(data=arr, index=multi_idx, columns=fields_)
    if "trading_date" in r_map_fields:
        df["trading_date"] = df["trading_date"].astype(int).apply(int8_to_datetime)
    if func_is_singletz is not None:
        setattr(pd._libs.lib, 'is_datetime_with_singletz_array', func_is_singletz)
    return df[fields]


def get_history_tick(order_book_ids, start_date, end_date, gtw_fields, columns, market):
    data = get_client().execute("get_tickbar", order_book_ids, start_date, end_date, gtw_fields, market=market)
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    history_df = convert_history_tick_to_multi_df(data, "datetime", columns, int17_to_datetime_v, market=market)

    if isinstance(history_df, pd.DataFrame) and 'iopv' in history_df.columns:
        history_df['iopv'] = history_df['iopv'].replace(0.0, np.nan)
    return history_df


def get_tick_price_multi_df(order_book_ids, start_date, end_date, fields, market):
    start_date, end_date = ensure_date_range(start_date, end_date, datetime.timedelta(days=3))

    live_date = current_trading_date()
    if start_date > live_date:
        return

    ins_list = ensure_instruments(order_book_ids, market=market)
    order_book_ids = [
        getattr(ins, "unique_id", ins.order_book_id)
        for ins in ins_list
    ]
    types = {ins.type for ins in ins_list}

    if "Future" in types or "Option" in types or "Spot" in types:
        base_fields = FUTURE_TICK_FIELDS
        base_columns = FUTURE_TICK_COLUMNS

        if ('Option' in types and any((i.type == 'Option' and i.exchange in ('XSHG', 'XSHE')) for i in ins_list)
        ) or ('CS' in types) or ('LOF' in types) or ('ETF' in types) or ('REITs' in types) or ('FUND' in types) or ('Convertible' in types):
            base_columns = base_columns + ['num_trades']
    elif 'ETF' in types or 'LOF' in types or 'REITs' in types or 'FUND' in types:
        base_fields = FUND_TICK_FIELDS
        base_columns = FUND_TICK_COLUMNS
    else:
        # Equities, fields are different according to market
        if market == "hk":
            base_fields = HK_EQUITIES_TICK_FIELDS
            base_columns = HK_EQUITIES_TICK_COLUMNS
        else:
            base_fields = EQUITIES_TICK_FIELDS
            base_columns = EQUITIES_TICK_COLUMNS

    if fields:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, base_fields, "fields")
        columns = [f for f in base_columns if f in fields]
    else:
        fields = base_fields
        columns = base_columns

    gtw_fields = set(fields) | {"date", "time"}
    if set(fields) & RELATED_DABAR_FIELDS:
        gtw_fields.update({"volume", "last"})

    try:
        history_df = get_history_tick(order_book_ids, start_date, end_date, list(gtw_fields), columns, market)
    except (PermissionDenied, MarketNotSupportError, NoSuchService) as e:
        warnings.warn("get history tick failed: " + str(e))
        history_df = None
    if end_date < live_date:
        return history_df

    live_date_str = '%d-%02d-%02d' % (live_date // 10000, live_date % 10000 // 100, live_date % 100)
    live_dfs = []
    for ins in ins_list:
        if ins.de_listed_date != '0000-00-00' and ins.de_listed_date < live_date_str:
            continue
        try:
            if history_df is not None and date_to_int8(_to_trading_date(
                    history_df.loc[ins.order_book_id].index.max())) == live_date:
                continue
        except KeyError:
            pass

        try:
            live_df = get_ticks(ins.order_book_id, start_date=live_date, end_date=live_date, expect_df=True,
                                market=market)
            if live_df is None:
                continue
            if "trading_date" not in live_df.columns:
                live_df["trading_date"] = int8_to_datetime(live_date)
            else:
                live_df["trading_date"] = live_df["trading_date"].apply(to_datetime)
            if ins.type in ("Future", "Option", "Spot"):
                live_df["change_rate"] = live_df["last"] / live_df["prev_settlement"] - 1
            else:
                live_df["change_rate"] = live_df["last"] / live_df["prev_close"] - 1
            live_df = live_df.reindex(columns=columns)
            live_dfs.append(live_df)
        except (PermissionDenied, MarketNotSupportError, NoSuchService):
            pass

    if not live_dfs:
        return history_df

    if history_df is None:
        return pd.concat(live_dfs)
    return pd.concat([history_df] + live_dfs)


def _to_trading_date(dt):
    if 7 <= dt.hour < 18:
        return datetime.datetime(year=dt.year, month=dt.month, day=dt.day)
    return get_next_trading_date(dt - datetime.timedelta(hours=4))


@ttl_cache(12 * 3600)
def _get_hk_part_list():
    """ 获取港股联交所参与者名单 """
    data = get_client().execute("get_hk_part_list")
    hk_part_df = pd.DataFrame(data)
    return hk_part_df


@export_as_api()
def get_stock_connect_holding_details(order_book_ids, start_date=None, end_date=None):
    """
    获取北向资金席位持股明细数据
    :param order_book_ids: 标的合约
    :param start_date: 起始日期
    :param end_date: 结束日期
    :return: pd.DataFrame
    """
    order_book_ids = ensure_order_book_ids(order_book_ids, type="CS", market="cn")
    start_date, end_date = ensure_date_range(start_date, end_date, datetime.timedelta(days=3))

    # 北向机构持股明细
    data = get_client().execute("get_stock_connect_holding_details", order_book_ids, start_date, end_date, market="cn")
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    df_list = []
    for obid, d in data:
        df = pd.DataFrame(d)
        df["order_book_id"] = obid
        df_list.append(df)
    if len(df_list) == 0:
        return
    df = pd.concat(df_list, ignore_index=True)
    df.rename(columns={"datetime": "date"}, inplace=True)
    df["participant_number"] = df["participant_number"].map(lambda s: s.decode())
    df["date"] = pd.to_datetime(df["date"].astype("str"), format="%Y%m%d")

    # 获取港股联交所参与者名单来合并获取机构名称
    hk_part_df = _get_hk_part_list()
    df = pd.merge(df, hk_part_df, how="left", on="participant_number")

    df.set_index(keys=["order_book_id", "date"], inplace=True)
    cols = ["participant_number", "ccass_name", "shares_holding", "holding_ratio"]
    df = df[cols]

    return df


@export_as_api
def get_vwap(order_book_ids, start_date=None, end_date=None, frequency="1d"):
    """
    获取日度成交量加权平均价格

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，可传入 order_book_id, order_book_id list
    start_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        开始日期
    end_date : int | str | datetime.date | datetime.datetime | pandas.Timestamp, optional
        结束日期
    frequency : str, optional
        历史数据的频率。 默认为'1d'。
        1m - 分钟级别
        1d - 日级别
        分钟可选取不同频率，例如'5m'代表 5 分线

    Returns
    -------
    Series
        vwap

    Examples
    --------
    获取 000001.XSHE 在 2024-01-01~02-01 的日级别 vwap 数据

    >>> rqdatac.get_vwap('000001.XSHE',20240101,20240201)
    order_book_id  date
    000001.XSHE    2024-01-02    9.286718
                   2024-01-03    9.182990
                   2024-01-04    9.112191
                   2024-01-05    9.302265
                   2024-01-08    9.178084
                   2024-01-09    9.140973
                   2024-01-10    9.128256
                   2024-01-11    9.135580
                   2024-01-12    9.203141
                   2024-01-15    9.205756
                   2024-01-16    9.277993
                   2024-01-17    9.314772
                   2024-01-18    9.103892
                   2024-01-19    9.148154
                   2024-01-22    9.174462
                   2024-01-23    9.078450
                   2024-01-24    9.209698
                   2024-01-25    9.422591
                   2024-01-26    9.562171
                   2024-01-29    9.730338
                   2024-01-30    9.594930
                   2024-01-31    9.481586
                   2024-02-01    9.414571
    Name: 000001.XSHE, dtype: float64

    获取 IF2403 在 2024-02-01 的 1m 级别 vwap 数据

    >>> rqdatac.get_vwap('IF2403',20240201,20240201,'1m')
    order_book_id  datetime
    IF2403         2024-02-01 09:31:00    3201.224586
                   2024-02-01 09:32:00    3196.929688
                   2024-02-01 09:33:00    3200.511346
                   2024-02-01 09:34:00    3201.722967
                   2024-02-01 09:35:00    3203.796373
                                             ...
                   2024-02-01 14:56:00    3210.391429
                   2024-02-01 14:57:00    3209.169421
                   2024-02-01 14:58:00    3208.998773
                   2024-02-01 14:59:00    3208.252893
                   2024-02-01 15:00:00    3207.797403
    Name: IF2403, Length: 240, dtype: float64

    """
    if frequency == "tick":
        raise ValueError("doesn't support get vwap by tick frequency")

    order_book_ids = ensure_order_book_ids(order_book_ids, {"CS", "Future", "Option", "ETF", "Convertible"})
    price = get_price(
        order_book_ids,
        start_date,
        end_date,
        fields=["total_turnover", "volume"],
        frequency=frequency,
        adjust_type="none"
    )
    if price is None:
        return None
    vwap = price["total_turnover"] / price["volume"]
    vwap.fillna(0, inplace=True)
    vwap.name = 'vwap'

    # 针对一些合约还需要除以 contract_multiplier
    order_book_ids = list(set(vwap.index.levels[0]))
    insts = instruments(order_book_ids)
    multiplier = {i.order_book_id: getattr(i, "contract_multiplier", 1) for i in insts}
    return vwap.groupby("order_book_id", group_keys=False).apply(
        lambda one_vwap: one_vwap / multiplier[one_vwap.name]
    )
