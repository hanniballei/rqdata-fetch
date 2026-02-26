# -*- coding: utf-8 -*-
import warnings
import datetime
import numpy as np
import pandas as pd

from rqdatac.services.detail.resample_helper import resample_week_df
from rqdatac.services.get_price import _ensure_fields

from rqdatac.services.calendar import is_trading_date, current_trading_date, get_next_trading_date
from rqdatac.utils import (
    int14_to_datetime_v,
    int8_to_datetime_v,
    date_to_int8,
    convert_bar_to_multi_df,
)
from rqdatac.client import get_client
from rqdatac.services.future import get_dominant, current_real_contract
from rqdatac.services.basic import instruments
from rqdatac.services.stock_status import is_suspended
from rqdatac.share.errors import PermissionDenied, MarketNotSupportError, NoSuchService

DAYBAR_FIELDS = {
    "future": ["settlement", "prev_settlement", "open_interest", "limit_up", "limit_down",
               "day_session_open"],
    "common": ["open", "close", "high", "low", "total_turnover", "volume", "prev_close"],
    "stock": ["limit_up", "limit_down", "num_trades"],
    "fund": ["limit_up", "limit_down", "num_trades", "iopv"],
    "spot": ["settlement", "prev_settlement", "open_interest", "limit_up", "limit_down"],
    "option": ["open_interest", "strike_price", "contract_multiplier", "prev_settlement", "settlement", "limit_up",
               "limit_down", "day_session_open"],
    "convertible": ["limit_up", "limit_down", "num_trades"],
    "index": [],
    "repo": ["num_trades"],
}

WEEKBAR_FIELDS = {
    "future": ["settlement", "prev_settlement", "open_interest", "day_session_open"],
    "common": ["open", "close", "high", "low", "total_turnover", "volume"],
    "stock": ["num_trades"],
    "fund": ["num_trades", "iopv"],
    "spot": ["settlement", "prev_settlement", "open_interest"],
    "option": ["open_interest", "strike_price", "contract_multiplier", "settlement", "day_session_open"],
    "convertible": ["num_trades"],
    "index": [],
    "repo": ["num_trades"],
}

MINBAR_FIELDS = {
    "future": ["trading_date", "open_interest"],
    "common": ["open", "close", "high", "low", "total_turnover", "volume"],
    "stock": ["num_trades"],
    "fund": ["num_trades", "iopv"],
    "spot": ["trading_date", "open_interest"],
    "option": ["trading_date", "open_interest"],
    "convertible": ["num_trades"],
    "index": [],
    "repo": [],
}


ZERO_FILL_FIELDS = frozenset({"total_turnover", "open_interest", "volume"})

SPOT_DIRECTION_MAP = {0: "null", 1: "多支付空", 2: "空支付多", 3: "交收平衡"}


def _get_pride_df_day(
        order_book_ids,
        start_date,
        end_date,
        duration,
        fields,
        adjust_type,
        skip_suspended,
        stocks,
        funds,
        indexes,
        futures,
        futures888,
        spots,
        options,
        convertibles,
        repos,
        market
):
    fields, has_dominant_id = _ensure_fields(fields, DAYBAR_FIELDS, stocks, funds, futures, futures888, spots, options, convertibles, indexes, repos)
    pf, obid_slice_map = get_daybar(order_book_ids, start_date, end_date, fields, duration, market)
    if pf is not None:
        _fill_contract_multiplier_and_strike_price(pf)
        return _adjust_pf(
            pf,
            stocks,
            funds,
            convertibles,
            futures888,
            start_date,
            end_date,
            has_dominant_id,
            adjust_type,
            skip_suspended,
            obid_slice_map,
            market,
        )


def get_price_df(
        order_book_ids,
        start_date,
        end_date,
        frequency,
        duration,
        fields,
        adjust_type,
        skip_suspended,
        stocks,
        funds,
        indexes,
        futures,
        futures888,
        spots,
        options,
        convertibles,
        repos,
        market
):
    live_date = current_trading_date()
    if start_date > live_date:
        return

    if frequency == "d":
        return _get_pride_df_day(
            order_book_ids,
            start_date,
            end_date,
            duration,
            fields,
            adjust_type,
            skip_suspended,
            stocks,
            funds,
            indexes,
            futures,
            futures888,
            spots,
            options,
            convertibles,
            repos,
            market
        )

    fields, has_dominant_id = _ensure_fields(fields, MINBAR_FIELDS, stocks, funds, futures, futures888, spots, options, convertibles, indexes, repos)
    try:
        pf, obid_slice_map = get_minbar(order_book_ids, start_date, end_date, fields, duration, market)
    except (PermissionDenied, MarketNotSupportError, NoSuchService) as e:
        warnings.warn("get history minbar price failed: " + str(e))
        pf = obid_slice_map = None

    if end_date < live_date:
        return _adjust_pf(
            pf,
            stocks,
            funds,
            convertibles,
            futures888,
            start_date,
            end_date,
            has_dominant_id,
            adjust_type,
            skip_suspended,
            obid_slice_map,
            market,
        ) if pf is not None else None

    def _trading_date_of(dt):
        if 8 < dt.hour < 18:
            return date_to_int8(dt)
        return date_to_int8(get_next_trading_date(dt - datetime.timedelta(hours=4)))

    live_date_str = '%d-%02d-%02d' % (live_date // 10000, live_date % 10000 // 100, live_date % 100)
    live_obs = set(
        ins.order_book_id for ins in instruments(order_book_ids)
        if (ins.de_listed_date == '0000-00-00' or ins.de_listed_date >= live_date_str)
        and ins.listed_date <= live_date_str
    )

    if pf is not None:
        dts = pf.index.get_level_values(1)
        for ob, slice_ in obid_slice_map.items():
            if ob not in live_obs:
                continue
            if _trading_date_of(dts[slice_][-1]) == live_date:
                live_obs.remove(ob)

    if live_obs:
        try:
            today_pf, today_obid_slice_map = get_today_minbar(live_obs, fields, duration, market)
            if today_pf is not None:
                line_no, new_slice_map, available_dfs = 0, {}, []
                index = today_pf.index.get_level_values(1)
                for obid, slc in today_obid_slice_map.items():
                    if _trading_date_of(index[slc.stop - 1]) != live_date:
                        continue
                    available_dfs.append(today_pf[slc])
                    new_slice_map[obid] = slice(line_no, line_no + slc.stop - slc.start)
                    line_no += slc.stop - slc.start
                if available_dfs:
                    today_pf, today_obid_slice_map = pd.concat(available_dfs), new_slice_map
                else:
                    today_pf, today_obid_slice_map = None, None

            if pf is None:
                pf, obid_slice_map = today_pf, today_obid_slice_map
            elif today_pf is not None:
                # sort_index 后 obid 为字典序
                pf = pd.concat([pf, today_pf]).sort_index()
                line_no, obid_slice_map = 0, {}
                # np.unique 默认顺序也为字典序，因此不需要再调整
                obids, counts = np.unique(pf.index.get_level_values(0), return_counts=True)
                for obid, ct in zip(obids, counts):
                    obid_slice_map[obid] = slice(line_no, line_no + ct, None)
                    line_no += ct
        except (PermissionDenied, MarketNotSupportError, NoSuchService):
            warnings.warn("Not permit to get realtime minbar price")

    if pf is not None:
        return _adjust_pf(
            pf,
            stocks,
            funds,
            convertibles,
            futures888,
            start_date,
            end_date,
            has_dominant_id,
            adjust_type,
            skip_suspended,
            obid_slice_map,
            market,
        )


def get_daybar(order_book_ids, start_date, end_date, fields, duration, market):
    data = get_client().execute(
        "get_daybar_v", order_book_ids, start_date, end_date, fields, duration, market
    )
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    return convert_bar_to_multi_df(data, 'date', fields, int8_to_datetime_v, return_slice_map=True)


def get_future_indx_daybar(order_book_ids, start_date, end_date, fields, duration=1, market="cn"):
    data = get_client().execute(
        "futures.get_future_indx_daybar_v", order_book_ids, start_date, end_date, fields, duration, market
    )
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    multi_df, _ = convert_bar_to_multi_df(data, 'date', fields, int8_to_datetime_v, return_slice_map=True)
    if multi_df is not None:
        return multi_df


def get_minbar(order_book_ids, start_date, end_date, fields, duration, market):
    data = get_client().execute(
        "get_minbar_v", order_book_ids, start_date, end_date, fields, duration, market
    )
    data = [(obid, {k: np.frombuffer(*v) for k, v in d.items()}) for obid, d in data]
    return convert_bar_to_multi_df(data, "datetime", fields, int14_to_datetime_v, return_slice_map=True)


def get_today_minbar(order_book_ids, fields, duration, market="cn"):
    futures_88 = [i for i in instruments(order_book_ids) if i.order_book_id.endswith('88') and i.type == 'Future']
    real_contracts = {
        i.order_book_id: current_real_contract(i.underlying_symbol, market)
        for i in futures_88
    }
    real_contracts = {k: v for k, v in real_contracts.items() if v is not None}
    order_book_ids = set(order_book_ids)
    obs = list(order_book_ids.union(set(real_contracts.values())))
    data = get_client().execute("get_today_minbar", obs, fields, duration, market)
    data = dict(data)
    data.update((ob, data.get(c)) for ob, c in real_contracts.items())
    data = [(k, v) for k, v in data.items() if k in order_book_ids]
    return convert_bar_to_multi_df(data, "datetime", fields, int14_to_datetime_v, return_slice_map=True)


def _fill_contract_multiplier_and_strike_price(pf):
    """ 补齐非ETF期权的contract_multiplier, strike_price """
    valid_fields = set(pf.columns) & {'contract_multiplier', 'strike_price'}
    if not valid_fields:
        return
    index = pf.index
    for ob in index.levels[0]:
        ins = instruments(ob)
        if ins.type != 'Option' or ins.exchange in ('XSHG', 'XSHE'):
            continue
        loc = index.get_loc(ob)
        for f in valid_fields:
            pf[f].values[loc] = getattr(ins, f)


def _adjust_pf(
        pf,
        stocks,
        funds,
        convertibles,
        futures888,
        start_date,
        end_date,
        has_dominant_id,
        adjust_type,
        skip_suspended,
        obid_slice_map,
        market,
):
    adjust = (stocks or funds) and adjust_type in {"pre", "post", "pre_volume", "post_volume"}
    if adjust:
        from rqdatac.services.detail.adjust_price import adjust_price_multi_df
        adjust_price_multi_df(pf, stocks + funds, adjust_type, obid_slice_map, market)
    if has_dominant_id:
        # 1.全为非正常合约 2.有期货类型合约并且指定dominant_id字段
        # 只有满足其中一种才在返回字段中增加dominant_id
        add_dominant_id(pf, futures888, obid_slice_map)
    if skip_suspended and (stocks or convertibles):
        pf = filter_suspended(pf, start_date, end_date, convertibles, stocks, market=market)

    if "trading_date" in pf:
        td = pf.trading_date.values
        dtidx = pf.index.get_level_values(1).map(date_to_int8)
        new_td = np.where(~(td != td), td, dtidx.values).astype(int)
        pf.trading_date = int8_to_datetime_v(new_td)

    return pf


def add_dominant_id(result, futures888, obid_slice_map):
    from rqdatac.services.calendar import get_next_trading_date
    def _may_shift_date(d):
        # 夜盘的开始时间是晚上9点, 这时候对应的 dominant_id 应该是下一个交易日的 dominant_id
        if d.hour >= 21 or not is_trading_date(d):
            return pd.Timestamp(get_next_trading_date(d))
        return d

    for order_book_id, underlying in futures888.items():
        if order_book_id in obid_slice_map:
            slice_ = obid_slice_map[order_book_id]
            dts = result.index.get_level_values(1)[slice_].map(_may_shift_date)
            dominants = get_dominant(
                underlying, dts[0].date(), dts[-1].date())
            if dominants is not None:
                result.loc[result.index[slice_], "dominant_id"] = np.take(
                    dominants.values, dominants.index.searchsorted(dts, side="right") - 1)


def filter_suspended(ret, start_date, end_date, convertibles, stocks, market):
    from rqdatac.services.convertible import is_suspended as is_convertible_suspend
    s = pd.concat([is_suspended(stocks, start_date, end_date, market) if stocks else pd.Series(),
                   is_convertible_suspend(convertibles, start_date, end_date) if convertibles else pd.Series()], axis=1)
    s = s.unstack()
    s.dropna(inplace=True)
    s.index.names = ['order_book_id', 'date']
    s = s[s]
    index = pd.MultiIndex.from_arrays(
        [ret.index.get_level_values(0), pd.to_datetime(ret.index.get_level_values(1).date)],
        names=('order_book_id', 'date'))
    return ret[~index.isin(s.index)]


def get_week_df(order_book_ids, start_date, end_date, fields, adjust_type, market, stocks, funds, indexes, futures,
                futures888, spots, options, convertibles, repos):
    fields, has_dominant_id = _ensure_fields(fields, WEEKBAR_FIELDS, stocks, funds, futures, futures888, spots,
                                             options, convertibles, indexes, repos)
    has_volume_field = 'volume' in fields
    if not has_volume_field:
        fields.append('volume')
    df = get_price_df(
        order_book_ids, start_date, end_date, 'd', 1, fields, adjust_type, False,
        stocks, funds, indexes, futures, futures888, spots, options, convertibles, repos, market
    )
    if df is None:
        return
    res = resample_week_df(df, fields)
    if not has_volume_field:
        res.drop(columns=['volume'], inplace=True)
    return res
