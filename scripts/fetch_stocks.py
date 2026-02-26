#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 RQData 增量拉取A股日频行情及财务数据。

量价数据：$RQDATA_STORE_PATH/stock_data/daily/{YYYY}/{MM}/{YYYY-MM-DD}.csv
财务数据：$RQDATA_STORE_PATH/stock_data/financials/{quarter}.csv

环境变量：RQDATA_PRIMARY_URI, RQDATA_BACKUP_USERNAME/PASSWORD/HOST/PORT, RQDATA_STORE_PATH

用法：
  python3 fetch_stocks.py                                    # 最近一个交易日
  python3 fetch_stocks.py --date 2024-01-15                  # 指定日期
  python3 fetch_stocks.py --backfill-days 5                  # 回补最近5个交易日
  python3 fetch_stocks.py --all-cs                           # 包含B股
  python3 fetch_stocks.py --partition year                   # 按年分区
  python3 fetch_stocks.py --no-filter                        # 关闭默认过滤
  python3 fetch_stocks.py --no-emit-meta                     # 不输出元数据列
  python3 fetch_stocks.py --fetch-financials --quarter 2024q3  # 拉取财务数据
  python3 fetch_stocks.py --fetch-financials --start-quarter 2023q1 --end-quarter 2024q4
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd

# 支持直接运行：python3 scripts/fetch_stocks.py（将 scripts/ 加入 sys.path）
sys.path.insert(0, str(Path(__file__).parent))
from common import init_rqdatac, get_store_path  # noqa: E402


# ===================== 时间工具 =====================

def _to_cn_date() -> _dt.date:
    from zoneinfo import ZoneInfo
    return _dt.datetime.now(ZoneInfo("Asia/Shanghai")).date()


def get_last_trading_day(rqdatac) -> _dt.date:
    today = _to_cn_date()
    start = (today - _dt.timedelta(days=60)).isoformat()
    days = rqdatac.get_trading_dates(start_date=start, end_date=today.isoformat(), market="cn")
    if not days:
        raise RuntimeError("未获取到近60日内的交易日")
    last = days[-1]
    if isinstance(last, _dt.datetime):
        return last.date()
    if isinstance(last, _dt.date):
        return last
    return _dt.date.fromisoformat(str(last))


def get_trading_days(rqdatac, start: str, end: str) -> List[str]:
    days = rqdatac.get_trading_dates(start_date=start, end_date=end, market="cn")
    out = []
    for d in days:
        if isinstance(d, _dt.datetime):
            out.append(d.date().isoformat())
        elif isinstance(d, _dt.date):
            out.append(d.isoformat())
        else:
            out.append(_dt.date.fromisoformat(str(d)).isoformat())
    return out


# ===================== 股票池 =====================

def get_universe(rqdatac, as_of: str, a_only: bool = True) -> pd.DataFrame:
    """获取截至 as_of 仍在市的股票列表。"""
    ins = rqdatac.all_instruments(type="CS")
    as_of_date = pd.Timestamp(as_of)

    listed = pd.to_datetime(ins["listed_date"], errors="coerce") <= as_of_date
    de = pd.to_datetime(ins["de_listed_date"], errors="coerce")
    alive = de.isna() | (de >= as_of_date)
    df = ins.loc[listed & alive].copy()

    if a_only:
        if "traded_currency" in df.columns:
            df = df.loc[df["traded_currency"].astype(str).str.upper() == "CNY"]
        else:
            ob = df["order_book_id"].astype(str)
            df = df.loc[~(ob.str.startswith("900") | ob.str.startswith("200"))]

    cols = [c for c in ["order_book_id", "symbol", "listed_date", "de_listed_date"] if c in df.columns]
    return df[cols].copy()


# ===================== 批量 API 调用 =====================

def _batch_get_price(rqdatac, ids: List[str], start: str, end: str) -> Optional[pd.DataFrame]:
    """批量拉取 OHLCV（单次 API 调用覆盖所有标的）。"""
    try:
        df = rqdatac.get_price(
            order_book_ids=ids, start_date=start, end_date=end,
            frequency="1d", adjust_type="none", expect_df=True,
        )
        return df if df is not None and not df.empty else None
    except Exception as e:
        print(f"[WARN] batch get_price({start}~{end}) 失败: {e}")
        return None


def _batch_is_st(rqdatac, ids: List[str], start: str, end: str) -> Optional[pd.DataFrame]:
    """批量拉取 ST 状态。"""
    try:
        df = rqdatac.is_st_stock(order_book_ids=ids, start_date=start, end_date=end)
        if df is None:
            return None
        return df if not (isinstance(df, pd.DataFrame) and df.empty) else None
    except Exception as e:
        print(f"[WARN] batch is_st_stock 失败: {e}")
        return None


def _batch_is_suspended(rqdatac, ids: List[str], start: str, end: str) -> Optional[pd.DataFrame]:
    """批量拉取停牌状态。"""
    try:
        df = rqdatac.is_suspended(order_book_ids=ids, start_date=start, end_date=end)
        if df is None:
            return None
        return df if not (isinstance(df, pd.DataFrame) and df.empty) else None
    except Exception as e:
        print(f"[WARN] batch is_suspended 失败: {e}")
        return None


def _batch_market_cap(rqdatac, ids: List[str], start: str, end: str) -> Optional[pd.DataFrame]:
    """批量拉取市值因子（市值 = 收盘价 × 总股本）。"""
    for factor_name in ["market_cap", "total_market_cap"]:
        try:
            df = rqdatac.get_factor(
                order_book_ids=ids, factor=factor_name,
                start_date=start, end_date=end,
            )
            if df is not None and not (isinstance(df, pd.DataFrame) and df.empty):
                return df
        except Exception:
            continue
    return None


# ===================== 批量结果提取 =====================

def _extract_ohlcv(batch: Optional[pd.DataFrame], oid: str) -> pd.DataFrame:
    """从批量 get_price 结果中提取单股 OHLCV，index 为 DatetimeIndex。"""
    if batch is None:
        return pd.DataFrame()
    try:
        sub = pd.DataFrame()
        if isinstance(batch.index, pd.MultiIndex):
            for lvl in range(batch.index.nlevels):
                if oid in batch.index.get_level_values(lvl):
                    sub = batch.xs(oid, level=lvl).copy()
                    break
        if sub.empty:
            return pd.DataFrame()
        sub.index = pd.to_datetime(sub.index, errors="coerce")
        if "total_turnover" in sub.columns and "money" not in sub.columns:
            sub = sub.rename(columns={"total_turnover": "money"})
        required = ["open", "high", "low", "close", "volume"]
        if not all(c in sub.columns for c in required):
            return pd.DataFrame()
        sub = sub.dropna(subset=required)
        return sub
    except Exception:
        return pd.DataFrame()


def _extract_bool(
    batch: Optional[pd.DataFrame], oid: str,
    ref_index: pd.Index, default: bool = False,
) -> pd.Series:
    """从批量 is_st_stock/is_suspended 结果中提取单股布尔 Series。"""
    fallback = pd.Series(default, index=ref_index, dtype=bool)
    if batch is None:
        return fallback
    try:
        if isinstance(batch, pd.DataFrame):
            if oid in batch.columns:
                s = batch[oid].copy()
                s.index = pd.to_datetime(s.index, errors="coerce")
                return s.reindex(ref_index).fillna(default).astype(bool)
            if isinstance(batch.index, pd.MultiIndex):
                for lvl in range(batch.index.nlevels):
                    if oid in batch.index.get_level_values(lvl):
                        sub = batch.xs(oid, level=lvl)
                        if isinstance(sub, pd.DataFrame):
                            if sub.empty or sub.shape[1] == 0:
                                break
                            s = sub.iloc[:, 0]
                        else:
                            s = sub
                        s.index = pd.to_datetime(s.index, errors="coerce")
                        return s.reindex(ref_index).fillna(default).astype(bool)
    except Exception:
        pass
    return fallback


def _extract_numeric(
    batch: Optional[pd.DataFrame], oid: str, ref_index: pd.Index,
) -> Optional[pd.Series]:
    """从批量 get_factor 结果中提取单股数值 Series。"""
    if batch is None:
        return None
    try:
        if isinstance(batch, pd.DataFrame):
            if oid in batch.columns:
                s = batch[oid].copy()
                s.index = pd.to_datetime(s.index, errors="coerce")
                return s.reindex(ref_index).ffill()
            if isinstance(batch.index, pd.MultiIndex):
                for lvl in range(batch.index.nlevels):
                    if oid in batch.index.get_level_values(lvl):
                        sub = batch.xs(oid, level=lvl)
                        if isinstance(sub, pd.DataFrame):
                            if sub.empty or sub.shape[1] == 0:
                                break
                            s = sub.iloc[:, 0]
                        else:
                            s = sub
                        s.index = pd.to_datetime(s.index, errors="coerce")
                        return s.reindex(ref_index).ffill()
    except Exception:
        pass
    return None


# ===================== CSV 行格式化 =====================

BASE_COLS = ["order_book_id", "open", "high", "low", "close", "volume", "money"]
META_COLS = ["symbol", "listed_date", "is_st", "is_suspended", "market_cap"]


def _format_row(
    oid: str, row: pd.Series, *,
    emit_meta: bool = False, symbol: str = "",
    listed_date=None, is_st: bool = False, is_suspended: bool = False,
    market_cap=None,
) -> str:
    """格式化单行 CSV 数据（不含换行符）。"""
    money_val = row.get("money", row.get("total_turnover", 0))
    base = (
        f"{oid},{row['open']:.6f},{row['high']:.6f},{row['low']:.6f},"
        f"{row['close']:.6f},{int(row['volume'])},{float(money_val):.2f}"
    )
    if not emit_meta:
        return base
    sym = (symbol or "").replace(",", " ").replace('"', '""')
    ld = (
        listed_date.date().isoformat() if isinstance(listed_date, pd.Timestamp)
        else (str(listed_date) if listed_date else "")
    )
    ist = "True" if is_st else "False"
    sus = "True" if is_suspended else "False"
    mc = f"{float(market_cap):.2f}" if market_cap is not None else ""
    return f"{base},{sym},{ld},{ist},{sus},{mc}"


# ===================== 日频行情拉取 =====================

def run_daily(
    rqdatac, *,
    date: Optional[str] = None,
    backfill_days: int = 1,
    a_only: bool = True,
    partition: str = "month",
    min_mktcap: float = 1e8,
    emit_meta: bool = True,
    enable_filters: bool = True,
) -> bool:
    """返回 True 表示所有目标交易日均处理成功，False 表示至少有一天失败。"""
    store = get_store_path()
    out_dir = store / "stock_data" / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)

    if date:
        target_days = [date]
    else:
        last = get_last_trading_day(rqdatac)
        start_w = (last - _dt.timedelta(days=90)).isoformat()
        all_days = get_trading_days(rqdatac, start_w, last.isoformat())
        target_days = all_days[-backfill_days:]

    print(f"[INFO] 目标交易日: {', '.join(target_days)}")

    all_ok = True
    for d in target_days:
        print(f"\n[INFO] === 处理 {d} ===")
        try:
            _process_day(
                rqdatac,
                d,
                out_dir,
                a_only,
                partition,
                min_mktcap,
                emit_meta,
                enable_filters,
            )
        except Exception as e:
            print(f"[ERROR] 处理 {d} 失败: {e}")
            all_ok = False

    print("\n[DONE] 日频行情拉取完成。")
    return all_ok


def _process_day(
    rqdatac,
    date_str,
    out_dir,
    a_only,
    partition,
    min_mktcap,
    emit_meta,
    enable_filters,
):
    """处理单个交易日：批量拉取 → 过滤 → 原子写入。"""
    uni = get_universe(rqdatac, date_str, a_only)
    ids = uni["order_book_id"].tolist()
    sym_map: Dict[str, str] = {}
    listed_map: Dict[str, Optional[pd.Timestamp]] = {}
    if "symbol" in uni.columns:
        sym_map = {str(r.order_book_id): str(r.symbol) for r in uni.itertuples()}
    if "listed_date" in uni.columns:
        listed_map = {
            str(r.order_book_id): pd.to_datetime(r.listed_date, errors="coerce")
            for r in uni.itertuples()
        }

    print(f"[INFO] 标的数: {len(ids)}")

    # ---- 批量拉取（约 5 次 API 调用，取代逐股约 25,000 次调用）----
    lookback_start = (_dt.date.fromisoformat(date_str) - _dt.timedelta(days=15)).isoformat()
    print("[INFO] 批量拉取 OHLCV ...")
    batch_ohlcv = _batch_get_price(rqdatac, ids, date_str, date_str)
    if batch_ohlcv is None:
        raise RuntimeError("批量拉取 OHLCV 失败或返回空数据")

    batch_ohlcv_ext = None
    if enable_filters:
        print("[INFO] 批量拉取扩展 OHLCV (close<1 检测) ...")
        batch_ohlcv_ext = _batch_get_price(rqdatac, ids, lookback_start, date_str)

    need_meta = emit_meta or enable_filters
    batch_st = None
    batch_sus = None
    batch_mktcap = None
    if need_meta:
        print("[INFO] 批量拉取 ST 状态 ...")
        batch_st = _batch_is_st(rqdatac, ids, date_str, date_str)
        print("[INFO] 批量拉取停牌状态 ...")
        batch_sus = _batch_is_suspended(rqdatac, ids, date_str, date_str)
        print("[INFO] 批量拉取市值 ...")
        batch_mktcap = _batch_market_cap(rqdatac, ids, date_str, date_str)

    # ---- 逐股过滤，收集输出行 ----
    file_rows: Dict[Path, List[str]] = {}
    ok_count = 0

    for oid in ids:
        try:
            ohlcv = _extract_ohlcv(batch_ohlcv, oid)
            if ohlcv.empty:
                continue

            ref_idx = ohlcv.index
            is_st = _extract_bool(batch_st, oid, ref_idx, default=False)
            sus = _extract_bool(batch_sus, oid, ref_idx, default=False)

            mktcap_s = _extract_numeric(batch_mktcap, oid, ref_idx)
            if mktcap_s is None or not mktcap_s.notna().any():
                mktcap = pd.Series(np.inf, index=ref_idx)
            else:
                mktcap = mktcap_s.ffill().fillna(np.inf)

            if enable_filters:
                # 连续3日 close < 1 过滤
                ohlcv_ext = _extract_ohlcv(batch_ohlcv_ext, oid)
                if not ohlcv_ext.empty:
                    lt1 = (ohlcv_ext["close"] < 1.0).rolling(3, min_periods=3).sum() == 3
                    lt1 = lt1.reindex(ref_idx).fillna(False)
                else:
                    lt1 = pd.Series(False, index=ref_idx)
                mask = (~is_st) & (~lt1) & (mktcap >= min_mktcap) & (~sus)
            else:
                mask = pd.Series(True, index=ref_idx, dtype=bool)

            if not mask.any():
                continue

            for dt, passed in mask.items():
                if not passed:
                    continue
                if partition == "month":
                    day_path = (
                        out_dir / f"{dt.year:04d}" / f"{dt.month:02d}"
                        / f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}.csv"
                    )
                else:
                    day_path = out_dir / f"{dt.year:04d}" / f"{dt.date().isoformat()}.csv"

                if day_path not in file_rows:
                    file_rows[day_path] = []

                mc_val = float(mktcap.loc[dt])
                file_rows[day_path].append(_format_row(
                    oid, ohlcv.loc[dt],
                    emit_meta=emit_meta,
                    symbol=sym_map.get(oid, ""),
                    listed_date=listed_map.get(oid),
                    is_st=bool(is_st.loc[dt]) if dt in is_st.index else False,
                    is_suspended=bool(sus.loc[dt]) if dt in sus.index else False,
                    market_cap=mc_val if not np.isinf(mc_val) else None,
                ))

            ok_count += 1

        except Exception as e:
            print(f"[ERROR] {oid}: {e}")

    # ---- 原子写入（write to .tmp → rename，避免写入中断留下残缺文件）----
    cols = BASE_COLS + (META_COLS if emit_meta else [])
    header = ",".join(cols) + "\n"
    for day_path, rows in file_rows.items():
        if not rows:
            continue
        day_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = day_path.with_suffix(".tmp")
        try:
            with tmp.open("w", encoding="utf-8") as f:
                f.write(header)
                for row in rows:
                    f.write(row + "\n")
            tmp.replace(day_path)
        except Exception as e:
            print(f"[ERROR] 写入 {day_path}: {e}")
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    print(f"[INFO] {date_str} 完成, 有效标的: {ok_count}/{len(ids)}")


# ===================== 财务数据拉取 =====================

DEFAULT_FINANCIAL_FIELDS = [
    # 利润表
    "revenue", "operating_revenue", "net_profit", "gross_profit",
    "profit_from_operation", "basic_earnings_per_share",
    # 资产负债表
    "total_assets", "total_liabilities", "total_equity",
    "cash_equivalent", "inventory", "total_fixed_assets",
    # 现金流量表
    "cash_flow_from_operating_activities",
    "cash_flow_from_investing_activities",
    "cash_flow_from_financing_activities",
]

DEFAULT_FACTOR_FIELDS = [
    "pe_ratio_ttm", "pb_ratio_lf", "ps_ratio_ttm",
    "market_cap", "return_on_equity_ttm",
    "net_profit_margin_ttm", "gross_profit_margin_ttm",
    "debt_to_asset_ratio_lyr", "current_ratio_lyr",
    "net_profit_growth_ratio_ttm", "operating_revenue_growth_ratio_ttm",
]


def _normalize_quarter_arg(value: str, flag_name: str) -> str:
    """规范化季度参数并校验格式，返回形如 2024q3。"""
    val = value.strip().lower()
    m = re.fullmatch(r"(\d{4})q([1-4])", val)
    if not m:
        raise ValueError(f"{flag_name} 格式非法: {value!r}，应为 YYYYq[1-4]（如 2024q3）")
    year, quarter = int(m.group(1)), int(m.group(2))
    return f"{year:04d}q{quarter}"


def _quarter_to_tuple(q: str) -> tuple:
    y, qi = int(q[:4]), int(q[-1])
    return y, qi


def _quarter_range(start_q: str, end_q: str) -> List[str]:
    """生成季度列表，如 '2023q1' -> '2024q4'。"""
    sy, sq = _quarter_to_tuple(start_q)
    ey, eq = _quarter_to_tuple(end_q)
    out = []
    y, q = sy, sq
    while (y, q) <= (ey, eq):
        out.append(f"{y}q{q}")
        q += 1
        if q > 4:
            q = 1
            y += 1
    return out


def _last_trading_day_of_quarter(rqdatac, qy: int, qq: int) -> str:
    """获取指定季度的最后一个交易日（避免季末日落在周末/节假日的问题）。"""
    quarter_starts = {1: f"{qy}-01-01", 2: f"{qy}-04-01", 3: f"{qy}-07-01", 4: f"{qy}-10-01"}
    quarter_ends = {1: f"{qy}-03-31", 2: f"{qy}-06-30", 3: f"{qy}-09-30", 4: f"{qy}-12-31"}
    try:
        days = rqdatac.get_trading_dates(
            start_date=quarter_starts[qq], end_date=quarter_ends[qq], market="cn"
        )
        if days:
            last = days[-1]
            if isinstance(last, _dt.datetime):
                return last.date().isoformat()
            if isinstance(last, _dt.date):
                return last.isoformat()
            return str(last)
    except Exception:
        pass
    return quarter_ends[qq]  # 降级：使用日历季末日


def run_financials(
    rqdatac, *,
    quarter: Optional[str] = None,
    start_quarter: Optional[str] = None,
    end_quarter: Optional[str] = None,
    a_only: bool = True,
    fields: Optional[List[str]] = None,
    factors: Optional[List[str]] = None,
) -> bool:
    """拉取财务数据。返回 True 表示所有季度均有数据写出，False 表示至少有一个季度失败/无数据。"""
    store = get_store_path()
    fin_dir = store / "stock_data" / "financials"
    fin_dir.mkdir(parents=True, exist_ok=True)

    if fields is None:
        fields = DEFAULT_FINANCIAL_FIELDS
    if factors is None:
        factors = DEFAULT_FACTOR_FIELDS

    # 确定季度范围
    if quarter:
        q = _normalize_quarter_arg(quarter, "--quarter")
        quarters = [q]
    elif start_quarter and end_quarter:
        sq = _normalize_quarter_arg(start_quarter, "--start-quarter")
        eq = _normalize_quarter_arg(end_quarter, "--end-quarter")
        if _quarter_to_tuple(sq) > _quarter_to_tuple(eq):
            raise ValueError(
                f"--start-quarter ({sq}) 不能晚于 --end-quarter ({eq})"
            )
        quarters = _quarter_range(sq, eq)
    else:
        # 默认最近4个季度
        today = _to_cn_date()
        y, m = today.year, today.month
        cur_q = (m - 1) // 3 + 1
        quarters = []
        for _ in range(4):
            quarters.append(f"{y}q{cur_q}")
            cur_q -= 1
            if cur_q < 1:
                cur_q = 4
                y -= 1
        quarters.reverse()

    print(f"[INFO] 拉取财务数据: {quarters}")

    # 获取股票池
    as_of = _to_cn_date().isoformat()
    uni = get_universe(rqdatac, as_of, a_only)
    ids = uni["order_book_id"].tolist()
    print(f"[INFO] 标的数: {len(ids)}")

    all_ok = True
    for q in quarters:
        print(f"\n[INFO] === 季度 {q} ===")
        out_path = fin_dir / f"{q}.csv"

        # 1) 财务报表数据 (get_pit_financials_ex)
        try:
            fin_df = rqdatac.get_pit_financials_ex(
                order_book_ids=ids,
                fields=fields,
                start_quarter=q,
                end_quarter=q,
            )
            if fin_df is not None and not fin_df.empty:
                fin_df = fin_df.reset_index()
                print(f"  [OK] 财务报表: {len(fin_df)} 条")
            else:
                fin_df = pd.DataFrame()
                print("  [WARN] 财务报表无数据")
        except Exception as e:
            print(f"  [ERROR] 财务报表: {e}")
            fin_df = pd.DataFrame()

        # 2) 估值因子数据 (get_factor) — 取季度最后一个交易日
        qy, qq = int(q[:4]), int(q[-1])
        qend = _last_trading_day_of_quarter(rqdatac, qy, qq)
        try:
            fac_df = rqdatac.get_factor(
                order_book_ids=ids,
                factor=factors,
                start_date=qend,
                end_date=qend,
            )
            if fac_df is not None and not fac_df.empty:
                fac_df = fac_df.reset_index()
                print(f"  [OK] 估值因子: {len(fac_df)} 条")
            else:
                fac_df = pd.DataFrame()
                print("  [WARN] 估值因子无数据")
        except Exception as e:
            print(f"  [ERROR] 估值因子: {e}")
            fac_df = pd.DataFrame()

        # 合并输出
        if not fin_df.empty:
            result = fin_df
            if not fac_df.empty:
                merge_col = "order_book_id" if "order_book_id" in fac_df.columns else None
                if merge_col and merge_col in result.columns:
                    fac_cols = [c for c in fac_df.columns if c not in result.columns or c == merge_col]
                    result = result.merge(fac_df[fac_cols], on=merge_col, how="left")
        elif not fac_df.empty:
            result = fac_df
        else:
            print(f"  [SKIP] {q} 无任何数据")
            all_ok = False
            continue

        result.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  [SAVE] {out_path} ({len(result)} 行)")

    print("\n[DONE] 财务数据拉取完成。")
    return all_ok


# ===================== Per-symbol 频率模式 =====================

STOCK_FREQ_CONFIG = {
    "1m":  {"dir": "1min_stocks",   "suffix": "1m",  "duration_min": 1},
    "5m":  {"dir": "5min_stocks",   "suffix": "5m",  "duration_min": 5},
    "15m": {"dir": "15min_stocks",  "suffix": "15m", "duration_min": 15},
    "30m": {"dir": "30min_stocks",  "suffix": "30m", "duration_min": 30},
    "60m": {"dir": "hourly_stocks", "suffix": "1h",  "duration_min": 60},
    "1d":  {"dir": "daily_stocks",  "suffix": "1d",  "duration_min": None},
    "1w":  {"dir": "weekly_stocks", "suffix": "1w",  "duration_min": None},
}


def _normalize_stock_oid(s: str) -> str:
    """补全裸股票代码的交易所后缀。"""
    s = s.strip()
    if "." in s:
        return s  # 已含后缀，原样使用
    if s.startswith("6") or s.startswith("688"):
        return f"{s}.XSHG"
    if s.startswith("0") or s.startswith("3"):
        return f"{s}.XSHE"
    return s


def get_all_stock_symbols(rqdatac) -> List[str]:
    """获取当前所有在市A股 order_book_id 列表。"""
    today = _to_cn_date().isoformat()
    uni = get_universe(rqdatac, today, a_only=True)
    return uni["order_book_id"].tolist()


def parse_stock_symbols(arg: str, rqdatac) -> List[str]:
    if arg.strip().lower() == "all":
        syms = get_all_stock_symbols(rqdatac)
        print(f"[INFO] 获取到 {len(syms)} 只A股")
        return syms
    seen: Set[str] = set()
    out: List[str] = []
    for s in arg.split(","):
        oid = _normalize_stock_oid(s.strip())
        if oid and oid not in seen:
            seen.add(oid)
            out.append(oid)
    return out


def _get_stock_output_dir(frequency: str) -> Path:
    d = get_store_path() / "stock_data" / STOCK_FREQ_CONFIG[frequency]["dir"]
    d.mkdir(parents=True, exist_ok=True)
    return d


def _stock_csv_path(output_dir: Path, oid: str, frequency: str) -> Path:
    safe = oid.replace(".", "_")
    suffix = STOCK_FREQ_CONFIG[frequency]["suffix"]
    return output_dir / f"{safe}_{suffix}.csv"


def _read_last_stock_time(path: Path) -> Optional[pd.Timestamp]:
    """读取 CSV 末尾时间戳（只读时间列，避免全量加载）。"""
    if not path.exists():
        return None
    try:
        headers = pd.read_csv(path, nrows=0).columns.tolist()
        col = "bar_end" if "bar_end" in headers else ("date" if "date" in headers else None)
        if col is None:
            return None
        df = pd.read_csv(path, usecols=[col])
        ts = pd.to_datetime(df[col], errors="coerce").dropna()
        return ts.max() if not ts.empty else None
    except Exception:
        return None


def _compute_stock_date_range(
    path: Path,
    lookback_days: int,
    years: int,
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple:
    today = _dt.date.today()
    _end = end_date or today.strftime("%Y-%m-%d")

    if start_date:
        return start_date, _end

    last = _read_last_stock_time(path)
    if last is not None:
        s = last.date() - _dt.timedelta(days=max(0, lookback_days))
        return s.strftime("%Y-%m-%d"), _end

    s = today - _dt.timedelta(days=years * 365)
    return s.strftime("%Y-%m-%d"), _end


def _append_bar_start_stock(df: pd.DataFrame, duration_min: int) -> pd.DataFrame:
    """为A股分钟级K线补充 bar_start（早盘09:30，午盘13:00，无夜盘）。"""
    if df.empty:
        df["bar_start"] = pd.NaT
        return df

    df = df.copy()
    df["bar_end"] = pd.to_datetime(df["bar_end"])
    df = df.sort_values(["order_book_id", "bar_end"]).reset_index(drop=True)

    duration = pd.Timedelta(minutes=duration_min)
    prev_end = df.groupby("order_book_id")["bar_end"].shift(1)
    delta = df["bar_end"] - prev_end

    bar_start = df["bar_end"] - duration
    use_prev = delta.notna() & (delta <= duration)
    bar_start = bar_start.where(~use_prev, prev_end)

    bar_end = df["bar_end"]
    open_date = bar_end.dt.normalize()
    hour = bar_end.dt.hour

    # A股早盘 09:30，午盘 13:00，无夜盘
    session_open = (open_date + pd.Timedelta(hours=9, minutes=30)).where(
        ~(hour >= 13), open_date + pd.Timedelta(hours=13)
    )

    is_first = ~use_prev
    clamp = is_first & (session_open < bar_end) & (bar_start < session_open)
    bar_start = bar_start.where(~clamp, session_open)

    df.insert(df.columns.get_loc("bar_end"), "bar_start", bar_start)
    return df


def _fetch_stock_symbol(
    rqdatac, oid: str, start: str, end: str, freq: str
) -> Optional[pd.DataFrame]:
    is_intraday = STOCK_FREQ_CONFIG[freq]["duration_min"] is not None
    base_fields = ["open", "high", "low", "close", "volume", "total_turnover"]
    fields = base_fields + (["trading_date"] if is_intraday else [])

    def _call_get_price(call_fields: List[str]) -> Optional[pd.DataFrame]:
        data = rqdatac.get_price(
            oid, start_date=start, end_date=end,
            frequency=freq, fields=call_fields, adjust_type="none",
            expect_df=True,
        )
        return data if data is not None and not data.empty else None

    try:
        data = _call_get_price(fields)
        if data is not None:
            print(f"  [OK] {oid}: {len(data)} 条")
            return data
        print(f"  [SKIP] {oid} 无数据")
    except Exception as e:
        # 某些 rqdatac 版本/权限下，股票分钟线不支持 trading_date 字段；回退重试
        if is_intraday and "trading_date" in str(e):
            print(f"  [WARN] {oid}: trading_date 不可用，回退为基础字段重试")
            try:
                data = _call_get_price(base_fields)
                if data is not None:
                    print(f"  [OK] {oid}: {len(data)} 条 (fallback)")
                    return data
                print(f"  [SKIP] {oid} 无数据")
                return None
            except Exception as e2:
                print(f"  [ERROR] {oid}: {e2}")
                return None
        print(f"  [ERROR] {oid}: {e}")
    return None


def _normalize_stock_data(data: pd.DataFrame, freq: str) -> pd.DataFrame:
    df = data.reset_index()
    is_intraday = STOCK_FREQ_CONFIG[freq]["duration_min"] is not None
    if is_intraday:
        if "datetime" in df.columns:
            df = df.rename(columns={"datetime": "bar_end"})
    else:
        if "date" not in df.columns and "datetime" in df.columns:
            df = df.rename(columns={"datetime": "date"})
    return df


def _merge_stock(csv_p: Path, incoming: pd.DataFrame, freq: str) -> pd.DataFrame:
    config = STOCK_FREQ_CONFIG[freq]
    is_intraday = config["duration_min"] is not None
    tcol = "bar_end" if is_intraday else "date"

    incoming = incoming.copy()
    if "bar_start" in incoming.columns:
        incoming.drop(columns=["bar_start"], inplace=True)
    incoming[tcol] = pd.to_datetime(incoming[tcol], errors="coerce")

    pieces = [incoming]
    if csv_p.exists():
        try:
            old = pd.read_csv(csv_p)
            if not old.empty:
                if "bar_start" in old.columns:
                    old.drop(columns=["bar_start"], inplace=True)
                old[tcol] = pd.to_datetime(old[tcol], errors="coerce")
                pieces.insert(0, old)
        except Exception:
            pass

    merged = pd.concat(pieces, ignore_index=True, sort=False)
    dedup = ["order_book_id", tcol]
    present_dedup = [c for c in dedup if c in merged.columns]
    merged.dropna(subset=present_dedup, inplace=True)
    if len(present_dedup) == len(dedup):
        merged.sort_values(dedup, inplace=True)
        merged.drop_duplicates(subset=dedup, keep="last", inplace=True)

    if is_intraday:
        if "bar_start" in merged.columns:
            merged.drop(columns=["bar_start"], inplace=True)
        merged = _append_bar_start_stock(merged, config["duration_min"])
        desired = [
            "order_book_id", "bar_start", "bar_end",
            "open", "high", "low", "close",
            "volume", "total_turnover",
            "trading_date", "symbol",
        ]
    else:
        desired = [
            "order_book_id", "date",
            "open", "high", "low", "close",
            "volume", "total_turnover",
            "symbol",
        ]

    cols = [c for c in desired if c in merged.columns]
    extra = [c for c in merged.columns if c not in cols]
    return merged[cols + extra].reset_index(drop=True)


def update_stock_symbol(
    rqdatac, oid: str, freq: str, outdir: Path,
    lookback: int, years: int,
    start_date: Optional[str], end_date: Optional[str],
) -> bool:
    cp = _stock_csv_path(outdir, oid, freq)
    s, e = _compute_stock_date_range(cp, lookback, years, start_date, end_date)

    last = _read_last_stock_time(cp)
    tag = f"增量 (最新: {last})" if last else f"全量 (近{years}年)"
    print(f"  [{tag}] {s} ~ {e}")

    data = _fetch_stock_symbol(rqdatac, oid, s, e, freq)
    if data is None or data.empty:
        return False

    incoming = _normalize_stock_data(data, freq)
    if "order_book_id" not in incoming.columns:
        incoming["order_book_id"] = oid
    if "symbol" not in incoming.columns:
        incoming["symbol"] = oid.split(".")[0]

    merged = _merge_stock(cp, incoming, freq)
    merged.to_csv(cp, index=False, encoding="utf-8-sig")
    print(f"  [SAVE] {cp.name} ({len(merged)} 行)")
    return True


def run_stock_freq(
    rqdatac, *,
    frequency: str,
    symbols_arg: str,
    start_date: Optional[str],
    end_date: Optional[str],
    lookback_days: int,
    years: int,
) -> bool:
    outdir = _get_stock_output_dir(frequency)
    symbols = parse_stock_symbols(symbols_arg, rqdatac)

    if not symbols:
        print("[ERROR] 未找到任何股票标的")
        return False

    print(f"[INFO] 标的数: {len(symbols)} | 频率: {frequency} | 输出: {outdir}")
    print("=" * 70)

    ok, failed = 0, []
    for i, oid in enumerate(symbols, 1):
        print(f"\n[{i}/{len(symbols)}] {oid}")
        try:
            if update_stock_symbol(rqdatac, oid, frequency, outdir,
                                   lookback_days, years,
                                   start_date, end_date):
                ok += 1
            else:
                failed.append(oid)
        except Exception as e:
            print(f"  [ERROR] {e}")
            failed.append(oid)

    print(f"\n{'=' * 70}")
    print(f"完成! 成功: {ok}/{len(symbols)}, 失败: {len(failed)}")
    if failed:
        print(f"失败标的: {', '.join(failed)}")
    return len(failed) == 0


# ===================== main =====================

def main() -> int:
    p = argparse.ArgumentParser(
        description="RQData A股数据拉取 (日频行情 + 财务 + 全频率per-symbol)"
    )

    # 模式选择
    p.add_argument("--fetch-financials", action="store_true", help="拉取财务数据模式")

    # 日频行情参数（全市场快照模式）
    p.add_argument("--date", default=None, help="指定日期 YYYY-MM-DD（全市场快照模式）")
    p.add_argument("--backfill-days", type=int, default=1, help="回补交易日数 (默认 1)")
    p.add_argument("--all-cs", action="store_true", help="包含B股 (默认仅A股)")
    p.add_argument("--partition", choices=["year", "month"], default="month",
                   help="分区方式 (默认 month)")
    p.add_argument("--no-filter", action="store_true",
                   help="关闭 ST/停牌/低价/市值过滤（仅全市场日频模式生效）")
    p.add_argument("--min-mktcap", type=float, default=1e8,
                   help="市值过滤阈值(元) (默认 1e8)")
    p.add_argument("--emit-meta", dest="emit_meta", action="store_true", default=True)
    p.add_argument("--no-emit-meta", dest="emit_meta", action="store_false")

    # 财务数据参数
    p.add_argument("--quarter", default=None, help="单个季度, 如 2024q3")
    p.add_argument("--start-quarter", default=None, help="起始季度, 如 2023q1")
    p.add_argument("--end-quarter", default=None, help="截止季度, 如 2024q4")

    # per-symbol 频率模式参数
    p.add_argument("-f", "--frequency", default=None, choices=list(STOCK_FREQ_CONFIG),
                   help="数据频率，触发 per-symbol 模式 (1m/5m/15m/30m/60m/1d/1w)")
    p.add_argument("-s", "--symbols", default=None,
                   help="股票代码(逗号分隔)或 'all'，触发 per-symbol 模式")
    p.add_argument("--start-date", default=None,
                   help="per-symbol 模式起始日期 YYYY-MM-DD")
    p.add_argument("--end-date", default=None,
                   help="per-symbol 模式截止日期 YYYY-MM-DD")
    p.add_argument("--lookback-days", type=int, default=7,
                   help="增量回看天数 (默认 7)")
    p.add_argument("--years", type=int, default=3,
                   help="全量拉取年数 (默认 3)")

    args = p.parse_args()

    print("=" * 70)
    try:
        rqdatac = init_rqdatac()
        get_store_path()  # 提前验证存储路径
    except RuntimeError as e:
        print(e)
        return 1

    if args.fetch_financials:
        quarter = args.quarter
        start_quarter = args.start_quarter
        end_quarter = args.end_quarter

        try:
            if quarter is not None:
                quarter = _normalize_quarter_arg(quarter, "--quarter")
            if start_quarter is not None:
                start_quarter = _normalize_quarter_arg(start_quarter, "--start-quarter")
            if end_quarter is not None:
                end_quarter = _normalize_quarter_arg(end_quarter, "--end-quarter")
        except ValueError as e:
            print(f"[ERROR] {e}")
            return 1

        if quarter and (start_quarter or end_quarter):
            print("[ERROR] --quarter 与 --start-quarter/--end-quarter 不能同时使用")
            return 1
        if (start_quarter and not end_quarter) or (end_quarter and not start_quarter):
            print("[ERROR] --start-quarter 和 --end-quarter 必须同时提供")
            return 1
        if start_quarter and end_quarter and _quarter_to_tuple(start_quarter) > _quarter_to_tuple(end_quarter):
            print(
                f"[ERROR] --start-quarter ({start_quarter}) 不能晚于 "
                f"--end-quarter ({end_quarter})"
            )
            return 1

        print("RQData A股财务数据拉取")
        print("=" * 70)
        try:
            ok = run_financials(
                rqdatac,
                quarter=quarter,
                start_quarter=start_quarter,
                end_quarter=end_quarter,
                a_only=not args.all_cs,
            )
        except ValueError as e:
            print(f"[ERROR] {e}")
            return 1
    elif args.symbols is not None or args.frequency is not None:
        freq = args.frequency or "1d"
        print(f"RQData A股 per-symbol 频率行情拉取 | 频率: {freq}")
        print("=" * 70)
        ok = run_stock_freq(
            rqdatac,
            frequency=freq,
            symbols_arg=args.symbols or "all",
            start_date=args.start_date,
            end_date=args.end_date,
            lookback_days=args.lookback_days,
            years=args.years,
        )
    else:
        print("RQData A股日频行情拉取")
        print("=" * 70)
        ok = run_daily(
            rqdatac,
            date=args.date,
            backfill_days=args.backfill_days,
            a_only=not args.all_cs,
            partition=args.partition,
            min_mktcap=args.min_mktcap,
            emit_meta=args.emit_meta,
            enable_filters=not args.no_filter,
        )

    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
