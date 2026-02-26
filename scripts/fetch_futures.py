#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 RQData 增量拉取国内期货连续/主力合约数据。

支持任意频率 (1m/5m/15m/30m/60m/1d/1w) 和任意品种。
环境变量：RQDATA_PRIMARY_URI, RQDATA_BACKUP_USERNAME/PASSWORD/HOST/PORT, RQDATA_STORE_PATH

用法：
  python3 fetch_futures.py --frequency 60m --symbols all
  python3 fetch_futures.py --frequency 60m --symbols all --contract-type dominant
  python3 fetch_futures.py --frequency 1d --symbols CU,RB,IF
  python3 fetch_futures.py --frequency 60m --symbols CU --lookback-days 7
  python3 fetch_futures.py --frequency 1d --symbols all --years 5
  python3 fetch_futures.py --frequency 60m --symbols all --start-date 2024-01-01 --end-date 2024-06-30
"""

from __future__ import annotations

import argparse
import datetime as _dt
import sys
from pathlib import Path
from typing import List, Optional, Set

import pandas as pd

# 支持直接运行：python3 scripts/fetch_futures.py（将 scripts/ 加入 sys.path）
sys.path.insert(0, str(Path(__file__).parent))
from common import init_rqdatac, get_store_path  # noqa: E402

# ---- 频率配置 ----
FREQ_CONFIG = {
    "1m":  {"dir": "1min_futures",   "suffix": "1m",  "duration_min": 1},
    "5m":  {"dir": "5min_futures",   "suffix": "5m",  "duration_min": 5},
    "15m": {"dir": "15min_futures",  "suffix": "15m", "duration_min": 15},
    "30m": {"dir": "30min_futures",  "suffix": "30m", "duration_min": 30},
    "60m": {"dir": "hourly_futures", "suffix": "1h",  "duration_min": 60},
    "1d":  {"dir": "daily_futures",  "suffix": "1d",  "duration_min": None},
    "1w":  {"dir": "weekly_futures", "suffix": "1w",  "duration_min": None},
}

CONTRACT_TYPE_CONFIG = {
    "continuous": {"label": "连续合约", "suffix": "99", "file_tag": ""},
    "dominant": {"label": "主力合约", "suffix": "88", "file_tag": "_dominant"},
}


# ===================== 品种列表 =====================

def get_all_futures_symbols(rqdatac) -> List[str]:
    """获取所有期货品种代码（去重、排序）。"""
    df = rqdatac.all_instruments(type="Future")
    if df is None or df.empty:
        return []
    symbols = df["underlying_symbol"].dropna().unique().tolist()
    symbols = sorted({s.strip().upper() for s in symbols if s and s.strip()})
    return symbols


def parse_symbols(arg: str, rqdatac) -> List[str]:
    if arg.strip().lower() == "all":
        symbols = get_all_futures_symbols(rqdatac)
        print(f"[INFO] 获取到 {len(symbols)} 个期货品种")
        return symbols
    seen: Set[str] = set()
    out: List[str] = []
    for s in arg.split(","):
        s = s.strip().upper()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


# ===================== 路径与增量 =====================

def get_output_dir(frequency: str) -> Path:
    d = get_store_path() / "futures_data" / FREQ_CONFIG[frequency]["dir"]
    d.mkdir(parents=True, exist_ok=True)
    return d


def csv_path(output_dir: Path, symbol: str, frequency: str, contract_type: str) -> Path:
    tag = CONTRACT_TYPE_CONFIG[contract_type]["file_tag"]
    return output_dir / f"{symbol}_{FREQ_CONFIG[frequency]['suffix']}{tag}.csv"


def _default_oid(symbol: str, contract_type: str) -> str:
    return f"{symbol}{CONTRACT_TYPE_CONFIG[contract_type]['suffix']}"


def _read_last_time(path: Path) -> Optional[pd.Timestamp]:
    """读取 CSV 末尾时间戳（只读取时间列，避免全量加载）。"""
    if not path.exists():
        return None
    try:
        # 先读取表头以确定时间列名
        headers = pd.read_csv(path, nrows=0).columns.tolist()
        col = "bar_end" if "bar_end" in headers else ("date" if "date" in headers else None)
        if col is None:
            return None
        df = pd.read_csv(path, usecols=[col])
        ts = pd.to_datetime(df[col], errors="coerce").dropna()
        return ts.max() if not ts.empty else None
    except Exception:
        return None


def _read_existing_oid(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, usecols=["order_book_id"], nrows=1)
        return str(df.iloc[0]["order_book_id"]).strip() if not df.empty else None
    except Exception:
        return None


def compute_date_range(
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

    last = _read_last_time(path)
    if last is not None:
        s = last.date() - _dt.timedelta(days=max(0, lookback_days))
        return s.strftime("%Y-%m-%d"), _end

    s = today - _dt.timedelta(days=years * 365)
    return s.strftime("%Y-%m-%d"), _end


# ===================== bar_start 计算 =====================

def _append_bar_start(df: pd.DataFrame, duration_min: int) -> pd.DataFrame:
    """为分钟级 K 线补充 bar_start，考虑盘中休市和夜盘。"""
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

    # 从 symbol 或 order_book_id 推导品种代码
    if "symbol" in df.columns:
        base = df["symbol"].astype(str).str.upper()
    else:
        base = df["order_book_id"].astype(str).str.replace(r"\d+$", "", regex=True).str.upper()

    # 股指期货品种（沪深300/上证50/中证500/中证1000），交易时段与商品期货不同
    is_index = base.isin({"IF", "IH", "IC", "IM"})
    bar_end = df["bar_end"]
    open_date = bar_end.dt.normalize()
    hour, minute = bar_end.dt.hour, bar_end.dt.minute

    # 商品期货 session open: 09:00 / 13:30 / 21:00 / 夜盘跨日
    com_open = open_date + pd.Timedelta(hours=9)
    com_open = com_open.where(
        ~((hour > 13) | ((hour == 13) & (minute >= 30))),
        open_date + pd.Timedelta(hours=13, minutes=30),
    )
    com_open = com_open.where(~(hour >= 21), open_date + pd.Timedelta(hours=21))
    com_open = com_open.where(
        ~(hour < 9),
        (open_date - pd.Timedelta(days=1)) + pd.Timedelta(hours=21),
    )

    # 股指期货 session open: 09:30 / 13:00
    idx_open = (open_date + pd.Timedelta(hours=9, minutes=30)).where(
        ~(hour >= 13), open_date + pd.Timedelta(hours=13)
    )

    session_open = com_open.where(~is_index, idx_open)
    is_first = ~use_prev
    clamp = is_first & (session_open < bar_end) & (bar_start < session_open)
    bar_start = bar_start.where(~clamp, session_open)

    df.insert(df.columns.get_loc("bar_end"), "bar_start", bar_start)
    return df


# ===================== 数据拉取与合并 =====================

def _fetch(rqdatac, symbol: str, oid: str, start: str, end: str, freq: str) -> Optional[pd.DataFrame]:
    fields = ["open", "high", "low", "close", "volume", "open_interest", "total_turnover"]
    if FREQ_CONFIG[freq]["duration_min"] is not None:
        fields.append("trading_date")
    try:
        data = rqdatac.get_price(
            oid, start_date=start, end_date=end,
            frequency=freq, fields=fields, adjust_type="none",
            expect_df=True,
        )
        if data is not None and not data.empty:
            data["symbol"] = symbol
            print(f"  [OK] {oid}: {len(data)} 条")
            return data
        print(f"  [SKIP] {oid} 无数据")
    except Exception as e:
        print(f"  [ERROR] {oid}: {e}")
    return None


def _normalize(data: pd.DataFrame, freq: str) -> pd.DataFrame:
    df = data.reset_index()
    is_intraday = FREQ_CONFIG[freq]["duration_min"] is not None
    if is_intraday:
        if "datetime" in df.columns:
            df = df.rename(columns={"datetime": "bar_end"})
    else:
        if "date" not in df.columns and "datetime" in df.columns:
            df = df.rename(columns={"datetime": "date"})
    return df


def _merge(csv_p: Path, incoming: pd.DataFrame, freq: str) -> pd.DataFrame:
    config = FREQ_CONFIG[freq]
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
    merged.dropna(subset=dedup, inplace=True)
    merged.sort_values(dedup, inplace=True)
    merged.drop_duplicates(subset=dedup, keep="last", inplace=True)

    if is_intraday:
        if "bar_start" in merged.columns:
            merged.drop(columns=["bar_start"], inplace=True)
        merged = _append_bar_start(merged, config["duration_min"])
        desired = [
            "order_book_id", "bar_start", "bar_end",
            "open", "high", "low", "close",
            "volume", "open_interest", "total_turnover",
            "trading_date", "symbol",
        ]
    else:
        desired = [
            "order_book_id", "date",
            "open", "high", "low", "close",
            "volume", "open_interest", "total_turnover",
            "symbol",
        ]

    cols = [c for c in desired if c in merged.columns]
    extra = [c for c in merged.columns if c not in cols]
    return merged[cols + extra].reset_index(drop=True)


# ===================== 单品种更新 =====================

def update_symbol(
    rqdatac, symbol: str, freq: str, outdir: Path,
    contract_type: str,
    lookback: int, years: int,
    start_date: Optional[str], end_date: Optional[str],
) -> bool:
    cp = csv_path(outdir, symbol, freq, contract_type)
    oid = _read_existing_oid(cp) or _default_oid(symbol, contract_type)
    s, e = compute_date_range(cp, lookback, years, start_date, end_date)

    last = _read_last_time(cp)
    tag = f"增量 (最新: {last})" if last else f"全量 (近{years}年)"
    print(f"  [{tag}] {s} ~ {e}")

    data = _fetch(rqdatac, symbol, oid, s, e, freq)
    if data is None or data.empty:
        return False

    incoming = _normalize(data, freq)
    if "order_book_id" not in incoming.columns:
        incoming["order_book_id"] = oid
    else:
        incoming["order_book_id"] = incoming["order_book_id"].fillna(oid)
    if "symbol" not in incoming.columns:
        incoming["symbol"] = symbol
    else:
        incoming["symbol"] = incoming["symbol"].fillna(symbol)
    merged = _merge(cp, incoming, freq)
    merged.to_csv(cp, index=False, encoding="utf-8-sig")
    print(f"  [SAVE] {cp.name} ({len(merged)} 行)")
    return True


# ===================== main =====================

def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="RQData 期货连续/主力合约数据拉取")
    p.add_argument("-f", "--frequency", default="60m", choices=list(FREQ_CONFIG),
                   help="数据频率 (默认 60m)")
    p.add_argument(
        "--contract-type",
        default="continuous",
        choices=list(CONTRACT_TYPE_CONFIG),
        help="合约类型: continuous(连续合约,99) / dominant(主力合约,88)",
    )
    p.add_argument("-s", "--symbols", default="all",
                   help="品种列表(逗号分隔)或 'all' (默认 all)")
    p.add_argument("--start-date", default=None, help="起始日期 YYYY-MM-DD")
    p.add_argument("--end-date", default=None, help="截止日期 YYYY-MM-DD")
    p.add_argument("--lookback-days", type=int, default=7, help="增量回看天数 (默认 7)")
    p.add_argument("--years", type=int, default=10, help="全量拉取年数 (默认 10)")
    args = p.parse_args(argv)

    print("=" * 70)
    ctype_label = CONTRACT_TYPE_CONFIG[args.contract_type]["label"]
    print(f"RQData 期货数据拉取 | 类型: {ctype_label} | 频率: {args.frequency}")
    print("=" * 70)

    try:
        rqdatac = init_rqdatac()
        outdir = get_output_dir(args.frequency)
    except RuntimeError as e:
        print(e)
        return 1
    symbols = parse_symbols(args.symbols, rqdatac)

    if not symbols:
        print("[ERROR] 未找到任何品种")
        return 1

    print(f"[INFO] 品种数: {len(symbols)} | 输出: {outdir}")
    print("=" * 70)

    ok, failed = 0, []
    for i, sym in enumerate(symbols, 1):
        print(f"\n[{i}/{len(symbols)}] {sym}")
        try:
            if update_symbol(
                rqdatac=rqdatac,
                symbol=sym,
                freq=args.frequency,
                outdir=outdir,
                contract_type=args.contract_type,
                lookback=args.lookback_days,
                years=args.years,
                start_date=args.start_date,
                end_date=args.end_date,
            ):
                ok += 1
            else:
                failed.append(sym)
        except Exception as e:
            print(f"  [ERROR] {e}")
            failed.append(sym)

    print(f"\n{'=' * 70}")
    print(f"完成! 成功: {ok}/{len(symbols)}, 失败: {len(failed)}")
    if failed:
        print(f"失败品种: {', '.join(failed)}")
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
