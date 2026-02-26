---
name: rqdata-fetch
description: >
  Fetch market data from RQData (米筐量化 / rqdatac / RiceQuant API) for Chinese futures and
  A-share stocks. Use this skill ONLY when the user explicitly asks to use RQData API to
  fetch/pull/download/update data (for example: "用 rqdata api 拉取", "用 rqdatac 获取",
  "use RiceQuant API"). Do NOT trigger for generic stock/futures data requests that do not
  explicitly mention RQData/rqdatac/RiceQuant API.
---

# RQData Fetch

Fetch Chinese futures and A-share data via RQData (rqdatac) API.

## Prerequisites

User must set environment variables in `~/.bashrc`:

```bash
# 必需：primary 连接 URI
export RQDATA_PRIMARY_URI='tcp://license:xxx@rqdatad-pro.ricequant.com:16011'

# 可选：backup 凭证（primary 失败时自动降级）
export RQDATA_BACKUP_USERNAME='license'
export RQDATA_BACKUP_PASSWORD='your_backup_license_key'
export RQDATA_BACKUP_HOST='rqdatad-pro.ricequant.com'
export RQDATA_BACKUP_PORT='16011'

# 必需：数据存储根路径
export RQDATA_STORE_PATH='/path/to/data/storage'
```

> Note: 上述值均为示例，请替换为你自己的真实配置。

**Install (recommended: isolated venv to avoid version conflicts):**

```bash
# 进入 skill 根目录（包含 SKILL.md / scripts / requirements.txt）
# 如果你已经在该目录，可跳过 cd
cd <your-local-rqdata-fetch-path>

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If `.venv` already exists in this folder, you can reuse it directly:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

Or without venv (system-wide, may conflict with other packages):

```bash
pip install rqdatac pandas numpy
```

> Requires Python >= 3.9 (`zoneinfo` is stdlib since 3.9).

## Scripts

### 1. Futures: `scripts/fetch_futures.py`

Fetch futures continuous/dominant contract OHLCV + open_interest data
(`{SYMBOL}99` for continuous, `{SYMBOL}88` for dominant).

**Usage:**
```bash
# All symbols, hourly
python3 scripts/fetch_futures.py -f 60m -s all

# Specific symbols, daily
python3 scripts/fetch_futures.py -f 1d -s CU,RB,IF

# Dominant contracts (主力合约, 88)
python3 scripts/fetch_futures.py -f 60m -s CU,RB --contract-type dominant

# Incremental with 7-day lookback
python3 scripts/fetch_futures.py -f 60m -s CU,RB --lookback-days 7

# Full fetch, 5 years
python3 scripts/fetch_futures.py -f 1d -s all --years 5

# Custom date range
python3 scripts/fetch_futures.py -f 60m -s all --start-date 2024-01-01 --end-date 2024-06-30
```

**Options:** `-f` frequency (1m/5m/15m/30m/60m/1d/1w), `-s` symbols (comma-separated or "all"),
`--contract-type` (`continuous`=99, `dominant`=88), `--start-date`, `--end-date`,
`--lookback-days` (default 7), `--years` (default 10)

**Output structure:**
```
$RQDATA_STORE_PATH/futures_data/
├── hourly_futures/          # -f 60m
│   ├── CU_1h.csv
│   ├── RB_1h.csv
│   ├── CU_1h_dominant.csv   # --contract-type dominant
│   └── ...
├── daily_futures/           # -f 1d
│   ├── CU_1d.csv
│   └── ...
├── 5min_futures/            # -f 5m
└── weekly_futures/          # -f 1w
```

**Intraday CSV columns:** order_book_id, bar_start, bar_end, open, high, low, close, volume, open_interest, total_turnover, trading_date, symbol

**Daily/Weekly CSV columns:** order_book_id, date, open, high, low, close, volume, open_interest, total_turnover, symbol

**Incremental logic:** If CSV exists, read last bar_end/date, fetch from `last - lookback_days` to today, merge and deduplicate. If no CSV, full fetch (default 10 years).

### 2. Stocks: `scripts/fetch_stocks.py`

Fetch A-share daily OHLCV and financial data. Supports two modes:

**Mode A – Full-market daily snapshot (existing, unchanged):**
```bash
# Latest trading day
python3 scripts/fetch_stocks.py

# Specific date
python3 scripts/fetch_stocks.py --date 2024-01-15

# Backfill last 5 trading days
python3 scripts/fetch_stocks.py --backfill-days 5

# Include B-shares
python3 scripts/fetch_stocks.py --all-cs

# Year partition (default: month)
python3 scripts/fetch_stocks.py --partition year

# Disable default filtering (keep all tradable rows)
python3 scripts/fetch_stocks.py --no-filter
```

**Mode B – Per-symbol frequency (new): triggered by `-s` or `-f`:**
```bash
# Single stock, 5-minute bars, last 1 year
python3 scripts/fetch_stocks.py -f 5m -s 601899 --years 1

# Multiple stocks, daily bars
python3 scripts/fetch_stocks.py -f 1d -s 601899,000001 --years 1

# All A-share stocks, hourly bars (incremental)
python3 scripts/fetch_stocks.py -f 60m -s all --lookback-days 7

# Weekly bars for specific stocks, custom date range
python3 scripts/fetch_stocks.py -f 1w -s 600519,000858 --start-date 2022-01-01 --end-date 2024-12-31

# If only -s given, defaults to 1d frequency
python3 scripts/fetch_stocks.py -s 601899,000001
```

**Options for per-symbol mode:** `-f` frequency (1m/5m/15m/30m/60m/1d/1w), `-s` symbols
(comma-separated bare codes or OIDs or "all"), `--start-date`, `--end-date`,
`--lookback-days` (default 7), `--years` (default 3)

Note: bare codes are auto-completed — `6xxxxx`/`688xxx` → `.XSHG`, `0xxxxx`/`3xxxxx` → `.XSHE`.

**Financial data usage:**
```bash
# Single quarter
python3 scripts/fetch_stocks.py --fetch-financials --quarter 2024q3

# Quarter range
python3 scripts/fetch_stocks.py --fetch-financials --start-quarter 2023q1 --end-quarter 2024q4

# Default: last 4 quarters
python3 scripts/fetch_stocks.py --fetch-financials
```

**Output structure:**
```
$RQDATA_STORE_PATH/stock_data/
├── daily/                   # Mode A: full-market snapshot (--partition month)
│   └── 2024/
│       ├── 01/
│       │   ├── 2024-01-02.csv
│       │   └── ...
│       └── ...
├── financials/              # Financial statements
│   ├── 2024q1.csv
│   └── ...
├── 1min_stocks/             # Mode B: -f 1m (per-symbol)
│   ├── 601899_XSHG_1m.csv
│   └── ...
├── 5min_stocks/             # Mode B: -f 5m
├── 15min_stocks/            # Mode B: -f 15m
├── 30min_stocks/            # Mode B: -f 30m
├── hourly_stocks/           # Mode B: -f 60m
│   ├── 601899_XSHG_1h.csv
│   └── ...
├── daily_stocks/            # Mode B: -f 1d (per-symbol daily)
│   ├── 601899_XSHG_1d.csv
│   └── ...
└── weekly_stocks/           # Mode B: -f 1w
```

**Mode A daily CSV columns:** order_book_id, open, high, low, close, volume, money [, symbol, listed_date, is_st, is_suspended, market_cap]

Note: metadata columns are included by **default** (`--emit-meta` is on). Use `--no-emit-meta` to suppress them.

**Mode B intraday CSV columns (1m/5m/15m/30m/60m):** order_book_id, bar_start, bar_end, open, high, low, close, volume, total_turnover, symbol (+ `trading_date` when rqdatac supports this field)

**Mode B daily/weekly CSV columns (1d/1w):** order_book_id, date, open, high, low, close, volume, total_turnover, symbol

**Filtering (Mode A only):** Default excludes ST stocks, suspended stocks, stocks with 3+ days close < 1 CNY, market cap < 1e8 CNY. Use `--no-filter` to disable these filters.

**Incremental logic (Mode B):** If CSV exists, read last bar_end/date, fetch from `last - lookback_days` to today. If no CSV, full fetch (default 3 years). Use `--start-date`/`--end-date` to override completely.

**Financial CSV:** Includes income statement (revenue, net_profit, etc.), balance sheet (total_assets, etc.), cash flow, plus valuation factors (PE, PB, ROE, etc.).

## Historical Backfill

### Futures (`fetch_futures.py`)

The default incremental mode only extends **forward** (from `last_date - lookback_days` to today).
To backfill data **earlier than what is already in the CSV**, use explicit `--start-date` and `--end-date`:

```bash
# CSV has 2020-2025; add 2015-2019 without re-fetching existing data
python3 scripts/fetch_futures.py -f 1d -s CU --start-date 2015-01-01 --end-date 2019-12-31
```

The `_merge` function reads the existing CSV, concatenates the new data, deduplicates, and re-saves.
Omitting `--end-date` is valid but will re-fetch already-stored dates (handled by dedup, but wastes API calls).

### Stocks (`fetch_stocks.py`)

**Mode A (full-market snapshot):** Each trading day is written to its own file, so historical backfill is simply:

```bash
python3 scripts/fetch_stocks.py --date 2024-01-15       # single day
python3 scripts/fetch_stocks.py --backfill-days 20      # last N trading days
```

**Mode B (per-symbol frequency):** Same incremental logic as futures — use explicit `--start-date` and `--end-date` to backfill earlier data:

```bash
# CSV has 2023-2025; add 2020-2022 without re-fetching existing data
python3 scripts/fetch_stocks.py -f 1d -s 601899 --start-date 2020-01-01 --end-date 2022-12-31
```

The `_merge_stock` function reads existing CSV, concatenates new data, deduplicates, and re-saves.

## API Reference

For detailed rqdatac API documentation, see [references/rqdata_api.md](references/rqdata_api.md).

## Customization

To modify default financial fields, edit `DEFAULT_FINANCIAL_FIELDS` and `DEFAULT_FACTOR_FIELDS` in `scripts/fetch_stocks.py`.

To add new frequency mappings for futures, edit `FREQ_CONFIG` in `scripts/fetch_futures.py`.

To add new frequency mappings for stocks, edit `STOCK_FREQ_CONFIG` in `scripts/fetch_stocks.py`.
