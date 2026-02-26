# RQData Python API Reference

## Initialization

```python
import rqdatac
rqdatac.init(username="license", password="<license_key>")
# or URI format:
rqdatac.init(uri="tcp://license:xxx@host:port")
# or positional args (used for backup/fallback connections):
rqdatac.init(username, password, (host, port))

# Reset connection before re-initializing (e.g., when switching to backup credentials)
rqdatac.reset()
```

## Core: get_price()

```python
rqdatac.get_price(
    order_book_ids,          # str or list: '000001.XSHE', 'CU99', ['IF2403', 'IC2403']
    start_date=None,         # str: '2024-01-01'
    end_date=None,           # str: '2024-12-31'
    frequency='1d',          # '1m','5m','15m','30m','60m','1d','1w','tick'
    fields=None,             # list or None (all)
    adjust_type='pre',       # 'none','pre','post','pre_volume','post_volume'
    skip_suspended=False,
    expect_df=True,
    market='cn',
)
```

**Fields:** open, high, low, close, volume, total_turnover, num_trades, prev_close, limit_up, limit_down, settlement (futures), prev_settlement (futures), open_interest (futures), trading_date (intraday only — the natural trading date a bar belongs to, handles night-session cross-day)

## Instruments

```python
# List all instruments of a type
rqdatac.all_instruments(type='Future')  # 'CS','ETF','Future','Option','INDX',...
rqdatac.all_instruments(type='CS', date='20240101')

# Single instrument metadata
rqdatac.instruments('000001.XSHE')
```

## Trading Calendar

```python
rqdatac.get_trading_dates(start_date, end_date, market='cn')
```

## Futures Module

```python
# Dominant contract series
rqdatac.futures.get_dominant('IF', start_date, end_date, rule=0, rank=1)

# Active contracts on a date
rqdatac.futures.get_contracts('IF', date='20240101')

# Continuous contract series
rqdatac.futures.get_continuous_contracts('IF', start_date, end_date, type='front_month')

# Dominant price (with roll adjustment)
rqdatac.futures.get_dominant_price('IF', start_date, end_date,
    frequency='1d', adjust_type='pre', adjust_method='prev_close_spread')
```

**Continuous contract order_book_id convention:** `{SYMBOL}99` (e.g., CU99, RB99, IF99)

## Stock Financial Data

```python
# Point-in-time financial statements
rqdatac.get_pit_financials_ex(
    order_book_ids=['000001.XSHE'],
    fields=['revenue', 'net_profit', 'total_assets'],
    start_quarter='2023q1', end_quarter='2024q4',
)

# Valuation/technical factors
rqdatac.get_factor(
    order_book_ids=['000001.XSHE'],
    factor=['pe_ratio_ttm', 'pb_ratio_lf', 'market_cap'],
    start_date='20240101', end_date='20240131',
)

# ST status / suspension
rqdatac.is_st_stock(order_book_ids=[oid], start_date=s, end_date=e)
rqdatac.is_suspended(order_book_ids=[oid], start_date=s, end_date=e)

# Shares outstanding
rqdatac.get_shares(order_book_ids=[oid], start_date=s, end_date=e)
```

## Common Financial Fields

**Income:** revenue, operating_revenue, net_profit, gross_profit, profit_from_operation, basic_earnings_per_share, ebitda

**Balance Sheet:** total_assets, total_liabilities, total_equity, cash_equivalent, inventory, fixed_assets

**Cash Flow:** cash_flow_from_operating_activities, cash_flow_from_investing_activities, cash_flow_from_financing_activities

## Common Factor Names

**Valuation:** pe_ratio_lyr, pe_ratio_ttm, pb_ratio_lyr, pb_ratio_lf, ps_ratio_ttm, market_cap, ev_ttm, dividend_yield_ttm

**Profitability:** return_on_equity_ttm, net_profit_margin_ttm, gross_profit_margin_ttm

**Growth:** net_profit_growth_ratio_ttm, operating_revenue_growth_ratio_ttm, net_asset_growth_ratio_lyr

**Leverage:** debt_to_asset_ratio_lyr, current_ratio_lyr, quick_ratio_lyr

## Order Book ID Format

| Type | Format | Example |
|------|--------|---------|
| Shanghai A | XXXXXX.XSHG | 600000.XSHG |
| Shenzhen A | XXXXXX.XSHE | 000001.XSHE |
| Futures | {code}{YYMM} | IF2403, CU2312 |
| Continuous | {symbol}99 | CU99, IF99 |
