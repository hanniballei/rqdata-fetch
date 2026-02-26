# rqdata-fetch skill

一个用于 **RQData / rqdatac（米筐量化）** 的数据拉取 skill，覆盖：

- 国内期货连续合约（99）与主力合约（88）
- A 股日频全市场快照
- A 股按标的多频率增量（1m/5m/15m/30m/60m/1d/1w）
- A 股财务与估值因子季度数据

## 功能概览

- **期货数据**（`scripts/fetch_futures.py`）
  - 支持频率：`1m/5m/15m/30m/60m/1d/1w`
  - 支持标的：指定品种（如 `CU,RB,IF`）或 `all`
  - 支持合约类型：`continuous`（99）/ `dominant`（88）
  - 支持增量、全量、自定义时间区间

- **股票数据**（`scripts/fetch_stocks.py`）
  - 模式 A：全市场日频快照（按交易日写文件）
  - 模式 B：按标的多频率增量（`-f/-s` 触发）
  - 财务模式：季度财务报表 + 估值因子

## 目录结构

```text
rqdata-fetch/
├── SKILL.md
├── scripts/
│   ├── common.py
│   ├── fetch_futures.py
│   └── fetch_stocks.py
├── references/
│   └── rqdata_api.md
├── tests/
│   └── test_rqdata_skill_updates.py
└── requirements.txt
```

## 环境要求

- Python 3.9+
- 可用的 `rqdatac` 权限

推荐使用虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 环境变量配置（`~/.bashrc`）

> 请使用你自己的真实凭证；不要把密钥提交到 GitHub。

```bash
# 必需：primary URI
export RQDATA_PRIMARY_URI='tcp://license:xxx@rqdatad-pro.ricequant.com:16011'

# 可选：primary 失败时降级
export RQDATA_BACKUP_USERNAME='license'
export RQDATA_BACKUP_PASSWORD='your_backup_license_key'
export RQDATA_BACKUP_HOST='rqdatad-pro.ricequant.com'
export RQDATA_BACKUP_PORT='16011'

# 必需：数据存储根目录
export RQDATA_STORE_PATH='/your/absolute/path/to/data'
```

生效：

```bash
source ~/.bashrc
```

## 快速开始

### 1) 期货

```bash
# 全品种，60m，连续合约（99）
python3 scripts/fetch_futures.py -f 60m -s all

# 指定品种，1d，主力合约（88）
python3 scripts/fetch_futures.py -f 1d -s CU,RB --contract-type dominant

# 自定义区间
python3 scripts/fetch_futures.py -f 60m -s IF --start-date 2024-01-01 --end-date 2024-12-31
```

### 2) 股票

```bash
# 模式A：全市场最近一个交易日日频快照
python3 scripts/fetch_stocks.py

# 模式B：按标的多频率（5m）
python3 scripts/fetch_stocks.py -f 5m -s 601899,000001 --years 1

# 模式B：全A股，60m，增量
python3 scripts/fetch_stocks.py -f 60m -s all --lookback-days 7
```

### 3) 财务

```bash
# 单季度
python3 scripts/fetch_stocks.py --fetch-financials --quarter 2024q3

# 区间季度
python3 scripts/fetch_stocks.py --fetch-financials --start-quarter 2023q1 --end-quarter 2024q4
```

## 输出路径规则

根目录由 `RQDATA_STORE_PATH` 决定。

- 期货：`$RQDATA_STORE_PATH/futures_data/<freq_dir>/...`
  - 例如：`daily_futures/`、`hourly_futures/`
- 股票：`$RQDATA_STORE_PATH/stock_data/...`
  - 模式 A：`daily/YYYY/MM/YYYY-MM-DD.csv`
  - 模式 B：`1min_stocks/`、`5min_stocks/`、`daily_stocks/` 等
  - 财务：`financials/<quarter>.csv`

## 触发规则（给 skill 使用者）

这个 skill 的设计是 **仅在用户明确提到 RQData/rqdatac/RiceQuant API** 时触发，
例如：

- “用 rqdata api 拉取…”
- “用 rqdatac 获取…”
- “use RiceQuant API to fetch …”

如果用户只是泛泛说“拉股票数据/期货数据”，但没有明确提到 RQData，默认不触发该 skill。

## 常见问题

- **A 股支持哪些频率？**
  - `1m/5m/15m/30m/60m/1d/1w`（由 `get_price` + 脚本频率映射支持）。
- **分钟线都一定可用吗？**
  - 取决于账号权限、标的与时间段。某些环境下分钟线 `trading_date` 字段不可用，脚本已内置回退。
- **财务季度参数格式？**
  - 必须是 `YYYYq[1-4]`，如 `2024q3`。

## 测试

```bash
.venv/bin/python -m unittest -v tests/test_rqdata_skill_updates.py
```
