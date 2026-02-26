import datetime
import warnings

import pandas as pd
from rqdatac.services.stock_status import get_shares
from rqdatac.services.get_price import get_price
from rqdatac.services.calendar import is_trading_date
from rqdatac.services.live import current_snapshot
from rqdatac.validators import ensure_order_book_ids
from rqdatac.decorators import export_as_api


@export_as_api
def current_freefloat_turnover(order_book_ids):
    """
    获取流通换手率数据

    Parameters
    ----------
    order_book_ids : str | list[str]
        给出单个或多个 order_book_id

    Returns
    -------
    pandas.Series
        截至到调用时间的当日累计自由流通换手率
        自由流通换手率=当日累计成交金额/自由流通市值（盘中实时分钟级别）

    Examples
    --------
    获取多个合约当日累计自由流通换手率

    >>> rqdatac.current_freefloat_turnover(['000001.XSHE','600000.XSHG'])
    order_book_id  datetime
    000001.XSHE    2025-12-30 10:38:27.000    0.003685
    600000.XSHG    2025-12-30 10:38:27.338    0.002200
    Name: freefloat_turnover, dtype: float64
    """
    today = datetime.date.today()
    if not is_trading_date(today):
        warnings.warn('today is not a trading day!')
        return None

    order_book_ids = ensure_order_book_ids(order_book_ids, type='CS')
    shares = get_shares(order_book_ids, today, today, fields='free_circulation')
    if shares is None:
        return None
    shares = shares.droplevel(1)['free_circulation']

    snapshots = current_snapshot(order_book_ids)
    if len(order_book_ids) == 1:
        snapshots = [snapshots]
    t_shares = pd.Series({(t.order_book_id, t.datetime): t.total_turnover/t.last for t in snapshots if t.datetime.date() == today})
    t_shares.index.names = ['order_book_id', 'datetime']
    turnover = t_shares / shares
    turnover.name = 'freefloat_turnover'
    return turnover


@export_as_api
def get_live_minute_price_change_rate(order_book_ids):
    """
    获取当日分钟累计收益率

    Parameters
    ----------
    order_book_ids : str | list[str]
        合约代码，给出单个或多个 order_book_id

    Returns
    -------
    pandas.DataFrame
        包含涨跌幅数据的 DataFrame

    Examples
    --------
    获取多个合约当日分钟涨跌幅

    >>> rqdatac.get_live_minute_price_change_rate(['000001.XSHE','600000.XSHG'])
    order_book_id        000001.XSHE  600000.XSHG
    datetime
    2022-09-23 09:31:00    -0.002441    -0.002809
    2022-09-23 09:32:00    -0.001627    -0.001404
    2022-09-23 09:33:00     0.000814    -0.002809
    2022-09-23 09:34:00     0.000814    -0.002809
    2022-09-23 09:35:00     0.000000    -0.001404
    ...                          ...          ...
    2022-09-23 14:56:00    -0.000814     0.004213
    2022-09-23 14:57:00     0.000000     0.004213
    2022-09-23 14:58:00     0.000814     0.004213
    2022-09-23 14:59:00     0.000814     0.004213
    2022-09-23 15:00:00     0.000000     0.005618
    [240 rows x 2 columns]

    """
    today = datetime.date.today()
    if not is_trading_date(today):
        warnings.warn('today is not a trading day!')
        return None

    order_book_ids = ensure_order_book_ids(order_book_ids)
    close = get_price(order_book_ids, today, today, '1m', fields='close', adjust_type='none')
    if close is None:
        warnings.warn('today minute data is not ready')
        return

    close = close['close'].unstack('order_book_id')
    snapshots = current_snapshot(order_book_ids)
    if len(order_book_ids) == 1:
        snapshots = [snapshots]
    prev_close = pd.Series({t.order_book_id: t.prev_close for t in snapshots})
    minute_return = close / prev_close - 1
    return minute_return
