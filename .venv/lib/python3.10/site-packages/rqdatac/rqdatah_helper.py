# -*- coding: utf-8 -*-

from functools import partial
try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable
import pandas as pd


def rqdatah_serialize(f=None, converter=None, **kwargs):
    """
    为api添加结果转化函数，方便rqdatah将api返回结果转为文本格式。

    params f: api函数
    params converter: 函数类型，负责将api返回结果转化成为字符串
    params kwargs: 传入converter的额外参数
    """
    if f is None:
        return partial(rqdatah_serialize, converter=converter, **kwargs)
    f.http_converter = partial(converter, **kwargs)
    return f


def rqdatah_no_index_mark(func):
    """
    为api添加result_has_no_index属性

    params func: api函数
    """
    setattr(func, "result_has_no_index", True)
    return func


# rqdatah converters，负责将api返回结果转化成为字符串


def http_conv_list_to_csv(data, **kwargs):
    """ list 转化成csv """
    if not isinstance(data, list):
        data = [data]
    name = kwargs.get('name', 'Unknown')
    return name + '\n' + '\n'.join(str(d) for d in data)


def http_conv_trading_hours(hours):
    header = 'start_at,end_at\n'
    if isinstance(hours, str):
        return header + hours

    return header + '\n'.join('{},{}'.format(str(s), str(e)) for (s, e) in hours)


def http_conv_instruments(instruments):
    """ instruments 转换成csv """
    if not instruments:
        return None
    if not isinstance(instruments, list):
        instruments = [instruments]

    df = pd.DataFrame([i.__dict__ for i in instruments])
    df.set_index('order_book_id', inplace=True)
    return df.to_csv()


def tick_to_dict(t):
    d = {n: getattr(t, n) for n in dir(t) if not n.startswith('_')}

    asks = d.pop('asks')
    if isinstance(asks, Iterable):
        d.update(('ask{}'.format(i), v) for i, v in enumerate(asks))

    ask_vols = d.pop('ask_vols')
    if isinstance(ask_vols, Iterable):
        d.update(('ask_vol{}'.format(i), v) for i, v in enumerate(ask_vols))

    bids = d.pop('bids')
    if isinstance(bids, Iterable):
        d.update(('bid{}'.format(i), v) for i, v in enumerate(bids))

    bid_vols = d.pop('bid_vols')
    if isinstance(bids, Iterable):
        d.update(('bid_vol{}'.format(i), v) for i, v in enumerate(bid_vols))

    return d


def http_conv_ticks(ticks):
    """ ticks 转换成csv """
    if not isinstance(ticks, list):
        ticks = [ticks]

    df = pd.DataFrame([tick_to_dict(t) for t in ticks])
    df.set_index('order_book_id', inplace=True)
    return df.to_csv()


def http_conv_dict_to_csv(d):
    """ dict 转换成csv """
    header = 'name,value\n'
    body = '\n'.join('{},{}'.format(k, v) for k, v in d.items())
    return header + body


def http_conv_index_compoents(data, **kwargs):
    if isinstance(data, list):
        return http_conv_list_to_csv(data, **kwargs)
    # expect dict
    if isinstance(data, dict):
        body = "date,order_book_id"
        for dt, obs in data.items():
            for oid in obs:
                body += "\n{},{}".format(dt, oid)
        return body
    return str(data)
