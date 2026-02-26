# -*- coding: utf-8 -*-
import bisect
import socket
import threading
import time
import datetime
import ssl
import warnings
import weakref
from collections import defaultdict

from rqdatac.services.basic import instruments, get_previous_trading_date
from rqdatac.validators import ensure_list_of_string
from rqdatac.decorators import export_as_api, retry, ttl_cache
from rqdatac.utils import datetime_to_int14, int8_to_date, connection_error
from rqdatac.client import get_client
from rqdatac.share.errors import PermissionDenied, QuotaExceeded

try:
    from orjson import dumps as json_dumps, loads as json_loads
except ImportError:
    try:
        import rapidjson as json
    except ImportError:
        import json

    def json_dumps(*args, **kwargs):
        return json.dumps(*args, **kwargs).encode('utf-8')

    def json_loads(*args, **kwargs):
        return json.loads(*args, **kwargs)


def str_to_dt_time(s):
    """ '21:31' -->  datetime.time(21, 31) """
    return datetime.time(int(s[0:2]), int(s[3:5]))


@ttl_cache(1800)
def to_trading_periods(trading_hours, date):
    trading_hours = [t.split("-", 1) for t in trading_hours.split(",")]
    for i, (start, end) in enumerate(trading_hours):
        trading_hours[i][0] = str_to_dt_time(start)
        trading_hours[i][1] = str_to_dt_time(end)

    td = int8_to_date(date)
    prev_td = get_previous_trading_date(date)
    prev_td_next = prev_td + datetime.timedelta(days=1)

    for i, (start, end) in enumerate(trading_hours):
        if start.hour > 16:
            start_dt = prev_td
            end_dt = start_dt if end.hour > 16 else prev_td_next
        else:
            start_dt = end_dt = td
        trading_hours[i][0] = datetime.datetime.combine(start_dt, start)
        trading_hours[i][1] = datetime.datetime.combine(end_dt, end)

    return trading_hours


def get_trading_minutes(order_book_id, trading_date):
    result = []
    delta = datetime.timedelta(minutes=1)
    for start, end in to_trading_periods(instruments(order_book_id).trading_hours, trading_date):
        dt = start
        while dt <= end:
            result.append(dt)
            dt += delta
    return result


def futures_resample_dts(trading_minutes, freq):
    delta = datetime.timedelta(minutes=freq)
    dt = trading_minutes[0] - datetime.timedelta(minutes=1) + delta
    dts = set()
    while dt <= trading_minutes[-1]:
        dts.add(trading_minutes[bisect.bisect_right(trading_minutes, dt) - 1])
        dt += delta
    if trading_minutes[-1] not in dts:
        dts.add(trading_minutes[-1])
    return sorted(dts)


class MinbarResampler:
    _resamples = weakref.WeakSet()
    _cache = {}

    @classmethod
    def batch_get_bar_from_rqdatac(cls, freq, dt):
        if (freq, dt) in cls._cache:
            return cls._cache[freq, dt]

        fields = set()
        order_book_ids = []
        for r in cls._resamples:
            if r.freq == freq and r._bar_dts is not None and dt in r._bar_dts:
                order_book_ids.append(r.order_book_id)
                if r.fields is not None:
                    fields.update(r.fields)

        if not order_book_ids or not fields:
            return

        try:
            bars = get_client().execute('get_today_minbar', order_book_ids, list(fields), freq, market='cn')
        except PermissionDenied:
            cls._cache[freq, dt] = None
            return

        if not bars:
            return

        result = {}
        for ob, data in bars:
            try:
                index = data['datetime'].index(dt)
            except ValueError:
                continue

            result[ob] = {k: v[index] for k, v in data.items()}

        cls._cache = {k: v for k, v in cls._cache.items() if k[1] == dt}
        cls._cache[freq, dt] = result

        return result

    def __init__(self, order_book_id, freq):
        MinbarResampler._resamples.add(self)
        self.order_book_id = order_book_id
        self.freq = freq
        self.fields = None
        self._current_bar = None
        self._max_bar_dt = None
        self._bar_dts = None
        self.channel = 'bar_' + order_book_id + '_' + str(freq) + 'm'

    def get_bar_from_rqdatac(self, dt):
        bars = MinbarResampler.batch_get_bar_from_rqdatac(self.freq, dt)
        if bars is None or self.order_book_id not in bars:
            return

        bar = bars[self.order_book_id]
        bar = {k: bar.get(k) for k in self.fields}
        bar['datetime'] = dt
        bar['order_book_id'] = self.order_book_id
        bar['channel'] = self.channel
        bar['action'] = 'feed'
        return bar

    def reset(self):
        self._current_bar = None

    def enqueue(self, bar):
        if self.freq == 1:
            bar = bar.copy()
            bar['channel'] = self.channel
            return bar

        if self.fields is None:
            self.fields = [f for f in bar.keys() if f not in ['order_book_id', 'datetime', 'channel', 'action']]

        dt = bar['datetime']
        if self._max_bar_dt is None or dt > self._max_bar_dt:
            trading_minutes = get_trading_minutes(self.order_book_id, bar['trading_date'])
            ins = instruments(self.order_book_id)
            if ins.type in ('Future', 'Option', 'Spot') and ins.exchange not in ("CFFEX", "CCFX", 'XSHG', 'XSHE'):
                bar_dts = futures_resample_dts(trading_minutes, self.freq)
            elif len(trading_minutes) % self.freq == 0:
                bar_dts = trading_minutes[self.freq-1::self.freq]
            else:
                bar_dts = trading_minutes[self.freq-1::self.freq] + trading_minutes[-1:]

            bar_dts = [datetime_to_int14(dt) for dt in bar_dts]
            self._bar_dts = set(bar_dts)
            self._max_bar_dt = bar_dts[-1]
            trading_minutes = [datetime_to_int14(dt) for dt in trading_minutes]

            if dt == trading_minutes[0] or trading_minutes[trading_minutes.index(dt) - 1] in self._bar_dts:
                self._current_bar = bar.copy()
                return

        if dt in self._bar_dts:
            if self._current_bar is not None:
                self._update_current_bar(bar)
                bar, self._current_bar = self._current_bar, {}
                bar['channel'] = self.channel
                return bar
            else:
                self._current_bar = {}
                return self.get_bar_from_rqdatac(dt)

        if self._current_bar is not None:
            self._update_current_bar(bar)

    def _update_current_bar(self, bar):
        for k, v in bar.items():
            if k in ('volume', 'total_turnover', 'num_trades'):
                self._current_bar[k] = self._current_bar.get(k, 0) + v
            elif k == 'open':
                if 'open' not in self._current_bar:
                    self._current_bar['open'] = v
            elif k == 'low':
                if 'low' not in self._current_bar or self._current_bar['low'] > v:
                    self._current_bar['low'] = v
            elif k == 'high':
                if 'high' not in self._current_bar or self._current_bar['high'] < v:
                    self._current_bar['high'] = v
            else:
                self._current_bar[k] = v


def _to_url(proxy_info):
    scheme, host, port, user, password = proxy_info
    from urllib.parse import quote
    url = scheme + '://'
    if user:
        url += quote(user) + ':' + quote(password if password else '') + '@'
    url += host + ':' + str(port)
    return url


@export_as_api
class LiveMarketDataClient:
    def __init__(self, ws_server_uri="wss://rqdata.ricequant.com/live_md", proxy_info=None):
        """websocket对象初始化

        :param ws_server_uri: websocket服务地址, 如 wss://rqdata.ricequant.com/live_md
        :param proxy_info: 代理信息, 需要为5个元素的元组: (proxy_type, host, port, user, password),
            如 ("http", "localhost", 18089, None, None)

        """
        self._info = None
        self._client = None
        self._ws_server_uri = ws_server_uri
        self._subscribed = set()
        if proxy_info:
            if not isinstance(proxy_info, tuple) or len(proxy_info) != 5:
                raise ValueError("expected a tuple like (proxy_type, host, port, user, password)")

            self._proxy_url = _to_url(proxy_info)
        else:
            self._proxy_url = None

        self._init_websocket_client()
        self._resamplers = defaultdict(dict)
        self._subscribed_by_user = set()
        self._closed = False

    def _init_websocket_client(self):
        _token = get_client().execute(
            "user.get_live_md_auth_token",
        )

        try:
            import websockets
        except ImportError:
            raise ImportError(
                "LiveMarketDataClient requires websockets package; run 'pip install websockets' to fix.")

        login_data = {
            "action": "auth_by_token",
            "token": _token
        }

        retry(suppress_exceptions=(websockets.WebSocketException,), count=3)(self._connect_and_login)(login_data)

    def _connect_and_login(self, login_data):
        from websockets.sync.client import connect
        try:
            self._client = connect(self._ws_server_uri, proxy=self._proxy_url)
        except ssl.SSLCertVerificationError:
            context = ssl.SSLContext()
            context.verify_mode = ssl.CERT_NONE
            self._client = connect(self._ws_server_uri, proxy=self._proxy_url, ssl=context)

        self._client.send(json_dumps(login_data))
        res = self._client.recv(timeout=3, decode=False)
        self._info = json_loads(res)

    @property
    def info(self):
        return self._info

    @property
    def subscriptions(self):
        """
        获取当前正在订阅的所有频道
        """
        return list(self._subscribed_by_user)

    def close(self):
        self._closed = True
        try:
            self._client.close()
        except:
            pass

    def subscribe(self, channels):
        """订阅实时行情

        :param channels: 订阅的标的列表 分钟和tick分别以 bar_ 和tick_开头 以平安银行为例，
            subscribe('bar_000001.XSHE')  # 订阅分钟线的实时行情
            subscribe('bar_AU2112_15m')   # 订阅15分钟线的实时行情
            subscribe('tick_000001.XSHE')  # 订阅tick的实时行情
            可以同时订阅多支标的 subscribe(['bar_000001.XSHE'， 'bar_000002.XSHE')

        """
        if self._closed:
            raise RuntimeError('this connection is closed.')

        channels = ensure_list_of_string(channels)
        to_subscribe = set()
        for ch in channels:
            ob = ch.split('_')[1]
            if not instruments(ob):
                warnings.warn("invalid order_book_id: {}, channel {} ignored".format(ob, ch), stacklevel=0)
                continue

            self._subscribed_by_user.add(ch)

            if ch.startswith('bar_') and ch.endswith('m'):
                _, order_book_id, freq = ch.split('_')
                if int(freq[:-1]) == 1:
                    warnings.warn('channel {}: for 1-minute bar, please use the format {} directyly'.format(
                        ch, 'bar_' + order_book_id), stacklevel=0)

                to_subscribe.add('bar_' + order_book_id)
                if ch not in self._resamplers['bar_' + order_book_id]:
                    resampler = MinbarResampler(order_book_id, int(freq[:-1]))
                    self._resamplers['bar_' + order_book_id][resampler.channel] = resampler
            else:
                to_subscribe.add(ch)

        data = {
            "action": "subscribe",
            "channels": list(to_subscribe),
        }

        try:
            import websockets
            self._client.send(json_dumps(data))
        except (websockets.ConnectionClosed, connection_error):
            # 有重连，因此错误可以忽略
            pass

    def unsubscribe(self, channels):
        """取消订阅实时行情

        :param channels: 取消订阅的标的列表 分钟和tick分别以 bar_ 和tick_开头 以平安银行为例，
            unsubscribe('bar_000001.XSHE')  # 订阅分钟线的实时行情
            unsubscribe('tick_000001.XSHE')  # 订阅tick的实时行情

        """
        if self._closed:
            raise RuntimeError('this connection is closed.')

        channels = ensure_list_of_string(channels)
        for ch in channels:
            self._subscribed_by_user.discard(ch)
            if ch.startswith('bar_') and ch.endswith('m'):
                _, order_book_id, freq = ch.split('_')
                self._resamplers['bar_' + order_book_id].pop(ch, None)

        channels = [
            ch for ch in channels
            if ch not in self._subscribed_by_user and (ch not in self._resamplers or not self._resamplers[ch])
        ]

        data = {
            "action": "unsubscribe",
            "channels": channels,
        }

        try:
            import websockets
            self._client.send(json_dumps(data))
        except (websockets.ConnectionClosed, connection_error):
            pass

    def _reconnect(self):
        from websockets import WebSocketException
        while True:
            try:
                if self._closed:
                    return

                self._init_websocket_client()
                self.subscribe(list(self._subscribed_by_user))
                for resamplers in self._resamplers.values():
                    for resampler in resamplers.values():
                        resampler.reset()
                return
            except (socket.error, WebSocketException) as e:
                warnings.warn('web socket reconnect failed: {}, retry ..'.format(str(e)))
                time.sleep(0.5)

    def _listen(self):
        from websockets import WebSocketException, ConnectionClosed
        while not self._closed:
            try:
                data = self._client.recv(decode=False)
                data = json_loads(data)
                if data['action'] == 'feed':
                    ch = data['channel']
                    if ch in self._resamplers:
                        for resampler in self._resamplers[ch].values():
                            bar = resampler.enqueue(data)
                            if bar is not None:
                                yield bar
                    if ch in self._subscribed_by_user:
                        yield data
                elif data['action'] == 'subscribe_reply':
                    self._subscribed.update(data['subscribed'])
                elif data['action'] == 'unsubscribe_reply':
                    self._subscribed -= set(data['unsubscribed'])
            except ConnectionClosed as e:
                if e.rcvd and e.rcvd.code == 4003 and e.rcvd.reason == 'quota exceeded':
                    raise QuotaExceeded()
                else:
                    warnings.warn('web socket closed: {}, will reconnect'.format(str(e)), stacklevel=0)
                    self._client.close()
                    time.sleep(0.1)
                    self._reconnect()
            except (WebSocketException, connection_error) as e:
                warnings.warn("exception in : {}, will reconnect".format(str(e)), stacklevel=0)
                time.sleep(0.1)
                self._reconnect()

    def listen(self, handler=None):
        """获取实时行情。
        当 handler 参数为 None (默认情况) 时，此函数返回一个generator，用法如下：

            for msg in client.listen():
                process(msg)

        当 handler 不为 None 时，此函数会启动一个新的线程，在线程中执行：

            for msg in client.listen():
                handler(msg)

        注意，此时 handler 在另一个线程中执行；请小心处理线程之间的同步问题。此时函数不再阻塞，返回创建的线程对象。
        当调用 `client.close()` 时，线程会关闭。

        :returns: generator | threading.Thread
        """
        if self._closed:
            raise RuntimeError('this connection is closed.')

        if handler is None:
            return self._listen()

        def _process_msg():
            for msg in self._listen():
                if self._closed:
                    break
                handler(msg)

        thread = threading.Thread(target=_process_msg, daemon=True)
        thread.start()
        return thread
