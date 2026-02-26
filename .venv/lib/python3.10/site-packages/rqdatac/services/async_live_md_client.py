import asyncio
import warnings
from collections import defaultdict

from rqdatac.decorators import export_as_api
from rqdatac.services.live_md_client import (
    ensure_list_of_string,
    instruments,
    MinbarResampler,
    json_dumps,
    get_client,
    json_loads,
)


@export_as_api
class AsyncLiveMarketDataClient:
    def __init__(self, ws_server_uri="wss://rqdata.ricequant.com/live_md"):
        self._ws_server_url = ws_server_uri
        self._lock = asyncio.Lock()

        self._subscribed = set()
        self._subscribed_by_user = set()
        self._resamplers = defaultdict(dict)
        self._info = None

        self._ws_connection = None
        self._closed = False

    async def _connect(self):
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "AsyncLiveMarketData requires websockets: run 'pip install websockets' first"
            )

        # 这是一个阻塞的操作
        _token = get_client().execute(
            "user.get_live_md_auth_token",
        )

        login_data = {"action": "auth_by_token", "token": _token}
        count = 3
        while count > 0:
            try:
                self._ws_connection = await websockets.connect(self._ws_server_url)
                await self._ws_connection.send(json_dumps(login_data))
                res = await self._ws_connection.recv()
                self._info = json_loads(res)
            except websockets.WebSocketException as e:
                count -= 1
                if count == 0:
                    raise e
                warnings.warn(f"Login failed: {e}, Retrying...")
                await asyncio.sleep(1)
            else:
                break

    async def _get_connection(self):
        async with self._lock:
            if not self._ws_connection:
                await self._connect()
        return self._ws_connection

    @property
    def info(self):
        return self._info

    def close(self):
        self._closed = True
        asyncio.create_task(self._ws_connection.close())

    async def subscribe(self, channels):
        """订阅实时行情

        :param channels: 订阅的标的列表 分钟和tick分别以 bar_ 和tick_开头 以平安银行为例，
            subscribe('bar_000001.XSHE')  # 订阅分钟线的实时行情
            subscribe('bar_AU2112_15m')   # 订阅15分钟线的实时行情
            subscribe('tick_000001.XSHE')  # 订阅tick的实时行情
            可以同时订阅多支标的 subscribe(['bar_000001.XSHE'， 'bar_000002.XSHE')
        """
        if self._closed:
            raise RuntimeError("this connection is closed.")

        channels = ensure_list_of_string(channels)
        to_subscribe = []
        for ch in channels:
            ob = ch.split("_")[1]
            if not instruments(ob):
                warnings.warn(
                    "invalid order_book_id: {}, channel {} ignored".format(ob, ch), stacklevel=0
                )
                continue

            self._subscribed_by_user.add(ch)

            if ch.startswith("bar_") and ch.endswith("m"):
                _, order_book_id, freq = ch.split("_")
                to_subscribe.append("bar_" + order_book_id)
                resampler = MinbarResampler(order_book_id, int(freq[:-1]))
                if resampler.channel not in self._resamplers["bar_" + order_book_id]:
                    self._resamplers["bar_" + order_book_id][resampler.channel] = resampler
            else:
                to_subscribe.append(ch)

        data = {
            "action": "subscribe",
            "channels": to_subscribe,
        }

        connection = await self._get_connection()
        await connection.send(json_dumps(data))

    async def unsubscribe(self, channels):
        """取消订阅实时行情

        :param channels: 取消订阅的标的列表 分钟和tick分别以 bar_ 和tick_开头 以平安银行为例，
            unsubscribe('bar_000001.XSHE')  # 订阅分钟线的实时行情
            unsubscribe('tick_000001.XSHE')  # 订阅tick的实时行情
        """

        if self._closed:
            raise RuntimeError("this connection is closed.")

        channels = ensure_list_of_string(channels)
        for ch in channels:
            self._subscribed_by_user.discard(ch)
            if ch.startswith("bar_") and ch.endswith("m"):
                _, order_book_id, freq = ch.split("_")
                self._resamplers["bar_" + order_book_id].pop(ch, None)

        channels = [
            ch
            for ch in channels
            if ch not in self._subscribed_by_user
            and (ch not in self._resamplers or not self._resamplers[ch])
        ]

        data = {
            "action": "unsubscribe",
            "channels": channels,
        }

        connection = await self._get_connection()
        await connection.send(json_dumps(data))

    async def listen(self):
        """获取实时行情。
        返回一个 AsyncGenerator，用法如下：

            async for msg in client.listen():
                print(msg)
        """
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "AsyncLiveMarketData requires websockets: run 'pip install websockets' first"
            )

        if self._closed:
            raise RuntimeError("this connection is closed.")
        await self._get_connection()
        while not self._closed:
            try:
                res = await self._ws_connection.recv()  # noqa
            except websockets.ConnectionClosed:
                warnings.warn("Connectio closed, reconnecting...")
                self._ws_connection = None
                await self._get_connection()
            else:
                if res:
                    data = json_loads(res)
                    if data["action"] == "feed":
                        ch = data["channel"]
                        if ch in self._resamplers:
                            for resampler in self._resamplers[ch].values():
                                bar = resampler.enqueue(data)
                                if bar is not None:
                                    yield bar
                        if ch in self._subscribed_by_user:
                            yield data
                    elif data["action"] == "subscribe_reply":
                        self._subscribed.update(data["subscribed"])
                    elif data["action"] == "unsubscribe_reply":
                        self._subscribed -= set(data["unsubscribed"])
