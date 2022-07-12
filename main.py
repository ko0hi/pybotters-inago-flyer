from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Generator, Union

if TYPE_CHECKING:
    from pybotters.store import DataStoreManager

import asyncio
from collections import deque

import loguru
import numpy as np
import pandas as pd
import pybotters
import pybotters.models.bitflyer


def check_side(side):
    assert side in ["BUY", "SELL"]


# 注文ヘルパー
async def market_order(client, symbol, side, size):
    res = await client.post(
        "/v1/me/sendchildorder",
        data={
            "product_code": symbol,
            "side": side,
            "size": f"{size:.8f}",
            "child_order_type": "MARKET",
        },
    )

    data = await res.json()

    if res.status != 200:
        raise RuntimeError(f"Invalid request: {data}")
    else:
        return data["child_order_acceptance_id"]


async def watch_execution(execution: pybotters.models.bitflyer.ChildOrders, order_id):
    with execution.watch() as stream:
        async for msg in stream:
            if (
                msg.operation == "insert"
                and msg.data["child_order_acceptance_id"] == order_id
                and msg.data["event_type"] == "EXECUTION"
            ):
                return msg.data


class AbstractPriceTrailer:
    """面倒になったのでここは箱だけ..."""


class BarBasedPriceTrailer(AbstractPriceTrailer):
    """足が確定するたびに最後の足のclose ± marginのところにstopを置き直す（ストップ値が悪化（？）する場合は更新なし）"""

    def __init__(
        self,
        price: int,
        side: str,
        bar: AbstractTimeBar,
        margin: int,
        logger=loguru.logger,
    ):
        check_side(side)
        self._price = price
        self._side = side
        self._bar = bar
        self._margin = margin
        self._logger = logger
        self._task = asyncio.create_task(self.auto_trail())

    def __del__(self):
        # gcの機嫌次第でいつ呼ばれるか（はたまた本当に呼ばれるのか）わからないが一応オブジェクト破棄時に
        # Taskをキャンセルをするようにする。
        self.cancel()

    async def auto_trail(self):
        while True:
            # 最新足確定まで待機
            bar = await self._bar.get_bar_at_settled()
            last_close = bar[-1, 3]

            if self._side == "BUY":
                new_price = last_close - self._margin
                if new_price > self._price:
                    self._price = new_price
            else:
                new_price = last_close + self._margin
                if new_price < self._price:
                    self._price = new_price

            self._logger.debug(
                f"[TRAIL] {self._price:.0f} (last_close={last_close:.0f})"
            )

    def cancel(self, msg=None):
        return self._task.cancel(msg)

    @property
    def price(self):
        return self._price


class StreamArray:
    """queue-likeなnumpy array"""

    def __init__(self, shape, array=None):
        self._a = np.full(shape, np.nan)
        if array is not None:
            self.init(array)

    def __repr__(self):
        return self._a.__repr__()

    def __getitem__(self, *args):
        return self._a.__getitem__(*args)

    def __len__(self):
        return self._a.__len__()

    def append(self, x):
        def shift(arr: np.ndarray, num: int, fill_value=0) -> np.ndarray:
            result = np.empty_like(arr)

            if num > 0:
                result[:num] = fill_value
                result[num:] = arr[:-num]
            elif num < 0:
                result[num:] = fill_value
                result[:num] = arr[-num:]
            else:
                result[:] = arr

            return result

        self._a = shift(self._a, -1)
        self._a[-1] = x

    def init(self, array):
        if self._a.shape[0] <= array.shape[0]:
            array = array[-self._a.shape[0] :]
            self._a = array
        else:
            self._a[-array.shape[0] :] = array

    @property
    def a(self):
        return self._a


class AbstractTimeBar:
    """Time-bar用の抽象クラス。

    storeには約定情報を取得する``DataStore``を与える（例: bitFlyer -> ``pybotters.models.bitflyer.Executions``)

    """

    def __init__(
        self,
        store: "pybotters.store.DataStore",
        unit_seconds: int,
        maxlen: int = 9999,
        callbacks: list[Callable[[AbstractTimeBar], None]] = (),
    ):
        self._store = store
        self._seconds = unit_seconds
        self._rule = f"{unit_seconds}S"
        self._bar = StreamArray((maxlen, 7))
        self._cur_bar = np.zeros(7)
        self._timestamp = deque(maxlen=maxlen)

        # callback
        self._callbacks = callbacks or []
        self._d = {}

        # 確定足取得用
        self._queue = asyncio.Queue(1)

        self._task = asyncio.create_task(self.auto_update())

    async def init(self, executions: list[dict]):
        [self.update(e) for e in executions]

    def new_bar(
        self,
        price: Union[int, float],
        volume: float,
        side: str,
        timestamp: pd.Timestamp = None,
    ):
        """最新足の確定"""
        check_side(side)

        if side == "BUY":
            buy_volume, sell_volume = volume, 0
        else:
            buy_volume, sell_volume = 0, volume

        self._bar.append(self._cur_bar)
        self._cur_bar = np.array(
            [price, price, price, price, volume, buy_volume, sell_volume]
        )
        if timestamp is None:
            timestamp = pd.Timestamp.utcnow().floor(self._rule)
        self._timestamp.append(timestamp)

        # 確定した最新barを格納
        try:
            self._queue.put_nowait(self._bar)
        except asyncio.QueueFull:
            self._queue.get_nowait()
            self._queue.put_nowait(self._bar)

    def update_cur_bar(self, price: Union[int, float], volume: float, side: str):
        """未確定足の更新"""
        # high
        self._cur_bar[1] = max(price, self._cur_bar[1])
        # low
        self._cur_bar[2] = min(price, self._cur_bar[2])
        # close
        self._cur_bar[3] = price
        # volume
        self._cur_bar[4] += volume
        if side == "BUY":
            self._cur_bar[5] += volume
        elif side == "SELL":
            self._cur_bar[6] += volume
        else:
            raise RuntimeError(f"Unsupported: {side}")

    def update(self, e: pybotters.store.StoreChange):
        """要オーバーライド"""
        raise NotImplementedError

    async def auto_update(self):
        """約定情報の受信タスク"""
        async for e in self.execution_stream():
            self.update(e)

            # 約定情報を受信するたびにcallback
            for cb in self._callbacks:
                self.d.update(cb(self))

    async def execution_stream(self):
        """pybottersのwatchを使って約定情報を随時generateする。"""
        with self._store.watch() as stream:
            async for msg in stream:
                if msg.operation == "insert":
                    yield msg.data

    async def get_bar_at_settled(self) -> np.ndarray:
        """``await bar.get_bar_at_settled()``で最新足確定時に取得できる。"""
        return await self._queue.get()

    @property
    def store(self):
        return self._store

    @property
    def bar(self):
        return self._bar

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def cur_bar(self):
        return self._cur_bar

    @property
    def open(self):
        return self._bar[:, 0]

    @property
    def o(self):
        return self._bar[-1, 0]

    @property
    def high(self):
        return self._bar[:, 1]

    @property
    def h(self):
        return self._bar[-1, 1]

    @property
    def low(self):
        return self._bar[:, 2]

    @property
    def l(self):
        return self._bar[-1, 2]

    @property
    def close(self):
        return self._bar[:, 3]

    @property
    def c(self):
        return self._bar[-1, 3]

    @property
    def volume(self):
        return self._bar[:, 4]

    @property
    def v(self):
        return self._bar[-1, 4]

    @property
    def buy_volume(self):
        return self._bar[:, 5]

    @property
    def bv(self):
        return self._bar[-1, 5]

    @property
    def sell_volume(self):
        return self._bar[:, 6]

    @property
    def sv(self):
        return self._bar[-1, 6]

    @property
    def d(self):
        return self._d


class BitflyerTimeBar(AbstractTimeBar):
    """bitFlyer用のTimeBar実装"""

    def __init__(
        self,
        store: pybotters.models.bitflyer.Executions,
        unit_seconds: int,
        maxlen: int = 9999,
        callbacks: list[Callable[[BitflyerTimeBar], None]] = (),
    ):
        super(BitflyerTimeBar, self).__init__(store, unit_seconds, maxlen, callbacks)

    def update(self, e: dict):
        cur_ts = pd.to_datetime(e["exec_date"]).floor(self._rule)

        if len(self._timestamp) == 0:
            last_ts = None
        else:
            last_ts = self._timestamp[-1].floor(f"{self._rule}")

        if last_ts is None or last_ts != cur_ts:
            self.new_bar(e["price"], e["size"], e["side"], cur_ts)
        else:
            self.update_cur_bar(e["price"], e["size"], e["side"])


class AbstractInagoBot:
    """イナゴBOT用の抽象クラス（仮）"""

    def __init__(
        self,
        client: pybotters.Client,
        is_inago_start_fn: Callable[[AbstractInagoBot], tuple[bool, str]] = None,
        is_inago_end_fn: Callable[[AbstractInagoBot], bool] = None,
        off_loop_interval: float = 0.5,
        logger=loguru.logger,
    ):
        self._client = client
        self._is_inago_start_fn = is_inago_start_fn
        self._is_inago_end_fn = is_inago_end_fn
        self._off_loop_interval = off_loop_interval
        self._logger = logger
        self._side = None

    # 要オーバーライド
    async def inago_stream(self) -> Generator[dict]:
        """ "イナゴ"（主に約定情報になると思う）を流す"""
        raise NotImplementedError

    async def on_inago(self, inago):
        """各イナゴに対する処理"""
        pass

    async def is_inago_start(self) -> tuple[bool, str]:
        """イナゴ到来判定"""
        if self._is_inago_start_fn is None:
            raise NotImplementedError
        if asyncio.iscoroutinefunction(self._is_inago_start_fn):
            return await self._is_inago_start_fn(self)
        else:
            return self._is_inago_start_fn(self)

    async def is_inago_end(self) -> bool:
        """イナゴ終了判定"""
        if self._is_inago_end_fn is None:
            raise NotImplementedError
        if asyncio.iscoroutinefunction(self._is_inago_end_fn):
            return await self._is_inago_end_fn(self)
        else:
            return self._is_inago_end_fn(self)

    async def loop(self):
        """イナゴBOTのメインループ

        - off_loop(): イナゴ待機ループ（検知）
        - on_loop(): イナゴ処理ループ（トレード）

        この二つを繰り返す。

        - *_begin(), *_end(): 各関数の前後で呼ばれるhook

        """
        while True:
            self._logger.debug("BEGIN LOOP")
            await self.on_loop_begin()
            self._logger.debug("BEGIN OFF LOOP")
            await self.off_loop()
            self._logger.debug(f"END OFF LOOP")

            self._logger.debug(f"BEGIN ON LOOP: {self._side}")
            await self.on_loop()
            self._logger.debug("END ON LOOP")
            await self.on_loop_end()
            self._logger.debug("END LOOP")

    async def off_loop(self):
        """イナゴオフ時のループ＝イナゴ検知"""
        await self.on_off_loop_begin()
        while True:
            is_start, side = await self.is_inago_start()
            if is_start:
                self._side = side
                break
            await asyncio.sleep(self._off_loop_interval)
        await self.on_off_loop_end()

    async def on_loop(self):
        """イナゴオン時のループ＝トレード"""
        await self.on_on_loop_begin()
        async for inago in self.inago_stream():
            await self.on_inago_begin(inago)
            await self.on_inago(inago)
            is_end = await self.is_inago_end()
            if is_end:
                self._side = None
                break
            await self.on_inago_end(inago)
        await self.on_on_loop_end()

    async def on_loop_begin(self):
        pass

    async def on_loop_end(self):
        pass

    async def on_off_loop_begin(self):
        pass

    async def on_off_loop_end(self):
        pass

    async def on_on_loop_begin(self):
        pass

    async def on_on_loop_end(self):
        pass

    async def on_inago_begin(self, inago):
        pass

    async def on_inago_end(self, inago):
        pass

    @property
    def client(self):
        return self._client

    @property
    def side(self):
        return self._side


class BitflyerInagoBot(AbstractInagoBot):
    """今回のイナゴBOTのメイン実装。単純な成行IN/成行OUTのイナゴトレード。

    - bar_l: イナゴ終了用の長時間足（１分）
    - bar_s: イナゴ検知用の短時間足（数秒）
    - lower_threshold, upper_threshold: イナゴ検知用の閾値
    - entry_patience_seconds, entry_price_change: 騙し回避用のパラメター
    - trail_margin: トレイル決済のパラメター

    """

    def __init__(
        self,
        client: pybotters.Client,
        store: pybotters.bitFlyerDataStore,
        bar_l: BitflyerTimeBar,
        bar_s: BitflyerTimeBar,
        *,
        lower_threshold: float,
        upper_threshold: float,
        entry_patience_seconds: int,
        entry_price_change: int,
        trail_margin: int,
        symbol: str = "FX_BTC_JPY",
        size: float = 0.01,
        side: str = "BOTH",
        **kwargs,
    ):
        super(BitflyerInagoBot, self).__init__(client, **kwargs)
        self._store = store
        self._bar_l = bar_l
        self._bar_s = bar_s
        self._lower_threshold = lower_threshold
        self._upper_threshold = upper_threshold
        self._entry_patience_seconds = entry_patience_seconds
        self._entry_price_change = entry_price_change
        self._trail_margin = trail_margin
        self._symbol = symbol
        self._size = size
        self._entry_side = side
        self._entry_order_info = None
        self._exit_order_info = None

        self._asks, self._bids = None, None
        asyncio.create_task(self.auto_ask_bid_update())

    async def auto_ask_bid_update(self):
        """板情報の自動更新タスク"""
        while True:
            await self._store.board.wait()
            self._asks, self._bids = self._store.board.sorted().values()

    async def on_loop_end(self):
        """トレードログ"""
        assert self._entry_order_info is not None
        assert self._exit_order_info is not None
        pnl = self._exit_order_info["price"] - self._entry_order_info["price"]
        if self._entry_order_info["side"] == "SELL":
            pnl *= -1
        pnl *= self._entry_order_info["size"]
        self._logger.debug(f"[LOOP FINISH] pnl={pnl}")
        self._entry_order_info = None
        self._exit_order_info = None

    async def on_inago(self, inago):
        self._logger.debug(f"[ON INAGO] {self._bar_s.d} {inago}")

    async def inago_stream(self):
        with self._store.executions.watch() as stream:
            async for msg in stream:
                yield msg.data

    async def is_inago_start(self) -> tuple[bool, str]:
        """イナゴ検知ロジック。

        ２段階で検知する。

        （１）閾値判定：短期足（秒足）でのボリュームが閾値をクリア
        （２）経過判定：n秒（``self._entry_patience_seconds``）後にイナゴ方向に値動き（``self._entry_price_change``）があるか否か

        """
        d = self._bar_s.d

        if len(d) == 0:
            self._logger.warning(f"[INFORMATION IS EMPTY] {d}")
            return False, None

        self._logger.debug(f"[WAITING INAGO] {d}")

        async def _primary_check():
            """閾値判定"""
            if (
                self._entry_side in ("BUY", "BOTH")
                and d["sv_log"]
                < self._lower_threshold
                < d["bv_log"]
                < self._upper_threshold
            ):
                self._logger.debug("[PRIMARY CHECK] YES BUY")
                return "BUY"
            elif (
                self._entry_side in ("SELL", "BOTH")
                and d["bv_log"]
                < self._lower_threshold
                < d["sv_log"]
                < self._upper_threshold
            ):
                self._logger.debug("[PRIMARY CHECK] YES SELL")
                return "SELL"
            else:
                return None

        async def _secondary_check(s):
            """時間経過判定"""

            # 仲値を値動きの参照値に使う
            mark_price_start = int(self.mid)
            self._logger.debug(f"[SECONDARY CHECK] mark_price={mark_price_start}")

            while True:
                mark_price = int(self.mid)
                price_change = mark_price - mark_price_start
                if s == "SELL":
                    price_change *= -1

                print(
                    f"\r\033[31m>>> [SECONDARY CHECK] {mark_price_start}/{mark_price}/{price_change:+.0f}\033[0m",
                    end="",
                )

                if price_change > self._entry_price_change:
                    # イナゴ方向への値動きがあった
                    break

                await asyncio.sleep(0.1)

        # 閾値判定
        side = await _primary_check()

        if side:
            try:
                # 経過判定
                await asyncio.wait_for(
                    _secondary_check(side), timeout=self._entry_patience_seconds
                )
                # carriage return調整してるだけ
                print()
                # イナゴ検知
                return True, side
            except asyncio.TimeoutError as e:
                # carriage return調整してるだけ
                print()
                # 指定秒数以内に値動きがみられなかったのでスルー
                self._logger.debug(f"[CANCEL] mark_price={self.mid}")
                return False, None
        else:
            return False, None

    async def on_loop(self):
        """決済ロジック

        - 色々とhookを用意したものの、ロジック的に当てはめられなかったのでon_loop丸ごとオーバーライドしている（爆）
        - 「約定情報を参照して決済注文を出す」といったロジックであれば以下のように分けて実装できると思う（元々はそう考えていた）
            - ``on_on_loop_begin``で新規注文
            - ``is_inago_endo``で終了判定
            - ``on_on_loop_end``で決済注文

        """

        # 新規注文
        order_id = await market_order(self.client, self._symbol, self.side, self._size)
        self._entry_order_info = await watch_execution(
            self.store.childorderevents, order_id
        )
        self._logger.debug(f"[ENTRY ORDER] {self._entry_order_info}")

        entry_price = self._entry_order_info["price"]

        # 建値±``_trail_margin`` を初期ストップ値としてトレイルスタート
        if self.side == "BUY":
            stop_price = entry_price - self._trail_margin
        else:
            stop_price = entry_price + self._trail_margin

        trailer = BarBasedPriceTrailer(
            stop_price, self.side, self._bar_l, self._trail_margin, self._logger
        )

        self._logger.debug(f"[TRAIL START] entry={entry_price} stop={stop_price}")

        while True:
            await asyncio.sleep(0.1)

            # 最良気配値がストップ値を割ったら決済
            # ストップ値はtrailerが長期足（e.g., １分足）の確定毎に更新

            if self.side == "BUY":
                mark_price = self.best_bid
                pnl = mark_price - entry_price
                if mark_price <= trailer.price:
                    break
            else:
                mark_price = self.best_ask
                pnl = entry_price - mark_price
                if mark_price >= trailer.price:
                    break

            print(
                f"\r\033[31m>>> [TRAILING] entry={entry_price:.0f} stop={trailer.price:.0f} mark={mark_price:.0f} pnl={pnl:+.0f}\033[0m",
                end="",
            )

        # trailタスクが回り続けてしまうので明示的にキャンセルする
        # asyncio.Taskはオブジェクトがスコープを外れて破壊されてもキャンセルされない
        trailer.cancel()

        side = "SELL" if self._entry_order_info["side"] == "BUY" else "BUY"
        order_id = await market_order(self.client, self._symbol, side, self._size)
        self._exit_order_info = await watch_execution(
            self.store.childorderevents, order_id
        )
        self._logger.debug(f"[EXIT ORDER] {self._exit_order_info}")

    @property
    def store(self):
        return self._store

    @property
    def best_ask(self):
        if self._asks is None:
            return -1
        return self._asks[0]["price"]

    @property
    def best_bid(self):
        if self._bids is None:
            return -1
        return self._bids[0]["price"]

    @property
    def mid(self):
        return (self.best_ask + self.best_bid) / 2


async def main(args):
    logger = loguru.logger
    logger.add("log.txt", retention=3, rotation="10MB")

    async with pybotters.Client(
        args.api_key, base_url="https://api.bitflyer.com"
    ) as client:
        store = pybotters.bitFlyerDataStore()

        # time bar
        def log_volume(bar: AbstractTimeBar):
            """最新足のbuy/sell volumeのログを計算するcallback"""
            d = dict()
            d["bv_log"] = np.log1p(bar.bv)
            d["sv_log"] = np.log1p(bar.sv)
            return d

        bar_l = BitflyerTimeBar(
            unit_seconds=args.bar_unit_seconds_long,
            store=store.executions,
            maxlen=args.bar_maxlen,
            callbacks=[log_volume],
        )
        bar_s = BitflyerTimeBar(
            unit_seconds=args.bar_unit_seconds_short,
            store=store.executions,
            maxlen=args.bar_maxlen,
            callbacks=[log_volume],
        )

        # time barを約定履歴で初期化
        resp = await client.get(
            "/v1/getexecutions", params={"producet_code": args.symbol, "count": 500}
        )
        data = await resp.json()
        await bar_l.init(data[::-1])
        await bar_s.init(data[::-1])

        # web socketに接続
        await client.ws_connect(
            "wss://ws.lightstream.bitflyer.com/json-rpc",
            send_json=[
                {
                    "method": "subscribe",
                    "params": {"channel": f"lightning_board_snapshot_{args.symbol}"},
                    "id": 1,
                },
                {
                    "method": "subscribe",
                    "params": {"channel": f"lightning_board_{args.symbol}"},
                    "id": 2,
                },
                {
                    "method": "subscribe",
                    "params": {"channel": f"lightning_executions_{args.symbol}"},
                    "id": 3,
                },
                {
                    "method": "subscribe",
                    "params": {"channel": "child_order_events"},
                    "id": 4,
                },
            ],
            hdlr_json=store.onmessage,
        )
        while not all([len(w) for w in [store.board, store.executions]]):
            logger.debug("[WAITING SOCKET RESPONSE]")
            await store.wait()

        await BitflyerInagoBot(
            client,
            store,
            bar_l,
            bar_s,
            lower_threshold=args.lower_threshold,
            upper_threshold=args.upper_threshold,
            entry_patience_seconds=args.entry_patience_seconds,
            entry_price_change=args.entry_price_change,
            trail_margin=args.trail_margin,
            symbol=args.symbol,
            size=args.size,
            side=args.side,
            logger=logger,
        ).loop()


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--api_key", required=True, help="apiキーが入ったJSONファイル")
    parser.add_argument("--symbol", default="FX_BTC_JPY", help="取引通過")
    parser.add_argument(
        "--side", default="BOTH", choices=["BUY", "SELL", "BOTH"], help="エントリーサイド"
    )
    parser.add_argument("--size", default=0.01, type=float, help="注文サイズ")
    parser.add_argument("--bar_unit_seconds_long", default=60, type=int, help="長期足")
    parser.add_argument("--bar_unit_seconds_short", default=5, type=int, help="短期足")
    parser.add_argument("--bar_maxlen", default=999, type=int, help="足の最大履歴")
    parser.add_argument(
        "--lower_threshold",
        default=1,
        type=float,
        help="短期足のボリューム（log）がこの閾値以上であればエントリー待機",
    )
    parser.add_argument(
        "--upper_threshold",
        default=float("inf"),
        type=float,
        help="短期足のボリューム（log）がこの閾値以下であればエントリー待機",
    )
    parser.add_argument(
        "--entry_patience_seconds", default=3, type=int, help="閾値クリア後の経過観察時間"
    )
    parser.add_argument(
        "--entry_price_change", default=500, type=int, help="経過観察後の価格変動がこの閾値以上であればエントリー"
    )
    parser.add_argument("--trail_margin", default=2000, type=int, help="トレイル値幅")
    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        pass
