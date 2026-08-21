from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import zstandard

from polymarket_btc.data_collection.market_data.models import EventSource, EventStream
from polymarket_btc.data_collection.market_data.sources.binance_futures_historical import (
    OPEN_INTEREST_HISTORY_LIMIT_DAYS,
    _WEIGHT_SAFE_CEILING,
    BinanceApiError,
    _adaptive_delay,
    _paginate_by_time,
    _rows_or_raise,
    fetch_and_store_historical_agg_trades,
    fetch_and_store_historical_klines,
    fetch_and_store_historical_mark_price,
    fetch_and_store_historical_open_interest_and_long_short,
)
from polymarket_btc.data_collection.market_data.storage import RawEventStorage


class _Headers:
    def __init__(self, used_weight: int | None) -> None:
        self._used_weight = used_weight

    def get(self, name: str) -> str | None:
        if name == "x-mbx-used-weight-1m" and self._used_weight is not None:
            return str(self._used_weight)
        return None


class _Response:
    def __init__(self, payload: object, *, used_weight: int | None = None) -> None:
        self._body = json.dumps(payload).encode()
        self.headers = _Headers(used_weight)

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


def _kline_row(open_ms: int, close_ms: int) -> list:
    return [
        open_ms, "64800.00", "64820.00", "64790.00", "64810.00",
        "5.5", close_ms, "356755.0", 200, "2.5", "162025.0", "0",
    ]


def _agg_trade_row(aggregate_id: int, ts_ms: int) -> dict:
    # "nq" is a real extra field historical aggTrades rows carry that live
    # rows don't -- confirmed live, must not break parse_rest_agg_trades reuse.
    return {
        "a": aggregate_id, "p": "64831.90", "q": "0.250",
        "f": aggregate_id, "l": aggregate_id, "T": ts_ms, "m": True, "nq": "16.09",
    }


def _funding_row(funding_time_ms: int) -> dict:
    return {
        "symbol": "BTCUSDT", "fundingTime": funding_time_ms,
        "fundingRate": "-0.00006300", "markPrice": "64800.00000000", "rateType": "",
    }


def _oi_row(ts_ms: int) -> dict:
    return {
        "symbol": "BTCUSDT", "sumOpenInterest": "78123.456",
        "sumOpenInterestValue": "5063400000.00", "CMCCirculatingSupply": "19800000.00",
        "timestamp": ts_ms,
    }


def _long_short_row(ts_ms: int) -> dict:
    return {
        "symbol": "BTCUSDT", "longAccount": "0.5521", "longShortRatio": "1.2325",
        "shortAccount": "0.4479", "timestamp": ts_ms,
    }


def _read_events(data_dir: Path) -> list[dict]:
    events = []
    for path in sorted((data_dir / "raw").rglob("*.jsonl.zst")):
        payload = zstandard.ZstdDecompressor().decompress(path.read_bytes())
        events.extend(json.loads(line) for line in payload.splitlines())
    return events


def _noop_log(_line: str) -> None:
    pass


class PaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_pages_until_end_ms_reached_or_page_empty(self) -> None:
        pages = [[1, 2], [3, 4], []]

        async def fetch_page(start: int, end: int, limit: int) -> tuple[list, int | None]:
            return pages.pop(0), None

        collected = []
        async for page in _paginate_by_time(
            fetch_page, start_ms=0, end_ms=1000, page_limit=10, advance=lambda rows: rows[-1] * 100,
        ):
            collected.append(page)
        self.assertEqual(collected, [[1, 2], [3, 4]])

    async def test_stops_defensively_if_advance_does_not_move_forward(self) -> None:
        calls = 0

        async def fetch_page(start: int, end: int, limit: int) -> tuple[list, int | None]:
            nonlocal calls
            calls += 1
            return [1, 2], None  # same page forever

        collected = []
        async for page in _paginate_by_time(
            fetch_page, start_ms=0, end_ms=1000, page_limit=10, advance=lambda rows: 0,
        ):
            collected.append(page)
        self.assertEqual(len(collected), 1)  # stopped after the first non-advancing page
        self.assertEqual(calls, 1)

    async def test_sleeps_by_the_delay_the_reported_used_weight_implies(self) -> None:
        """Confirms _paginate_by_time actually wires fetch_page's reported
        used_weight into the inter-page delay, not just that _adaptive_delay
        itself is correct in isolation."""
        pages = [[1], [2], []]

        async def fetch_page(start: int, end: int, limit: int) -> tuple[list, int | None]:
            return pages.pop(0), 2000  # comfortably close to the safety ceiling

        sleeps: list[float] = []
        with patch("asyncio.sleep", side_effect=lambda seconds: sleeps.append(seconds)):
            async for _ in _paginate_by_time(
                fetch_page, start_ms=0, end_ms=1000, page_limit=10, advance=lambda rows: rows[-1] * 100,
            ):
                pass
        self.assertEqual(sleeps, [_adaptive_delay(2000)] * len(sleeps))
        self.assertGreater(sleeps[0], 0)


class AdaptiveDelayTests(unittest.TestCase):
    def test_missing_weight_falls_back_to_the_original_flat_courtesy_delay(self) -> None:
        self.assertEqual(_adaptive_delay(None), 0.15)

    def test_low_weight_yields_a_small_delay(self) -> None:
        self.assertLess(_adaptive_delay(10), 0.01)

    def test_delay_increases_as_weight_approaches_the_safety_ceiling(self) -> None:
        low = _adaptive_delay(int(_WEIGHT_SAFE_CEILING * 0.25))
        high = _adaptive_delay(int(_WEIGHT_SAFE_CEILING * 0.9))
        self.assertLess(low, high)

    def test_at_or_over_the_safety_ceiling_backs_off_firmly(self) -> None:
        self.assertEqual(_adaptive_delay(_WEIGHT_SAFE_CEILING), 1.0)
        self.assertEqual(_adaptive_delay(_WEIGHT_SAFE_CEILING * 10), 1.0)

    def test_never_exceeds_the_ceiling_delay_below_the_ceiling(self) -> None:
        just_under = _adaptive_delay(_WEIGHT_SAFE_CEILING - 1)
        self.assertLess(just_under, 1.0)


class KlineFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_reuses_live_kline_parser_and_writes_real_events(self) -> None:
        rows = [_kline_row(1_000, 59_999), _kline_row(60_000, 119_999)]

        def fake_urlopen(request, timeout=None):
            return _Response(rows)

        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            sequence = iter(range(1, 1000))
            with patch("urllib.request.urlopen", side_effect=[fake_urlopen(None), _Response([])]):
                count = await fetch_and_store_historical_klines(
                    symbol="BTCUSDT", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000,
                    raw_storage=storage, next_sequence=lambda: next(sequence), log=_noop_log,
                )
            storage.close()
            self.assertEqual(count, 2)
            events = _read_events(Path(directory))
            self.assertEqual(len(events), 2)
            self.assertEqual(events[0]["source"], EventSource.BINANCE_FUTURES_KLINE.value)
            self.assertEqual(events[0]["stream"], EventStream.BINANCE_KLINE.value)
            self.assertEqual(events[0]["instrument"], "BTCUSDT")
            self.assertEqual(events[0]["payload"]["close"], "64810.00")


class RowsOrRaiseTests(unittest.TestCase):
    def test_list_passes_through_unchanged(self) -> None:
        rows = [{"a": 1}]
        self.assertIs(_rows_or_raise(rows), rows)

    def test_binance_error_shape_raises_with_its_message(self) -> None:
        # {"code": ..., "msg": ...} is Binance's real, confirmed-live error
        # shape (e.g. an invalid symbol) -- previously silently treated as
        # an empty page by `data if isinstance(data, list) else []`.
        with self.assertRaises(BinanceApiError) as ctx:
            _rows_or_raise({"code": -1121, "msg": "Invalid symbol."})
        self.assertIn("Invalid symbol.", str(ctx.exception))

    def test_other_unexpected_shape_still_raises_not_silently_empties(self) -> None:
        with self.assertRaises(BinanceApiError):
            _rows_or_raise(None)


class FetcherSurfacesBinanceErrorsTests(unittest.IsolatedAsyncioTestCase):
    """A rejected request (invalid symbol, bad parameter, ...) must not look
    identical to "genuinely no data in this range" -- both used to silently
    produce 0 events with nothing in the logs to tell them apart."""

    async def test_kline_fetch_raises_instead_of_silently_returning_zero(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            sequence = iter(range(1, 1000))
            with patch("urllib.request.urlopen", return_value=_Response({"code": -1121, "msg": "Invalid symbol."})):
                with self.assertRaises(BinanceApiError):
                    await fetch_and_store_historical_klines(
                        symbol="NOTAREALCOINUSDT", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000,
                        raw_storage=storage, next_sequence=lambda: next(sequence), log=_noop_log,
                    )
            storage.close()

    async def test_agg_trade_fetch_raises_instead_of_silently_returning_zero(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            sequence = iter(range(1, 1000))
            with patch("urllib.request.urlopen", return_value=_Response({"code": -1121, "msg": "Invalid symbol."})):
                with self.assertRaises(BinanceApiError):
                    await fetch_and_store_historical_agg_trades(
                        symbol="NOTAREALCOINUSDT", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000,
                        raw_storage=storage, next_sequence=lambda: next(sequence), log=_noop_log,
                    )
            storage.close()


class ProgressReportingTests(unittest.IsolatedAsyncioTestCase):
    async def test_on_progress_reports_incremental_fractions_and_ends_at_one(self) -> None:
        base_ms = 1_700_000_000_000
        rows = [_kline_row(base_ms, base_ms + 499_999), _kline_row(base_ms + 500_000, base_ms + 999_999)]

        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            sequence = iter(range(1, 1000))
            progress: list[float] = []
            with patch("urllib.request.urlopen", side_effect=[_Response([rows[0]]), _Response([rows[1]])]):
                await fetch_and_store_historical_klines(
                    symbol="BTCUSDT", start_ts_ns=base_ms * 1_000_000, end_ts_ns=(base_ms + 1_000_000) * 1_000_000,
                    raw_storage=storage, next_sequence=lambda: next(sequence), log=_noop_log,
                    on_progress=progress.append,
                )
            storage.close()
            self.assertTrue(progress)
            self.assertEqual(progress[-1], 1.0)
            self.assertTrue(all(0.0 <= f <= 1.0 for f in progress))
            self.assertEqual(progress, sorted(progress))  # monotonically non-decreasing

    async def test_two_phase_fetcher_keeps_progress_within_its_own_half(self) -> None:
        # mark price / funding: klines phase must stay within [0, 0.5],
        # funding phase within [0.5, 1.0] -- never overshoot into the other
        # phase's share, and finish at exactly 1.0 overall.
        kline_rows = [_kline_row(1_000, 59_999)]
        funding_rows = [_funding_row(30_000)]

        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            sequence = iter(range(1, 1000))
            progress: list[float] = []
            with patch(
                "urllib.request.urlopen",
                side_effect=[_Response(kline_rows), _Response([]), _Response(funding_rows), _Response([])],
            ):
                await fetch_and_store_historical_mark_price(
                    symbol="BTCUSDT", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000,
                    raw_storage=storage, next_sequence=lambda: next(sequence), log=_noop_log,
                    on_progress=progress.append,
                )
            storage.close()
            self.assertEqual(progress[-1], 1.0)
            self.assertTrue(all(0.0 <= f <= 1.0 for f in progress))


class AggTradeFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_reuses_live_agg_trade_parser_despite_extra_nq_field(self) -> None:
        rows = [_agg_trade_row(100, 1_000), _agg_trade_row(101, 2_000)]

        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            sequence = iter(range(1, 1000))
            with patch("urllib.request.urlopen", side_effect=[_Response(rows), _Response([])]):
                count = await fetch_and_store_historical_agg_trades(
                    symbol="BTCUSDT", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000,
                    raw_storage=storage, next_sequence=lambda: next(sequence), log=_noop_log,
                )
            storage.close()
            self.assertEqual(count, 2)
            events = _read_events(Path(directory))
            self.assertEqual(len(events), 2)
            self.assertEqual(events[0]["source"], EventSource.BINANCE_FUTURES_TRADE.value)
            self.assertEqual(events[0]["payload"]["aggregate_trade_id"], 100)


class MarkPriceFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_combines_mark_price_klines_and_funding_history(self) -> None:
        kline_rows = [_kline_row(1_000, 59_999)]
        funding_rows = [_funding_row(30_000)]

        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            sequence = iter(range(1, 1000))
            with patch(
                "urllib.request.urlopen",
                side_effect=[_Response(kline_rows), _Response([]), _Response(funding_rows), _Response([])],
            ):
                count = await fetch_and_store_historical_mark_price(
                    symbol="BTCUSDT", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000,
                    raw_storage=storage, next_sequence=lambda: next(sequence), log=_noop_log,
                )
            storage.close()
            self.assertEqual(count, 2)
            events = _read_events(Path(directory))
            self.assertEqual(len(events), 2)
            kline_events = [e for e in events if e["stream"] == EventStream.BINANCE_KLINE.value]
            funding_events = [e for e in events if e["stream"] == EventStream.BINANCE_FUTURES_MARK_PRICE.value]
            self.assertEqual(len(kline_events), 1)
            self.assertEqual(kline_events[0]["source"], EventSource.BINANCE_FUTURES_MARK_PRICE.value)
            self.assertEqual(len(funding_events), 1)
            # markPrice doubles as index_price -- fundingRate has no separate index price field.
            self.assertEqual(funding_events[0]["payload"]["mark_price"], funding_events[0]["payload"]["index_price"])
            self.assertEqual(funding_events[0]["payload"]["funding_rate"], "-0.00006300")


class OpenInterestLongShortFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_reads_the_historical_field_names_correctly(self) -> None:
        import time

        now_ms = time.time_ns() // 1_000_000
        recent_ts = now_ms - 3_600_000
        oi_rows = [_oi_row(recent_ts)]
        long_short_rows = [_long_short_row(recent_ts)]

        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            sequence = iter(range(1, 1000))
            with patch(
                "urllib.request.urlopen",
                side_effect=[_Response(oi_rows), _Response([]), _Response(long_short_rows), _Response([])],
            ):
                count = await fetch_and_store_historical_open_interest_and_long_short(
                    symbol="BTCUSDT", start_ts_ns=(now_ms - 7_200_000) * 1_000_000, end_ts_ns=now_ms * 1_000_000,
                    raw_storage=storage, next_sequence=lambda: next(sequence), log=_noop_log,
                )
            storage.close()
            self.assertEqual(count, 2)
            events = _read_events(Path(directory))
            oi_events = [e for e in events if e["stream"] == EventStream.BINANCE_FUTURES_OPEN_INTEREST.value]
            ls_events = [e for e in events if e["stream"] == EventStream.BINANCE_FUTURES_LONG_SHORT_RATIO.value]
            self.assertEqual(len(oi_events), 1)
            self.assertEqual(oi_events[0]["source"], EventSource.BINANCE_FUTURES_REST.value)
            self.assertEqual(oi_events[0]["payload"]["open_interest"], "78123.456")
            self.assertEqual(len(ls_events), 1)
            self.assertEqual(ls_events[0]["payload"]["long_short_ratio"], "1.2325")

    async def test_clamps_a_too_early_start_to_the_30_day_window_and_logs_it(self) -> None:
        import time

        now_ns = time.time_ns()
        too_early_start_ns = now_ns - 90 * 86_400 * 1_000_000_000  # 90 days ago
        logs: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            sequence = iter(range(1, 1000))
            with patch(
                "urllib.request.urlopen",
                side_effect=[_Response([]), _Response([])],
            ):
                await fetch_and_store_historical_open_interest_and_long_short(
                    symbol="BTCUSDT", start_ts_ns=too_early_start_ns, end_ts_ns=now_ns,
                    raw_storage=storage, next_sequence=lambda: next(sequence), log=logs.append,
                )
            storage.close()
            self.assertTrue(any(str(OPEN_INTEREST_HISTORY_LIMIT_DAYS) in line for line in logs))

    async def test_range_entirely_outside_the_30_day_window_fetches_nothing(self) -> None:
        import time

        now_ns = time.time_ns()
        ancient_start_ns = now_ns - 200 * 86_400 * 1_000_000_000
        ancient_end_ns = now_ns - 100 * 86_400 * 1_000_000_000
        logs: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            sequence = iter(range(1, 1000))
            with patch("urllib.request.urlopen", side_effect=AssertionError("should not fetch")):
                count = await fetch_and_store_historical_open_interest_and_long_short(
                    symbol="BTCUSDT", start_ts_ns=ancient_start_ns, end_ts_ns=ancient_end_ns,
                    raw_storage=storage, next_sequence=lambda: next(sequence), log=logs.append,
                )
            storage.close()
            self.assertEqual(count, 0)
            self.assertTrue(any("rien à récupérer" in line for line in logs))


if __name__ == "__main__":
    unittest.main()
