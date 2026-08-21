"""Historical data access for the backtest engine: which collection runs are
usable for a given strategy, what date range they actually cover, and how to
read ordered, loosely-shaped records for a data-catalog key out of either
storage shape a completed run can be in.

Two incompatible storage shapes exist (see runs.py's `_run`/`_run_access`):

- `mode="collect"` runs have `data_dir/dataset.parquet`, one row per
  `MarketDataSnapshot` -- read via `snapshot_from_parquet_row`.
- `mode="access"` runs (bulk historical REST fetch -- the only mode that can
  cheaply cover a long backtest period) have no `dataset.parquet`, only raw
  event segments under `data_dir/raw/**`, read via `RawStreamChain`
  (`market_data/replay.py`).

Every record this module hands back is a plain dict using the same loose,
defensively-read key names (`price`/`timestamp`/`quantity`-style) the real
concepts already sitting in `concepts/` expect from `context.data[key]` --
see `concepts/fvg.py:221` and `docs/nouveau_concept_prompt.md`'s own worked
example. Timestamps are float seconds (unix epoch), matching what those
concepts' own `_TIME_KEYS`/`_MS_THRESHOLD` parsing already handles.

Scope: only the sources actually usable for a real backtest are supported --
`binance_futures_trade`, `binance_futures_kline`, `binance_futures_mark_price`
(access-mode-capable, so usable for long periods) plus `chainlink` and
`binance_spot` (collect-mode only, since neither has a historical REST
archive -- see gateway.py's own `SourceInfo` catalog). A key outside this set
(a plugin id, or a source with no known extractor) simply yields no records
rather than raising -- same "skip what isn't understood" philosophy as
`discover_concepts`.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

from ..market_data.models import (
    BinanceAggTradePayload,
    BinanceFuturesMarkPricePayload,
    BinanceKlinePayload,
    EventSource,
)
from ..market_data.replay import RawStreamChain
from ..market_data.storage import snapshot_from_parquet_row

# ISO strings produced by runs.py's _iso() sort correctly as plain strings:
# a timestamp with zero microseconds omits them (isoformat's own behaviour),
# and "+" (timezone sign) is ASCII-less-than ".", so a whole-second value
# always compares as strictly earlier than any positive-microsecond value at
# the same second -- comparisons below rely on this rather than reparsing.


def _merge_intervals(intervals: list[tuple[str, str]]) -> list[tuple[str, str]]:
    merged: list[list[str]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


def _intersect_pair(a: list[tuple[str, str]], b: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Intersects two lists of non-overlapping, sorted (start, end) ISO
    interval tuples -- both are already merge_intervals' own output shape."""
    result: list[tuple[str, str]] = []
    i = j = 0
    while i < len(a) and j < len(b):
        start = max(a[i][0], b[j][0])
        end = min(a[i][1], b[j][1])
        if start < end:
            result.append((start, end))
        if a[i][1] < b[j][1]:
            i += 1
        else:
            j += 1
    return result


def key_coverage(key: str, manifests: list[dict]) -> list[tuple[str, str]]:
    """Every interval `key` genuinely has data in, unioned across every
    manifest that collected it -- reads each manifest's own per-key
    `source_coverage` (runs.py's export_run, the *actual* range written, not
    the range that was merely requested). A manifest written before
    source_coverage existed falls back to its own run-level start_ts_utc/
    end_ts_utc -- still correct for a single-source run, just not as precise
    as a real per-key range for a multi-source one until re-collected."""
    intervals: list[tuple[str, str]] = []
    for manifest in manifests:
        available = set(manifest.get("sources") or []) | set(manifest.get("plugins") or [])
        if key not in available:
            continue
        per_key = (manifest.get("source_coverage") or {}).get(key)
        if per_key is not None:
            start, end = per_key.get("start_ts_utc"), per_key.get("end_ts_utc")
        else:
            start, end = manifest.get("start_ts_utc"), manifest.get("end_ts_utc")
        if start is not None:
            intervals.append((start, end or start))
    return _merge_intervals(intervals)


def combined_coverage(required_keys: set[str], manifests: list[dict]) -> list[tuple[str, str]]:
    """The intersection of every required key's own `key_coverage` -- the
    only time ranges where *every* required key genuinely has data, i.e.
    the honest answer to "when can this strategy actually be backtested."
    Empty the moment any required key has no coverage anywhere, exactly the
    "don't show a date unless every requirement is met" rule asked for."""
    if not required_keys:
        return []
    per_key = [key_coverage(key, manifests) for key in required_keys]
    if any(not intervals for intervals in per_key):
        return []
    combined = per_key[0]
    for intervals in per_key[1:]:
        combined = _intersect_pair(combined, intervals)
        if not combined:
            return []
    return combined


def _interval_span_seconds(intervals: list[tuple[str, str]]) -> float:
    if not intervals:
        return -1.0
    return sum(
        datetime.fromisoformat(end).timestamp() - datetime.fromisoformat(start).timestamp()
        for start, end in intervals
    )


def narrowest_key(required_keys: set[str], manifests: list[dict]) -> str | None:
    """The required key whose own total coverage spans the least time (or
    has none at all) -- the diagnostic reason a combined range is short or
    empty, so the UI can name which key to go collect more of instead of
    the user having to guess which of several requirements is the bottleneck."""
    if not required_keys:
        return None
    return min(required_keys, key=lambda key: _interval_span_seconds(key_coverage(key, manifests)))


PRICE_PATH_BASE_KEYS = ("binance_futures_trade", "binance_futures_kline", "binance_futures_mark_price")
PRICE_PATH_LABEL = "prix (trades / bougies / mark price)"


def price_path_coverage(instrument_asset: str, manifests: list[dict]) -> list[tuple[str, str]]:
    """The union of whichever price-capable key (trades, klines, or mark
    price -- run_backtest's own fallback chain, tried in exactly that
    priority order) has been collected for `instrument_asset`. A strategy
    needs *at least one* of these to price fills/stop-loss/take-profit
    against, regardless of what its own concepts/microsystems directly
    declare -- a strategy built purely on funding-rate or open-interest
    data still needs something to simulate the trade against. Matches the
    compound-key convention runs.py's `_catalog_key_for_raw_path` uses: the
    bare key for BTC, `"key:ASSET"` otherwise."""
    suffix = "" if instrument_asset == "BTC" else f":{instrument_asset}"
    intervals: list[tuple[str, str]] = []
    for base_key in PRICE_PATH_BASE_KEYS:
        intervals.extend(key_coverage(f"{base_key}{suffix}", manifests))
    return _merge_intervals(intervals)


def backtest_coverage(
    required_keys: set[str], instrument_asset: str, manifests: list[dict],
) -> tuple[list[tuple[str, str]], str | None]:
    """The real, final answer to "when can this strategy be backtested":
    every concept/microsystem-required key's own coverage, intersected with
    price_path_coverage for the resolved instrument -- eligibility must
    never promise a range `run_backtest`'s own price-path fallback would
    then reject for lacking exactly that (the concrete bug this closes: a
    strategy whose concepts never directly declare trades/klines/mark price
    -- e.g. one built only on open interest -- used to show a range as
    fully backtestable purely because its own declared keys were covered,
    letting the user start a backtest that immediately failed with "no
    price data available").

    Also returns the single limiting factor's label -- the narrowest
    required concept key, or PRICE_PATH_LABEL if price data is actually the
    bottleneck instead -- so the UI's existing "limité par : X" hint stays
    accurate without needing a separate code path for this case."""
    price_coverage = price_path_coverage(instrument_asset, manifests)
    coverage = _intersect_pair(combined_coverage(required_keys, manifests), price_coverage)

    concept_narrowest = narrowest_key(required_keys, manifests)
    concept_span = (
        _interval_span_seconds(key_coverage(concept_narrowest, manifests))
        if concept_narrowest is not None else float("inf")
    )
    price_span = _interval_span_seconds(price_coverage)
    limiting = PRICE_PATH_LABEL if price_span < concept_span else concept_narrowest
    return coverage, limiting


_ACCESS_EXTRACTORS = {
    BinanceAggTradePayload: lambda p, ts_ns: {
        "price": float(p.price), "quantity": float(p.quantity),
        "timestamp": ts_ns / 1e9, "taker_side": p.taker_side.value,
    },
    BinanceKlinePayload: lambda p, ts_ns: {
        "open": float(p.open), "high": float(p.high), "low": float(p.low), "close": float(p.close),
        "volume": float(p.base_volume), "timestamp": p.close_time_ns / 1e9,
        "open_time": p.open_time_ns / 1e9, "close_time": p.close_time_ns / 1e9, "is_closed": p.is_closed,
    },
    BinanceFuturesMarkPricePayload: lambda p, ts_ns: {
        "mark_price": float(p.mark_price), "index_price": float(p.index_price),
        "funding_rate": float(p.funding_rate), "timestamp": ts_ns / 1e9,
    },
}


def _read_access_records(
    key: str, start_ts_ns: int, end_ts_ns: int, data_dir: Path, instrument: str,
) -> list[dict[str, object]]:
    base_key = key.partition(":")[0]
    try:
        source = EventSource(base_key)
    except ValueError:
        return []
    raw_root = data_dir / "raw"
    if not raw_root.is_dir():
        return []
    manifests = [
        path for path in raw_root.rglob("*.manifest.json")
        if f"source={source.value}" in path.parts and f"instrument={instrument}" in path.parts
    ]
    if not manifests:
        return []
    records: list[dict[str, object]] = []
    for event in RawStreamChain(manifests):
        ts_ns = event.source_timestamp_ns or event.received_wall_timestamp_ns
        if not (start_ts_ns <= ts_ns <= end_ts_ns):
            continue
        extractor = _ACCESS_EXTRACTORS.get(type(event.payload))
        if extractor is None:
            continue
        records.append(extractor(event.payload, ts_ns))
    return records


_COLLECT_EXTRACTORS = {
    "chainlink": lambda snap: (
        [{"price": float(snap.chainlink.price), "timestamp": snap.snapshot_timestamp_ns / 1e9}]
        if snap.chainlink.price is not None else []
    ),
    "binance_spot": lambda snap: (
        [{"price": float(snap.binance.last_price), "timestamp": snap.snapshot_timestamp_ns / 1e9}]
        if snap.binance.last_price is not None else []
    ),
}


def _read_collect_records(key: str, start_ts_ns: int, end_ts_ns: int, data_dir: Path) -> list[dict[str, object]]:
    dataset_path = data_dir / "dataset.parquet"
    if not dataset_path.is_file():
        return []
    extractor = _COLLECT_EXTRACTORS.get(key)
    if extractor is None:
        return []
    table = pq.read_table(dataset_path)
    records: list[dict[str, object]] = []
    for row in table.to_pylist():
        ts_ns = row["snapshot_timestamp_ns"]
        if not (start_ts_ns <= ts_ns <= end_ts_ns):
            continue
        snapshot = snapshot_from_parquet_row(row)
        records.extend(extractor(snapshot))
    return records


def read_records(
    key: str, start_ts_ns: int, end_ts_ns: int, manifests: list[dict], *, instrument: str,
) -> list[dict[str, object]]:
    """Every record for `key` in [start_ts_ns, end_ts_ns], chronologically
    merged across every manifest that has it -- a strategy backtest can span
    more than one collection run, per the "same data, different dates"
    framing. `instrument` (e.g. "BTCUSDT") scopes access-mode raw storage,
    which is partitioned by instrument, not by asset name."""
    records: list[dict[str, object]] = []
    for manifest in manifests:
        available = set(manifest.get("sources") or []) | set(manifest.get("plugins") or [])
        if key not in available:
            continue
        data_dir = Path(manifest["data_dir"])
        if manifest.get("mode") == "access":
            records.extend(_read_access_records(key, start_ts_ns, end_ts_ns, data_dir, instrument))
        else:
            records.extend(_read_collect_records(key, start_ts_ns, end_ts_ns, data_dir))
    records.sort(key=lambda r: r["timestamp"])
    return records


__all__ = [
    "PRICE_PATH_BASE_KEYS", "PRICE_PATH_LABEL", "backtest_coverage", "combined_coverage",
    "key_coverage", "narrowest_key", "price_path_coverage", "read_records",
]
