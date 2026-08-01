"""Turns a reconstructed full order book into a bucketed DOM ladder.

Pure data transformation: sums resting size into fixed-width price buckets
over a configurable range around the mid price. No signal generation, no
gap/anomaly detection -- that's left for downstream analysis code the
caller writes separately.
"""
from __future__ import annotations

from decimal import ROUND_FLOOR, Decimal

from ..models import BinanceDomSnapshotPayload, DomBucketPayload
from .full_depth_reconstruction import FullDepthOrderBook


def build_dom_snapshot(
    book: FullDepthOrderBook,
    market: str,
    *,
    bucket_size: Decimal,
    price_range: Decimal,
) -> BinanceDomSnapshotPayload | None:
    mid = book.mid_price
    if mid is None:
        return None

    low = mid - price_range
    high = mid + price_range

    def floor_to_bucket(price: Decimal) -> Decimal:
        return (price / bucket_size).to_integral_value(rounding=ROUND_FLOOR) * bucket_size

    totals: dict[Decimal, list[Decimal]] = {}  # bucket_price -> [bid_qty, ask_qty]
    bucket = floor_to_bucket(low)
    while bucket <= high:
        totals[bucket] = [Decimal(0), Decimal(0)]
        bucket += bucket_size

    for price, qty in book.bids.items():
        if low <= price <= high:
            totals[floor_to_bucket(price)][0] += qty
    for price, qty in book.asks.items():
        if low <= price <= high:
            totals[floor_to_bucket(price)][1] += qty

    buckets = tuple(
        DomBucketPayload(price=p, bid_quantity=v[0], ask_quantity=v[1])
        for p, v in sorted(totals.items())
    )
    return BinanceDomSnapshotPayload(
        market=market,
        mid_price=mid,
        bucket_size=bucket_size,
        buckets=buckets,
    )
