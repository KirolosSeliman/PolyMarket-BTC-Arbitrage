"""Provider-shaped offline fixtures with synthetic identifiers."""

NOW_MS = 1_750_000_000_000
NOW_US = NOW_MS * 1_000
NOW_NS = NOW_MS * 1_000_000


def chainlink_update() -> dict[str, object]:
    return {
        "topic": "crypto_prices_chainlink",
        "type": "update",
        "timestamp": NOW_MS + 2,
        "payload": {
            "symbol": "btc/usd",
            "timestamp": NOW_MS,
            "value": "67234.5000",
        },
    }

def agg_trade(*, maker: bool = True) -> dict[str, object]:
    return {
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": NOW_US,
            "s": "BTCUSDT",
            "a": 12345,
            "p": "67234.50",
            "q": "0.125",
            "f": 100,
            "l": 105,
            "T": NOW_US - 100,
            "m": maker,
            "M": True,
        },
    }


def book_ticker() -> dict[str, object]:
    return {
        "stream": "btcusdt@bookTicker",
        "data": {
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "67234.40",
            "B": "1.2",
            "a": "67234.50",
            "A": "0.8",
        },
    }


def depth20(levels: int = 20) -> dict[str, object]:
    return {
        "stream": "btcusdt@depth20@100ms",
        "data": {
            "lastUpdateId": 400900218,
            "bids": [
                [str(67234.4 - index / 10), str(index + 1)]
                for index in range(levels)
            ],
            "asks": [
                [str(67234.5 + index / 10), str(index + 1)]
                for index in range(levels)
            ],
        },
    }


def clob_book(asset_id: str = "up-token") -> dict[str, object]:
    return {
        "event_type": "book",
        "asset_id": asset_id,
        "market": "condition-1",
        "bids": [{"price": ".48", "size": "30"}, {"price": ".49", "size": "20"}],
        "asks": [{"price": ".52", "size": "25"}, {"price": ".53", "size": "60"}],
        "timestamp": str(NOW_MS),
        "hash": "book-hash",
    }
