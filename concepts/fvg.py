"""Concept ICT : détection des Fair Value Gaps (FVG) à partir de trades bruts.

Reconstruit des bougies OHLC à partir des trades tick-by-tick (granularité
configurable, en dessous de la minute si besoin), puis cherche le pattern ICT
classique en 3 bougies glissantes : bougie 1 (avant le déplacement), bougie 2
(displacement, la bougie à fort corps qui crée l'imbalance) et bougie 3 (après
le déplacement). Un FVG haussier ("BISI") existe quand le plus bas de la
bougie 3 est au-dessus du plus haut de la bougie 1 ; un FVG baissier ("SIBI")
quand le plus haut de la bougie 3 est en dessous du plus bas de la bougie 1.
"""

from __future__ import annotations

CONCEPT_INFO = {
    "label": "Fair Value Gap (FVG)",
    "category": "ICT",
    "description": (
        "Détecte et qualifie les Fair Value Gaps (déséquilibres de prix sur "
        "3 bougies) selon la méthodologie ICT."
    ),
    "detail": (
        "Reconstruit des bougies OHLC à partir des trades reçus (granularité "
        "configurable en secondes) puis balaie chaque triplet de bougies "
        "consécutives pour détecter les Fair Value Gaps : un FVG haussier "
        "(BISI) apparaît quand le plus bas de la 3e bougie est strictement "
        "au-dessus du plus haut de la 1re ; un FVG baissier (SIBI) quand le "
        "plus haut de la 3e bougie est strictement en dessous du plus bas de "
        "la 1re. Pour chaque FVG, le concept calcule le 'consequent "
        "encroachment' (point médian à 50% du gap, le niveau ICT de "
        "rééquilibrage) et suit son statut au fil du temps : non comblé "
        "(untouched), partiellement comblé (mitigated) ou entièrement comblé "
        "(filled, auquel cas il sort de la liste des FVG actifs). Renvoie la "
        "liste des FVG actifs ainsi que le plus proche au-dessus et en "
        "dessous du dernier prix connu."
    ),
    "data_sources": ["binance_futures_trade"],
    "config_schema": [
        {
            "name": "candle_seconds",
            "type": "number",
            "label": "Granularité des bougies (secondes)",
            "default": 5,
            "description": (
                "Durée de chaque bougie reconstruite à partir des trades, en secondes."
            ),
        },
        {
            "name": "lookback_candles",
            "type": "number",
            "label": "Fenêtre d'analyse (nb de bougies)",
            "default": 500,
            "description": (
                "Nombre de bougies reconstruites (les plus récentes) sur lesquelles "
                "chercher des FVG."
            ),
        },
        {
            "name": "min_gap_pct",
            "type": "number",
            "label": "Taille minimale du gap (%)",
            "default": 0,
            "description": (
                "Taille minimale d'un FVG, en % du prix de clôture de la bougie de "
                "déplacement, pour être retenu. 0 = pas de filtre."
            ),
        },
        {
            "name": "direction",
            "type": "select",
            "label": "Sens des FVG",
            "default": "both",
            "options": ["both", "bullish", "bearish"],
            "description": "Ne détecter que les FVG haussiers, baissiers, ou les deux.",
        },
        {
            "name": "fill_threshold_pct",
            "type": "number",
            "label": "Seuil de comblement (%)",
            "default": 100,
            "description": (
                "Pourcentage de la zone traversée par le prix au-delà duquel un FVG "
                "est considéré comme comblé (filled) et retiré des FVG actifs. "
                "100 = seulement quand le prix a traversé toute la zone, 50 = dès le "
                "consequent encroachment."
            ),
        },
        {
            "name": "require_displacement",
            "type": "select",
            "label": "Exiger un déplacement net",
            "default": "true",
            "options": ["true", "false"],
            "description": (
                "Si vrai, exige que la bougie de déplacement (bougie 2) soit orientée "
                "dans le sens du FVG (haussière pour un FVG bullish, baissière pour "
                "un FVG bearish)."
            ),
        },
    ],
}

_PRICE_KEYS = ("price", "p", "close")
_QTY_KEYS = ("quantity", "qty", "q", "volume")
_TIME_KEYS = ("timestamp", "time", "T", "t", "trade_time", "ts")
_MS_THRESHOLD = 1e11


def _get(trade, keys):
    if not isinstance(trade, dict):
        return None
    for key in keys:
        value = trade.get(key)
        if value is not None:
            return value
    return None


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_candles(trades, candle_seconds):
    parsed = []
    for trade in trades or []:
        price = _to_float(_get(trade, _PRICE_KEYS))
        ts = _to_float(_get(trade, _TIME_KEYS))
        if price is None or ts is None:
            continue
        if ts > _MS_THRESHOLD:
            ts /= 1000.0
        qty = _to_float(_get(trade, _QTY_KEYS)) or 0.0
        parsed.append((ts, price, qty))
    parsed.sort(key=lambda row: row[0])

    bucket_size = max(_to_float(candle_seconds) or 5.0, 0.001)
    buckets = {}
    for ts, price, qty in parsed:
        start = int(ts // bucket_size) * bucket_size
        candle = buckets.get(start)
        if candle is None:
            buckets[start] = {
                "start": start,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": qty,
            }
        else:
            if price > candle["high"]:
                candle["high"] = price
            if price < candle["low"]:
                candle["low"] = price
            candle["close"] = price
            candle["volume"] += qty
    return sorted(buckets.values(), key=lambda c: c["start"])


def _detect_fvgs(candles, direction_filter, min_gap_pct, require_displacement):
    results = []
    for i in range(len(candles) - 2):
        c1, c2, c3 = candles[i], candles[i + 1], candles[i + 2]

        if c3["low"] > c1["high"] and direction_filter in ("both", "bullish"):
            fvg_direction = "bullish"
            gap_low, gap_high = c1["high"], c3["low"]
        elif c3["high"] < c1["low"] and direction_filter in ("both", "bearish"):
            fvg_direction = "bearish"
            gap_low, gap_high = c3["high"], c1["low"]
        else:
            continue

        if gap_high <= gap_low:
            continue

        if require_displacement:
            if fvg_direction == "bullish" and not (c2["close"] > c2["open"]):
                continue
            if fvg_direction == "bearish" and not (c2["close"] < c2["open"]):
                continue

        reference_price = c2["close"] or ((gap_high + gap_low) / 2)
        size_pct = ((gap_high - gap_low) / reference_price) * 100 if reference_price else 0.0
        if size_pct < min_gap_pct:
            continue

        subsequent = candles[i + 2:]
        if fvg_direction == "bullish":
            extreme = min(c["low"] for c in subsequent)
            if extreme <= gap_low:
                fill_pct = 100.0
            elif extreme < gap_high:
                fill_pct = ((gap_high - extreme) / (gap_high - gap_low)) * 100
            else:
                fill_pct = 0.0
        else:
            extreme = max(c["high"] for c in subsequent)
            if extreme >= gap_high:
                fill_pct = 100.0
            elif extreme > gap_low:
                fill_pct = ((extreme - gap_low) / (gap_high - gap_low)) * 100
            else:
                fill_pct = 0.0

        results.append({
            "direction": fvg_direction,
            "high": gap_high,
            "low": gap_low,
            "consequent_encroachment": (gap_high + gap_low) / 2,
            "size_pct": size_pct,
            "formed_at": c3["start"],
            "fill_pct": fill_pct,
        })
    return results


# Generous margin over lookback_candles * candle_seconds against quiet
# spans with fewer than one trade per candle-width -- a backtest only ever
# needs to give this back if it's provably too small, and BTCUSDT trades
# continuously enough that even 1x would be plenty in practice.
_LOOKBACK_SAFETY_FACTOR = 4


def required_lookback_seconds(config):
    candle_seconds = _to_float(config.get("candle_seconds", 5)) or 5.0
    lookback_candles = int(_to_float(config.get("lookback_candles", 500)) or 500)
    if lookback_candles <= 0:
        return None  # 0 = no trim, compute() itself never bounds its lookback either
    return candle_seconds * lookback_candles * _LOOKBACK_SAFETY_FACTOR


def compute(context) -> dict:
    trades = context.data.get("binance_futures_trade") or []

    candle_seconds = _to_float(context.config.get("candle_seconds", 5)) or 5.0
    lookback_candles = int(_to_float(context.config.get("lookback_candles", 500)) or 500)
    min_gap_pct = _to_float(context.config.get("min_gap_pct", 0)) or 0.0
    fill_threshold_pct = _to_float(context.config.get("fill_threshold_pct", 100))
    if fill_threshold_pct is None:
        fill_threshold_pct = 100.0
    direction = str(context.config.get("direction", "both") or "both").lower()
    if direction not in ("both", "bullish", "bearish"):
        direction = "both"
    require_displacement = str(
        context.config.get("require_displacement", "true")
    ).strip().lower() == "true"

    candles = _build_candles(trades, candle_seconds)
    if lookback_candles > 0:
        candles = candles[-lookback_candles:]

    if len(candles) < 3:
        context.log(f"pas assez de bougies reconstruites ({len(candles)}) pour chercher un FVG")
        return {
            "fvgs": [],
            "nearest_above": None,
            "nearest_below": None,
            "last_price": candles[-1]["close"] if candles else None,
            "candle_count": len(candles),
        }

    raw_fvgs = _detect_fvgs(candles, direction, min_gap_pct, require_displacement)
    active = [f for f in raw_fvgs if f["fill_pct"] < fill_threshold_pct]
    for f in active:
        f["status"] = "untouched" if f["fill_pct"] <= 0 else "mitigated"
    active.sort(key=lambda f: f["formed_at"], reverse=True)

    last_price = candles[-1]["close"]
    above_candidates = [f for f in active if f["low"] >= last_price]
    below_candidates = [f for f in active if f["high"] <= last_price]
    nearest_above = min(above_candidates, key=lambda f: f["low"]) if above_candidates else None
    nearest_below = max(below_candidates, key=lambda f: f["high"]) if below_candidates else None

    context.log(
        f"{len(candles)} bougies reconstruites ({candle_seconds:g}s) -- "
        f"{len(active)} FVG actifs sur {len(raw_fvgs)} détectés"
    )

    return {
        "fvgs": active,
        "nearest_above": nearest_above,
        "nearest_below": nearest_below,
        "last_price": last_price,
        "candle_count": len(candles),
    }
