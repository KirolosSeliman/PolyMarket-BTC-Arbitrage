"""Concept ICT : détection des Fair Value Gaps (FVG) à partir de bougies Binance.

Lit directement les bougies OHLCV natives Binance (voir binance_futures_kline
dans le catalogue de données -- la granularité se choisit à la collecte, pas
ici), puis cherche le pattern ICT classique en 3 bougies glissantes : bougie 1
(avant le déplacement), bougie 2 (displacement, la bougie à fort corps qui
crée l'imbalance) et bougie 3 (après le déplacement). Un FVG haussier ("BISI")
existe quand le plus bas de la bougie 3 est au-dessus du plus haut de la
bougie 1 ; un FVG baissier ("SIBI") quand le plus haut de la bougie 3 est en
dessous du plus bas de la bougie 1.
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
        "Balaie chaque triplet de bougies Binance consécutives pour détecter "
        "les Fair Value Gaps : un FVG haussier (BISI) apparaît quand le plus "
        "bas de la 3e bougie est strictement au-dessus du plus haut de la "
        "1re ; un FVG baissier (SIBI) quand le plus haut de la 3e bougie est "
        "strictement en dessous du plus bas de la 1re. Pour chaque FVG, le "
        "concept calcule le 'consequent encroachment' (point médian à 50% du "
        "gap, le niveau ICT de rééquilibrage) et suit son statut au fil du "
        "temps : non comblé (untouched), partiellement comblé (mitigated) ou "
        "entièrement comblé (filled, auquel cas il sort de la liste des FVG "
        "actifs). Renvoie la liste des FVG actifs ainsi que le plus proche "
        "au-dessus et en dessous du dernier prix connu, et la granularité "
        "des bougies reçues (candle_seconds), détectée automatiquement."
    ),
    "data_sources": ["binance_futures_kline"],
    "config_schema": [
        {
            "name": "lookback_candles",
            "type": "number",
            "label": "Fenêtre d'analyse (nb de bougies)",
            "default": 500,
            "description": (
                "Nombre de bougies reçues (les plus récentes) sur lesquelles "
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


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def required_lookback_seconds(config, candle_seconds):
    """Bounds the backtest engine's accumulated-history window to what
    compute()'s own candles[-lookback_candles:] trimming ever keeps --
    without this, compute() re-normalizes the entire history-so-far on
    every single evaluation step just to discard all but the last
    lookback_candles of it. candle_seconds is the engine's own detected
    record spacing for this instance's bound data (None when there isn't
    yet enough data to estimate) -- falls back to unbounded rather than
    guessing an interval, since collection granularity isn't fixed."""
    if candle_seconds is None:
        return None
    lookback_candles = int(_to_float(config.get("lookback_candles", 500)) or 500)
    if lookback_candles <= 0:
        return None
    # 1.5x margin: covers a data gap or slightly irregular candle spacing
    # so compute() is never left with fewer than lookback_candles candles.
    return lookback_candles * candle_seconds * 1.5


def _normalize_candles(records):
    """Binance klines arrive pre-built (open/high/low/close/open_time from
    read_records' own extractor -- see backtest_data.py) -- no reconstruction
    needed here, just validate/sort. "start" keeps the field name the
    detection logic below already used (formerly a reconstructed-bucket
    boundary, now a real candle's open_time) so that logic needs no changes."""
    candles = []
    for record in records or []:
        if not isinstance(record, dict):
            continue
        start = _to_float(record.get("open_time"))
        o, h, l, c = (
            _to_float(record.get("open")), _to_float(record.get("high")),
            _to_float(record.get("low")), _to_float(record.get("close")),
        )
        if None in (start, o, h, l, c):
            continue
        candles.append({
            "start": start, "open": o, "high": h, "low": l, "close": c,
            "volume": _to_float(record.get("volume")) or 0.0,
        })
    candles.sort(key=lambda row: row["start"])
    return candles


def _detect_candle_seconds(candles):
    """The real candle width, read straight off two consecutive candles'
    own timestamps -- no config field needed (and no risk of it silently
    not matching what was actually collected, the failure mode a manual
    "candle_seconds" setting used to have)."""
    if len(candles) < 2:
        return None
    return candles[1]["start"] - candles[0]["start"]


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


def compute(context) -> dict:
    candles = _normalize_candles(context.data.get("binance_futures_kline"))
    candle_seconds = _detect_candle_seconds(candles)

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

    if lookback_candles > 0:
        candles = candles[-lookback_candles:]

    if len(candles) < 3:
        context.log(f"pas assez de bougies reçues ({len(candles)}) pour chercher un FVG")
        return {
            "fvgs": [],
            "nearest_above": None,
            "nearest_below": None,
            "last_price": candles[-1]["close"] if candles else None,
            "candle_count": len(candles),
            "candle_seconds": candle_seconds,
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

    candle_info = f" ({candle_seconds:g}s)" if candle_seconds else ""
    context.log(
        f"{len(candles)} bougies reçues{candle_info} -- "
        f"{len(active)} FVG actifs sur {len(raw_fvgs)} détectés"
    )

    return {
        "fvgs": active,
        "nearest_above": nearest_above,
        "nearest_below": nearest_below,
        "last_price": last_price,
        "candle_count": len(candles),
        "candle_seconds": candle_seconds,
    }
