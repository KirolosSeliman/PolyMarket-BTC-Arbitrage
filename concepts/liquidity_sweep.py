"""Concept ICT : détection des prises de liquidité (liquidity sweep / stop hunt).

Lit directement les bougies OHLCV natives Binance (voir binance_futures_kline
dans le catalogue de données -- la granularité se choisit à la collecte, pas
ici), repère les "swing highs" (points hauts fractals) et "swing lows"
(points bas fractals), les regroupe en pools de liquidité (equal highs =
buyside liquidity, equal lows = sellside liquidity), puis détecte quand une
mèche perce ce niveau avant que le prix ne referme aussitôt de l'autre côté
-- le "stop hunt" / "Turtle Soup" de la méthodologie ICT. Une prise de
buyside liquidity (BSL) est un signal baissier ; une prise de sellside
liquidity (SSL) est un signal haussier.
"""

from __future__ import annotations

CONCEPT_INFO = {
    "label": "Liquidity Sweep (ICT)",
    "category": "ICT",
    "description": (
        "Détecte les prises de liquidité ICT : une mèche qui perce un plus "
        "haut/bas significatif (swing ou equal highs/lows) puis un retour "
        "du prix de l'autre côté du niveau."
    ),
    "detail": (
        "Détecte les swing highs/lows fractals sur les bougies Binance "
        "reçues, puis regroupe les swings proches en pools de liquidité : "
        "les swing highs forment la 'buyside liquidity' (BSL, equal highs -- "
        "où reposent les stops des vendeurs), les swing lows la 'sellside "
        "liquidity' (SSL, equal lows -- où reposent les stops des "
        "acheteurs). Un sweep est confirmé quand une bougie perce un pool "
        "avec sa mèche puis que le prix referme de l'autre côté du niveau "
        "dans un nombre limité de bougies (stop hunt / Turtle Soup) : une "
        "BSL balayée est un signal baissier, une SSL balayée est un signal "
        "haussier. Renvoie les sweeps confirmés ainsi que les pools de "
        "liquidité encore actifs (jamais balayés) au-dessus et en dessous "
        "du dernier prix connu, et la granularité des bougies reçues "
        "(candle_seconds), détectée automatiquement."
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
                "chercher des swings et des sweeps."
            ),
        },
        {
            "name": "swing_lookback",
            "type": "number",
            "label": "Largeur du fractal de swing",
            "default": 3,
            "description": (
                "Nombre de bougies de part et d'autre nécessaires pour confirmer un "
                "swing high/low."
            ),
        },
        {
            "name": "equal_level_tolerance_pct",
            "type": "number",
            "label": "Tolérance des niveaux égaux (%)",
            "default": 0.05,
            "description": (
                "Tolérance en % de prix pour regrouper deux swings proches dans le "
                "même pool 'equal highs/lows'."
            ),
        },
        {
            "name": "confirmation_candles",
            "type": "number",
            "label": "Bougies de confirmation",
            "default": 3,
            "description": (
                "Nombre maximal de bougies après la mèche de percée pour valider le "
                "retournement (clôture revenue de l'autre côté du niveau)."
            ),
        },
        {
            "name": "min_wick_pct",
            "type": "number",
            "label": "Profondeur minimale de mèche (%)",
            "default": 0,
            "description": (
                "Profondeur minimale de la mèche au-delà du niveau, en % du prix, "
                "pour filtrer les micro-percées insignifiantes."
            ),
        },
        {
            "name": "min_pool_touches",
            "type": "number",
            "label": "Touches minimales du pool",
            "default": 1,
            "description": (
                "Nombre minimal de swings regroupés pour qu'un niveau compte comme "
                "pool de liquidité valide (2+ = n'exige que des equal highs/lows "
                "avérés)."
            ),
        },
        {
            "name": "direction",
            "type": "select",
            "label": "Sens des sweeps",
            "default": "both",
            "options": ["both", "buyside", "sellside"],
            "description": (
                "Ne détecter que les prises de liquidité côté achat (buyside), côté "
                "vente (sellside), ou les deux."
            ),
        },
    ],
}


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _find_swings(candles, swing_lookback):
    highs = []
    lows = []
    n = len(candles)
    for i in range(swing_lookback, n - swing_lookback):
        window = candles[i - swing_lookback: i + swing_lookback + 1]
        candle = candles[i]
        other_highs = [w["high"] for w in window if w is not candle]
        other_lows = [w["low"] for w in window if w is not candle]
        if other_highs and candle["high"] > max(other_highs):
            highs.append((i, candle["start"], candle["high"]))
        if other_lows and candle["low"] < min(other_lows):
            lows.append((i, candle["start"], candle["low"]))
    return highs, lows


def _cluster_levels(swings, tolerance_pct):
    clusters = []
    current = []
    for swing in sorted(swings, key=lambda s: s[2]):
        if not current:
            current = [swing]
            continue
        ref_price = current[0][2]
        within_tolerance = ref_price and abs(swing[2] - ref_price) / ref_price * 100 <= tolerance_pct
        if within_tolerance:
            current.append(swing)
        else:
            clusters.append(current)
            current = [swing]
    if current:
        clusters.append(current)
    return clusters


def _build_pools(swings, side, tolerance_pct, min_pool_touches, candles):
    pools = []
    for cluster in _cluster_levels(swings, tolerance_pct):
        if len(cluster) < min_pool_touches:
            continue
        formed_index = max(s[0] for s in cluster)
        if side == "buyside":
            level = max(s[2] for s in cluster)
            pool_type = "equal_highs" if len(cluster) > 1 else "swing_high"
        else:
            level = min(s[2] for s in cluster)
            pool_type = "equal_lows" if len(cluster) > 1 else "swing_low"
        pools.append({
            "side": side,
            "level": level,
            "touches": len(cluster),
            "pool_type": pool_type,
            "formed_index": formed_index,
            "formed_at": candles[formed_index]["start"],
        })
    return pools


def _scan_sweep(candles, pool, min_wick_pct, confirmation_candles):
    level = pool["level"]
    n = len(candles)
    for j in range(pool["formed_index"] + 1, n):
        candle = candles[j]
        if pool["side"] == "buyside":
            if candle["high"] <= level:
                continue
            wick_pct = ((candle["high"] - level) / level) * 100 if level else 0.0
            if wick_pct < min_wick_pct:
                continue
            end = min(j + confirmation_candles, n)
            for k in range(j, end):
                if candles[k]["close"] < level:
                    return {"confirmed": True, "wick_pct": wick_pct, "delay": k - j, "swept_at": candle["start"]}
            return {"confirmed": False}
        else:
            if candle["low"] >= level:
                continue
            wick_pct = ((level - candle["low"]) / level) * 100 if level else 0.0
            if wick_pct < min_wick_pct:
                continue
            end = min(j + confirmation_candles, n)
            for k in range(j, end):
                if candles[k]["close"] > level:
                    return {"confirmed": True, "wick_pct": wick_pct, "delay": k - j, "swept_at": candle["start"]}
            return {"confirmed": False}
    return None


def compute(context) -> dict:
    candles = _normalize_candles(context.data.get("binance_futures_kline"))
    candle_seconds = _detect_candle_seconds(candles)
    cfg = context.config

    lookback_candles = int(_to_float(cfg.get("lookback_candles", 500)) or 500)
    swing_lookback = max(int(_to_float(cfg.get("swing_lookback", 3)) or 3), 1)
    tolerance_pct = _to_float(cfg.get("equal_level_tolerance_pct", 0.05))
    if tolerance_pct is None:
        tolerance_pct = 0.05
    confirmation_candles = max(int(_to_float(cfg.get("confirmation_candles", 3)) or 3), 1)
    min_wick_pct = _to_float(cfg.get("min_wick_pct", 0)) or 0.0
    min_pool_touches = max(int(_to_float(cfg.get("min_pool_touches", 1)) or 1), 1)
    direction = str(cfg.get("direction", "both") or "both").lower()
    if direction not in ("buyside", "sellside", "both"):
        direction = "both"

    if lookback_candles > 0:
        candles = candles[-lookback_candles:]

    min_len = swing_lookback * 2 + 1
    if len(candles) < min_len:
        context.log(
            f"pas assez de bougies reçues ({len(candles)}) pour détecter des "
            f"swings (minimum {min_len})"
        )
        return {
            "sweeps": [],
            "active_pools_above": [],
            "active_pools_below": [],
            "last_price": candles[-1]["close"] if candles else None,
            "candle_count": len(candles),
            "candle_seconds": candle_seconds,
        }

    swing_highs, swing_lows = _find_swings(candles, swing_lookback)

    pools = []
    if direction in ("both", "buyside"):
        pools += _build_pools(swing_highs, "buyside", tolerance_pct, min_pool_touches, candles)
    if direction in ("both", "sellside"):
        pools += _build_pools(swing_lows, "sellside", tolerance_pct, min_pool_touches, candles)

    sweeps = []
    active_pools = []
    for pool in pools:
        outcome = _scan_sweep(candles, pool, min_wick_pct, confirmation_candles)
        if outcome is None:
            active_pools.append(pool)
        elif outcome["confirmed"]:
            sweeps.append({
                "direction": "bearish" if pool["side"] == "buyside" else "bullish",
                "level": pool["level"],
                "pool_type": pool["pool_type"],
                "touches": pool["touches"],
                "wick_pct": outcome["wick_pct"],
                "formed_at": pool["formed_at"],
                "swept_at": outcome["swept_at"],
                "confirmation_delay": outcome["delay"],
            })
        # sinon : liquidité prise mais retournement non confirmé -> le pool sort
        # simplement de la liste (ni actif, ni sweep valide).

    sweeps.sort(key=lambda s: s["swept_at"], reverse=True)

    last_price = candles[-1]["close"]
    active_pools_above = sorted(
        (p for p in active_pools if p["level"] > last_price),
        key=lambda p: p["level"],
    )
    active_pools_below = sorted(
        (p for p in active_pools if p["level"] < last_price),
        key=lambda p: p["level"],
        reverse=True,
    )

    candle_info = f" ({candle_seconds:g}s)" if candle_seconds else ""
    context.log(
        f"{len(candles)} bougies reçues{candle_info} -- "
        f"{len(pools)} pools de liquidité, {len(sweeps)} sweeps confirmés, "
        f"{len(active_pools)} pools encore actifs"
    )

    return {
        "sweeps": sweeps,
        "active_pools_above": active_pools_above,
        "active_pools_below": active_pools_below,
        "last_price": last_price,
        "candle_count": len(candles),
        "candle_seconds": candle_seconds,
    }
