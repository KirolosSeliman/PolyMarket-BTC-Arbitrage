"""Profil de gestion ICT : stop sur mèche/zone selon confirmation, cible fin de leg.

Stop-loss : si le prix a déjà parcouru une marge suffisante au-delà de la
zone du FVG (proxy de "la bougie suivante a clôturé dans le sens du trade",
puisqu'un profil de gestion ne reçoit jamais les bougies brutes, seulement
la sortie déjà calculée des microsystèmes) -- le stop se place sur la mèche
de la bougie qui est entrée dans le FVG, reconstituée depuis `fill_pct` (la
profondeur de pénétration atteinte dans la zone). Sinon, le stop se place
sur le bord opposé de la zone (toute la zone sert de coussin).

Take-profit : vise toujours la fin de la leg précédente -- le pool de
liquidité actif le plus proche dans le sens du trade (buyside pour un long,
sellside pour un short), tel qu'exposé par exemple par le concept
`liquidity_sweep` via `active_pools_above`/`active_pools_below`. À défaut,
se rabat sur l'extrémité la plus proche parmi les FVG connus comme proxy
grossier de l'étendue de la leg.
"""

from __future__ import annotations

MANAGEMENT_INFO = {
    "label": "SL confirmation + TP fin de leg (ICT)",
    "category": "ICT",
    "description": (
        "Place le stop-loss sur la mèche d'entrée si la bougie suivante "
        "confirme dans le sens du trade (sinon sur le bord opposé du FVG), et "
        "vise toujours le sommet ou le creux de la leg précédente comme "
        "take-profit."
    ),
    "detail": (
        "Stop-loss : reconstitue le niveau de la mèche d'entrée dans le FVG à "
        "partir de fill_pct (profondeur de pénétration atteinte dans la "
        "zone). Si le prix a déjà avancé, au-delà de la zone, d'au moins la "
        "marge de confirmation configurée, ce niveau de mèche sert de stop "
        "(serré) -- sinon le bord opposé de la zone sert de stop (large). "
        "Take-profit : cible le pool de liquidité actif le plus proche dans "
        "le sens du trade (buyside pour un long, sellside pour un short) "
        "s'il est exposé par un microsystème en amont (ex. liquidity_sweep), "
        "sinon l'extrémité la plus proche parmi les FVG connus."
    ),
    "config_schema": [
        {
            "name": "confirmation_margin_pct",
            "type": "number",
            "label": "Marge de confirmation (%)",
            "default": 20,
            "description": (
                "Distance minimale, en % de la hauteur du gap, déjà parcourue par "
                "le prix au-delà de la zone pour considérer que la bougie suivante "
                "a confirmé dans le sens du trade (sinon le stop se place sur tout "
                "le bord de la zone, plus large)."
            ),
        },
        {
            "name": "sl_buffer_pct",
            "type": "number",
            "label": "Buffer de sécurité SL (%)",
            "default": 0,
            "description": "Marge ajoutée au-delà du niveau de stop-loss calculé (mèche ou bord de zone).",
        },
        {
            "name": "tp_buffer_pct",
            "type": "number",
            "label": "Buffer de sécurité TP (%)",
            "default": 0,
            "description": "Marge retirée avant le niveau de take-profit visé (previous top/low).",
        },
        {
            "name": "min_reward_risk_ratio",
            "type": "number",
            "label": "Ratio risque/récompense minimum",
            "default": 0,
            "description": (
                "Si supérieur à 0, invalide la gestion (SL/TP renvoyés à None) quand "
                "le ratio récompense/risque calculé est inférieur à ce minimum. 0 = "
                "pas de filtre."
            ),
        },
    ],
}


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _walk(value, depth=0):
    if depth > 6:
        return
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk(item, depth + 1)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _walk(item, depth + 1)


def _looks_like_fvg(node):
    return (
        isinstance(node, dict)
        and node.get("direction") in ("bullish", "bearish")
        and isinstance(node.get("high"), (int, float))
        and isinstance(node.get("low"), (int, float))
        and node["high"] > node["low"]
    )


def _looks_like_pool(node):
    return (
        isinstance(node, dict)
        and node.get("side") in ("buyside", "sellside")
        and isinstance(node.get("level"), (int, float))
    )


def _collect(microsystems, predicate):
    seen = set()
    results = []
    for output in microsystems.values():
        for node in _walk(output):
            if predicate(node) and id(node) not in seen:
                seen.add(id(node))
                results.append(node)
    return results


def _collect_reference_price(microsystems):
    for output in microsystems.values():
        for node in _walk(output):
            if isinstance(node, dict):
                price = _to_float(node.get("last_price"))
                if price is not None:
                    return price
    return None


def manage(context) -> dict:
    cfg = context.config
    confirmation_margin_pct = _to_float(cfg.get("confirmation_margin_pct", 20)) or 20.0
    sl_buffer_pct = _to_float(cfg.get("sl_buffer_pct", 0)) or 0.0
    tp_buffer_pct = _to_float(cfg.get("tp_buffer_pct", 0)) or 0.0
    min_rr = _to_float(cfg.get("min_reward_risk_ratio", 0)) or 0.0

    execution = context.execution if isinstance(context.execution, dict) else {}
    direction = execution.get("direction", "neutre")
    if direction not in ("haussier", "baissier"):
        context.log("aucune position à gérer (direction neutre)")
        return {"stop_loss": None, "take_profit": None, "reward_risk_ratio": None}

    fvg_direction = "bullish" if direction == "haussier" else "bearish"
    microsystems = context.microsystems if isinstance(context.microsystems, dict) else {}

    fvg = execution.get("fvg")
    if not _looks_like_fvg(fvg) or fvg.get("direction") != fvg_direction:
        candidates = [
            f for f in _collect(microsystems, _looks_like_fvg)
            if f.get("direction") == fvg_direction
        ]
        fvg = max(candidates, key=lambda f: _to_float(f.get("formed_at")) or 0.0) if candidates else None

    if fvg is None:
        context.log(f"aucun FVG associé à la position {direction} -- gestion impossible")
        return {"stop_loss": None, "take_profit": None, "reward_risk_ratio": None}

    high = _to_float(fvg.get("high"))
    low = _to_float(fvg.get("low"))
    fill_pct = _to_float(fvg.get("fill_pct")) or 0.0
    if high is None or low is None or high <= low:
        context.log("FVG associé invalide -- gestion impossible")
        return {"stop_loss": None, "take_profit": None, "reward_risk_ratio": None}
    gap_height = high - low

    last_price = _collect_reference_price(microsystems)
    reference_price = last_price if last_price is not None else (high if fvg_direction == "bullish" else low)

    if fvg_direction == "bullish":
        entry_wick_level = high - (fill_pct / 100.0) * gap_height
        distance_beyond_pct = ((reference_price - high) / gap_height) * 100.0
        confirmed = distance_beyond_pct >= confirmation_margin_pct
        sl_base = entry_wick_level if confirmed else low
        stop_loss = sl_base * (1 - sl_buffer_pct / 100.0)
    else:
        entry_wick_level = low + (fill_pct / 100.0) * gap_height
        distance_beyond_pct = ((low - reference_price) / gap_height) * 100.0
        confirmed = distance_beyond_pct >= confirmation_margin_pct
        sl_base = entry_wick_level if confirmed else high
        stop_loss = sl_base * (1 + sl_buffer_pct / 100.0)

    pools = _collect(microsystems, _looks_like_pool)
    take_profit = None
    if fvg_direction == "bullish":
        targets = [p for p in pools if p.get("side") == "buyside" and p["level"] > reference_price]
        if targets:
            take_profit = min(targets, key=lambda p: p["level"])["level"] * (1 - tp_buffer_pct / 100.0)
    else:
        targets = [p for p in pools if p.get("side") == "sellside" and p["level"] < reference_price]
        if targets:
            take_profit = max(targets, key=lambda p: p["level"])["level"] * (1 + tp_buffer_pct / 100.0)

    if take_profit is None:
        other_fvgs = _collect(microsystems, _looks_like_fvg)
        if fvg_direction == "bullish":
            highs = [f["high"] for f in other_fvgs if f["high"] > reference_price]
            if highs:
                take_profit = max(highs) * (1 - tp_buffer_pct / 100.0)
        else:
            lows = [f["low"] for f in other_fvgs if f["low"] < reference_price]
            if lows:
                take_profit = min(lows) * (1 + tp_buffer_pct / 100.0)

    reward_risk_ratio = None
    if take_profit is not None:
        risk = abs(reference_price - stop_loss)
        reward = abs(take_profit - reference_price)
        if risk > 0:
            reward_risk_ratio = reward / risk
            if min_rr > 0 and reward_risk_ratio < min_rr:
                context.log(
                    f"ratio risque/récompense {reward_risk_ratio:.2f} sous le "
                    f"minimum {min_rr:.2f} -- pas de gestion exploitable"
                )
                return {"stop_loss": None, "take_profit": None, "reward_risk_ratio": reward_risk_ratio}

    context.log(
        f"position {direction} -- SL={stop_loss:.6g} "
        f"({'mèche' if confirmed else 'bord de zone'}), "
        f"TP={'aucun' if take_profit is None else round(take_profit, 6)}"
    )

    return {
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "reward_risk_ratio": reward_risk_ratio,
        "trailing": False,
    }
