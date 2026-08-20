"""Profil d'exécution ICT : entrée sur rebond confirmé d'un FVG.

Cherche, parmi tous les FVG exposés par les microsystèmes de la stratégie
(n'importe quel dict imbriqué ayant la forme d'un FVG -- "direction", "high",
"low", "fill_pct"), un FVG dont le prix a retracé dedans sans jamais avoir
été considéré comme invalidé (une pénétration trop profonde, proxy du corps
de la bougie 1 puisque ce niveau exact n'est pas exposé par les concepts en
amont), puis dont le dernier prix connu est revenu à l'extérieur de la zone,
du même côté que celui par lequel il était entré -- le rebond confirmé qui
déclenche l'entrée, dans le sens d'origine du FVG.

Un profil d'exécution ne reçoit que la sortie déjà calculée des
microsystèmes (`context.microsystems`), jamais les bougies brutes : la
validation "ne pas toucher le corps de la bougie 1" est donc approximée par
un seuil sur `fill_pct` (la pénétration maximale déjà atteinte dans la zone,
telle que renvoyée par le concept `fvg`), et la confirmation du rebond
nécessite qu'un `last_price` soit exposé quelque part dans les
microsystèmes reçus -- sans lui, impossible de savoir si le prix est
ressorti de la zone.
"""

from __future__ import annotations

EXECUTION_INFO = {
    "label": "Entrée sur rebond FVG (ICT)",
    "category": "ICT",
    "description": (
        "Déclenche l'entrée quand le prix retrace dans un FVG sans jamais "
        "clôturer dans le corps de la bougie 1 qui l'a formé, rebondit, puis "
        "ressort de la zone du même côté par lequel il est entré."
    ),
    "detail": (
        "Machine à états sur chaque FVG actif trouvé dans les microsystèmes : "
        "TOUCH (le prix est entré dans la zone, fill_pct > 0) -> VALIDATION "
        "(fill_pct reste sous le seuil d'invalidation, un proxy du corps de la "
        "bougie 1 puisqu'il n'est pas exposé directement) -> REBOND (le "
        "dernier prix connu est ressorti de la zone du côté par lequel il "
        "était entré) -> ENTRÉE dans le sens d'origine du FVG (long si "
        "bullish, short si bearish)."
    ),
    "config_schema": [
        {
            "name": "body_tolerance_pct",
            "type": "number",
            "label": "Tolérance corps bougie 1 (%)",
            "default": 10,
            "description": (
                "Buffer en % de la taille du gap, utilisé pour approximer la "
                "limite du corps de la bougie 1 quand elle n'est pas exposée "
                "directement : au-delà de (100 - ce buffer) % de pénétration "
                "dans la zone, le setup est considéré invalidé."
            ),
        },
        {
            "name": "confirmation_mode",
            "type": "select",
            "label": "Confirmation du rebond",
            "default": "close",
            "options": ["close", "wick"],
            "description": (
                "'close' exige que le dernier prix connu soit ressorti de la "
                "zone ; 'wick' utilise en plus last_high/last_low si un "
                "microsystème les expose (sinon se comporte comme 'close')."
            ),
        },
        {
            "name": "candle_seconds",
            "type": "number",
            "label": "Granularité des bougies (secondes)",
            "default": 5,
            "description": (
                "Doit correspondre à la granularité utilisée par les concepts "
                "FVG en amont -- sert au calcul du timeout."
            ),
        },
        {
            "name": "timeout_candles",
            "type": "number",
            "label": "Timeout (nb de bougies)",
            "default": 10,
            "description": (
                "Nombre maximal de bougies, depuis la formation du FVG, à "
                "attendre pour qu'un rebond se confirme avant d'abandonner le "
                "setup. 0 = pas de timeout."
            ),
        },
        {
            "name": "require_ce_touch",
            "type": "select",
            "label": "Exiger le 50% (consequent encroachment)",
            "default": "false",
            "options": ["true", "false"],
            "description": (
                "Si vrai, n'entre que si le prix a retracé au moins jusqu'au "
                "point médian du gap avant de rebondir."
            ),
        },
        {
            "name": "allowed_direction",
            "type": "select",
            "label": "Sens autorisé",
            "default": "both",
            "options": ["both", "long", "short"],
            "description": "Restreint les entrées à long uniquement, short uniquement, ou les deux.",
        },
        {
            "name": "position_size_usd",
            "type": "number",
            "label": "Taille de position (USD)",
            "default": 500,
        },
    ],
}


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp(value, low, high):
    return max(low, min(high, value))


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


def _collect_fvgs(microsystems):
    seen = set()
    fvgs = []
    for output in microsystems.values():
        for node in _walk(output):
            if _looks_like_fvg(node) and id(node) not in seen:
                seen.add(id(node))
                fvgs.append(node)
    return fvgs


def _collect_reference_price(microsystems):
    for output in microsystems.values():
        for node in _walk(output):
            if isinstance(node, dict):
                price = _to_float(node.get("last_price"))
                if price is not None:
                    return price, _to_float(node.get("last_high")), _to_float(node.get("last_low"))
    return None, None, None


def _collect_now_proxy(microsystems):
    timestamps = []
    for output in microsystems.values():
        for node in _walk(output):
            if isinstance(node, dict):
                for key in ("formed_at", "swept_at"):
                    ts = _to_float(node.get(key))
                    if ts is not None:
                        timestamps.append(ts)
    return max(timestamps) if timestamps else None


def execute(context) -> dict:
    cfg = context.config
    body_tolerance_pct = _clamp(_to_float(cfg.get("body_tolerance_pct", 10)) or 10.0, 0.0, 100.0)
    confirmation_mode = str(cfg.get("confirmation_mode", "close") or "close").lower()
    if confirmation_mode not in ("close", "wick"):
        confirmation_mode = "close"
    candle_seconds = _to_float(cfg.get("candle_seconds", 5)) or 5.0
    timeout_candles = max(_to_float(cfg.get("timeout_candles", 10)) or 10.0, 0.0)
    require_ce_touch = str(cfg.get("require_ce_touch", "false")).strip().lower() == "true"
    allowed_direction = str(cfg.get("allowed_direction", "both") or "both").lower()
    if allowed_direction not in ("both", "long", "short"):
        allowed_direction = "both"
    position_size_usd = _to_float(cfg.get("position_size_usd", 500)) or 500.0

    microsystems = context.microsystems if isinstance(context.microsystems, dict) else {}

    fvgs = _collect_fvgs(microsystems)
    if not fvgs:
        context.log("aucun FVG trouvé dans les microsystèmes reçus")
        return {"position_usd": 0, "direction": "neutre"}

    last_price, last_high, last_low = _collect_reference_price(microsystems)
    if last_price is None:
        context.log(
            f"{len(fvgs)} FVG trouvés mais aucun 'last_price' exposé par les "
            f"microsystèmes -- impossible de confirmer un rebond"
        )
        return {"position_usd": 0, "direction": "neutre"}

    now_proxy = _collect_now_proxy(microsystems)
    invalidation_threshold = 100.0 - body_tolerance_pct
    min_fill_pct = 50.0 if require_ce_touch else 1e-9

    exit_high = last_high if (confirmation_mode == "wick" and last_high is not None) else last_price
    exit_low = last_low if (confirmation_mode == "wick" and last_low is not None) else last_price

    best = None
    for fvg in fvgs:
        direction = fvg.get("direction")
        if direction == "bullish" and allowed_direction == "short":
            continue
        if direction == "bearish" and allowed_direction == "long":
            continue

        fill_pct = _to_float(fvg.get("fill_pct"))
        if fill_pct is None or fill_pct < min_fill_pct or fill_pct >= invalidation_threshold:
            continue

        formed_at = _to_float(fvg.get("formed_at"))
        if now_proxy is not None and formed_at is not None and timeout_candles > 0:
            if (now_proxy - formed_at) > timeout_candles * candle_seconds:
                continue

        if direction == "bullish" and exit_high > fvg["high"]:
            candidate_direction = "haussier"
        elif direction == "bearish" and exit_low < fvg["low"]:
            candidate_direction = "baissier"
        else:
            continue

        rank = formed_at if formed_at is not None else 0.0
        if best is None or rank > best[0]:
            best = (rank, candidate_direction, fvg)

    if best is None:
        context.log(f"{len(fvgs)} FVG examinés, aucun rebond confirmé pour l'instant")
        return {"position_usd": 0, "direction": "neutre"}

    _, direction, fvg = best
    context.log(
        f"rebond confirmé sur un FVG {fvg.get('direction')} "
        f"[{fvg.get('low')}, {fvg.get('high')}] -- entrée {direction}"
    )
    return {"position_usd": position_size_usd, "direction": direction, "fvg": fvg}
